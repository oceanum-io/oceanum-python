import os
import io
import tempfile
import hashlib
import requests
import fsspec
import xarray
import geopandas
import pandas
from urllib.parse import urlparse
import asyncio
from functools import wraps, partial

from .datasource import Datasource
from .catalog import Catalog
from .query import Query

DEFAULT_CONFIG = {"DATAMESH_SERVICE": "https://datamesh.oceanum.io"}


class DatameshConnectError(Exception):
    pass


class DatameshQueryError(Exception):
    pass


def asyncwrapper(func):
    @wraps(func)
    async def run(*args, loop=None, executor=None, **kwargs):
        if loop is None:
            loop = asyncio.get_event_loop()
        pfunc = partial(func, *args, **kwargs)
        return await loop.run_in_executor(executor, pfunc)

    return run


class Connector(object):
    """Datamesh connector class.

    All datamesh operations are methods of this class
    """

    def __init__(
        self,
        token=None,
        service=os.environ.get("DATAMESH_SERVICE", DEFAULT_CONFIG["DATAMESH_SERVICE"]),
        gateway=None,
    ):
        """Datamesh connector constructor

        Args:
            token (string): Your datamesh access token. Defaults to os.environ.get("DATAMESH_TOKEN", None).
            service (string, optional): URL of datamesh service. Defaults to os.environ.get("DATAMESH_SERVICE", "https://datamesh.oceanum.io").
            gateway (string, optional): URL of gateway service. Defaults to os.environ.get("DATAMESH_GATEWAY", "https://gateway.<datamesh_service_domain>").

        Raises:
            ValueError: Missing or invalid arguments
        """
        if token is None:
            token = os.environ.get("DATAMESH_TOKEN", None)
            if token is None:
                raise ValueError(
                    "A valid key must be supplied as a connection constructor argument or defined in environment variables as DATAMESH_TOKEN"
                )
        self._token = token
        url = urlparse(service)
        self._proto = url.scheme
        self._host = url.hostname
        self._auth_headers = {
            "Authorization": "Token " + self._token,
            "X-DATAMESH-TOKEN": self._token,
        }
        self._gateway = gateway or f"{self._proto}://gateway.{self._host}"
        self._cachedir = tempfile.TemporaryDirectory(prefix="datamesh_")

    @property
    def host(self):
        """Datamesh host

        Returns:
            string: Datamesh server host
        """
        return self._host

    # Check the status of the metadata server
    def _status(self):
        resp = requests.get(f"{self._proto}://{self._host}", headers=self._auth_headers)
        return rest.status_code == 200

    def _metadata_request(self, datasource_id=""):
        resp = requests.get(
            f"{self._proto}://{self._host}/datasource/{datasource_id}",
            headers=self._auth_headers,
        )
        return resp

    def _zarr_proxy(self, datasource_id):
        try:
            mapper = fsspec.get_mapper(
                f"{self._gateway}/zarr/{datasource_id}",
                headers=self._auth_headers,
            )
        except Exception as e:
            raise DatameshConnectError(str(e))
        return mapper

    def _data_request(self, datasource_id, data_format="application/json", cache=False):
        tmpfile = os.path.join(self._cachedir.name, datasource_id)
        resp = requests.get(
            f"{self._gateway}/data/{datasource_id}",
            headers={"Accept": data_format, **self._auth_headers},
        )
        if not resp.status_code == 200:
            raise DatameshConnectError(resp.text)
        else:
            with open(tmpfile, "wb") as f:
                f.write(resp.content)
            return tmpfile

    def _query_request(self, query, data_format="application/json", cache=False):
        qhash = hashlib.sha224(query.json().encode()).hexdigest()
        headers = {"Accept": data_format, **self._auth_headers}
        resp = requests.post(
            f"{self._gateway}/oceanql/", headers=headers, data=query.json()
        )
        if not resp.status_code == 200:
            raise DatameshQueryError(resp.text)
        else:
            tmpfile = os.path.join(self._cachedir.name, qhash)
            with open(tmpfile, "wb") as f:
                f.write(resp.content)
            return tmpfile

    def _query(self, query):
        if not isinstance(query, Query):
            query = Query(**query)
        ds = self.get_datasource(query.datasource)
        transfer_format = (
            "application/x-netcdf4"
            if ds.container == xarray.Dataset
            else "application/parquet"
        )
        f = self._query_request(query, data_format=transfer_format)
        if ds.container == xarray.Dataset:
            return xarray.open_dataset(f, engine="h5netcdf", decode_coords="all").load()
        elif ds.container == geopandas.GeoDataFrame:
            return geopandas.read_parquet(f)
        else:
            return pandas.read_parquet(f)

    def get_catalog(self, filter={}):
        """Get datamesh catalog

        Args:
            filter (dict, optional): Set of filters to apply. Defaults to {}.

        Returns:
            :obj:`oceanum.datamesh.Catalog`: A datamesh catalog instance
        """
        cat = Catalog._init(
            self,
        )
        return cat

    async def get_catalog_async(self, filter={}):
        """Get datamesh catalog asynchronously

        Args:
            filter (dict, optional): Set of filters to apply. Defaults to {}.
            loop: event loop. default=None will use :obj:`asyncio.get_running_loop()`
            executor: :obj:`concurrent.futures.Executor` instance. default=None will use the default executor

        Returns:
            Coroutine<:obj:`oceanum.datamesh.Catalog`>: A datamesh catalog instance
        """
        cat = Catalog._init(
            self,
        )
        return cat

    def get_datasource(self, datasource_id):
        """Get a Datasource instance from the datamesh. This does not load the actual data.

        Args:
            datasource_id (string): Unique datasource id

        Returns:
            :obj:`oceanum.datamesh.Datasource`: A datasource instance

        Raises:
            DatameshConnectError: Datasource cannot be found or is not authorized for the datamesh key
        """
        return Datasource._init(self, datasource_id)

    @asyncwrapper
    def get_datasource_async(self, datasource_id):
        """Get a Datasource instance from the datamesh asynchronously. This does not load the actual data.

        Args:
            datasource_id (string): Unique datasource id
            loop: event loop. default=None will use :obj:`asyncio.get_running_loop()`
            executor: :obj:`concurrent.futures.Executor` instance. default=None will use the default executor

        Returns:
            Coroutine<:obj:`oceanum.datamesh.Datasource`>: A datasource instance

        Raises:
            DatameshConnectError: Datasource cannot be found or is not authorized for the datamesh key
        """
        return Datasource._init(self, datasource_id)

    def load_datasource(self, datasource_id, use_dask=True):
        """Load a datasource into the work environment

        Args:
            datasource_id (string): Unique datasource id
            use_dask (bool, optional): Load datasource as a dask enabled datasource if possible. Defaults to True.

        Returns:
            Union[:obj:`pandas.DataFrame`, :obj:`geopandas.GeoDataFrame`, :obj:`xarray.Dataset`]: The datasource container
        """
        ds = self.get_datasource(datasource_id)
        return ds.load()

    @asyncwrapper
    def load_datasource_async(self, datasource_id, use_dask=True):
        """Load a datasource asynchronously into the work environment

        Args:
            datasource_id (string): Unique datasource id
            use_dask (bool, optional): Load datasource as a dask enabled datasource if possible. Defaults to True.
            loop: event loop. default=None will use :obj:`asyncio.get_running_loop()`
            executor: :obj:`concurrent.futures.Executor` instance. default=None will use the default executor


        Returns:
            coroutine<Union[:obj:`pandas.DataFrame`, :obj:`geopandas.GeoDataFrame`, :obj:`xarray.Dataset`]>: The datasource container
        """
        ds = self.get_datasource(datasource_id)
        return ds.load()

    def query(self, query):
        """Make a datamesh query

        Args:
            query (Union[:obj:`oceanum.datamesh.Query`, dict]): Datamesh query as a query object or a valid query dictionary

        Returns:
            Union[:obj:`pandas.DataFrame`, :obj:`geopandas.GeoDataFrame`, :obj:`xarray.Dataset`]: The datasource container
        """
        return self._query(query)

    @asyncwrapper
    async def query_async(self, query):
        """Make a datamesh query asynchronously

        Args:
            query (Union[:obj:`oceanum.datamesh.Query`, dict]): Datamesh query as a query object or a valid query dictionary
            loop: event loop. default=None will use :obj:`asyncio.get_running_loop()`
            executor: :obj:`concurrent.futures.Executor` instance. default=None will use the default executor

        Returns:
            Coroutine<Union[:obj:`pandas.DataFrame`, :obj:`geopandas.GeoDataFrame`, :obj:`xarray.Dataset`]>: The datasource container
        """
        return self._query(query)