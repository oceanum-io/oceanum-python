import os
import io
import tempfile
import hashlib
import requests
import fsspec
import xarray
import geopandas
import pandas
import warnings
import tempfile
from urllib.parse import urlparse
import asyncio
from functools import wraps, partial

from .datasource import Datasource, _datasource_props, _datasource_driver
from .catalog import Catalog
from .query import Query, Stage, Container

DEFAULT_CONFIG = {"DATAMESH_SERVICE": "https://datamesh.oceanum.io"}


class DatameshConnectError(Exception):
    pass


class DatameshQueryError(Exception):
    pass


class DatameshWriteError(Exception):
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
        self._host = url.netloc
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
        if resp.status_code == 404:
            raise DatameshConnectError(f"Datasource {datasource_id} not found")
        elif resp.status_code == 401:
            raise DatameshConnectError(f"Datasource {datasource_id} not Authorized")
        elif resp.status_code != 200:
            raise DatameshConnectError(resp.text)
        return resp

    def _metadata_write(self, datasource):
        if datasource._exists:
            resp = requests.patch(
                f"{self._proto}://{self._host}/datasource/{datasource.id}/",
                data=datasource.json(by_alias=True),
                headers={**self._auth_headers, "Content-Type": "application/json"},
            )

        else:
            resp = requests.post(
                f"{self._proto}://{self._host}/datasource/",
                data=datasource.json(by_alias=True),
                headers={**self._auth_headers, "Content-Type": "application/json"},
            )
        if resp.status_code >= 300:
            raise DatameshConnectError(resp.text)
        return resp

    def _delete(self, datasource_id):
        resp = requests.delete(
            f"{self._gateway}/data/{datasource_id}",
            headers=self._auth_headers,
        )
        if resp.status_code >= 300:
            raise DatameshConnectError(resp.text)
        return True

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

    def _data_write(
        self,
        datasource_id,
        data,
        data_format="application/json",
        append=None,
        overwrite=False,
    ):
        if overwrite:
            resp = requests.put(
                f"{self._gateway}/data/{datasource_id}",
                data=data,
                headers={"Content-Type": data_format, **self._auth_headers},
            )
        else:
            headers = {"Content-Type": data_format, **self._auth_headers}
            if append:
                headers["X-Append"] = str(append)
            resp = requests.patch(
                f"{self._gateway}/data/{datasource_id}",
                data=data,
                headers=headers,
            )
        if not resp.status_code == 200:
            raise DatameshConnectError(resp.text)
        return Datasource(**resp.json())

    def _stage_request(self, query, cache=False):
        qhash = hashlib.sha224(query.json().encode()).hexdigest()

        resp = requests.post(
            f"{self._gateway}/oceanql/stage/",
            headers=self._auth_headers,
            data=query.json(),
        )
        if resp.status_code >= 400:
            raise DatameshQueryError(resp.text)
        elif resp.status_code == 204:
            return None
        else:
            return Stage(**resp.json())

    def _query(self, query, use_dask=True):
        if not isinstance(query, Query):
            query = Query(**query)
        stage = self._stage_request(query)
        if stage is None:
            warnings.warn("No data found for query")
            return None
        if use_dask and (stage.container == Container.Dataset):
            mapper = self._zarr_proxy(stage.qhash)
            return xarray.open_zarr(
                mapper, consolidated=True, decode_coords="all", mask_and_scale=True
            )
        else:
            transfer_format = (
                "application/x-netcdf4"
                if stage.container == Container.Dataset
                else "application/parquet"
            )
            headers = {"Accept": transfer_format, **self._auth_headers}
            resp = requests.post(
                f"{self._gateway}/oceanql/", headers=headers, data=query.json()
            )
            if resp.status_code >= 400:
                raise DatameshQueryError(resp.text)
            else:
                # tmpfile = os.path.join(self._cachedir.name, stage.qhash)
                with tempfile.NamedTemporaryFile("wb") as f:
                    f.write(resp.content)
                    f.seek(0)
                    if stage.container == Container.Dataset:
                        return xarray.load_dataset(f.name)
                    if stage.container == Container.GeoDataFrame:
                        return geopandas.read_parquet(f.name)
                    else:
                        return pandas.read_parquet(f.name)

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

    @asyncwrapper
    def get_catalog_async(self, filter={}):
        """Get datamesh catalog asynchronously

        Args:
            filter (dict, optional): Set of filters to apply. Defaults to {}.
            loop: event loop. default=None will use :obj:`asyncio.get_running_loop()`
            executor: :obj:`concurrent.futures.Executor` instance. default=None will use the default executor

        Returns:
            Coroutine<:obj:`oceanum.datamesh.Catalog`>: A datamesh catalog instance
        """
        return self.get_catalog(filter)

    def get_datasource(self, datasource_id):
        """Get a Datasource instance from the datamesh. This does not load the actual data.

        Args:
            datasource_id (string): Unique datasource id

        Returns:
            :obj:`oceanum.datamesh.Datasource`: A datasource instance

        Raises:
            DatameshConnectError: Datasource cannot be found or is not authorized for the datamesh key
        """
        meta = self._metadata_request(datasource_id)
        meta_dict = meta.json()
        props = {
            "id": datasource_id,
            "geom": meta_dict["geometry"],
            **meta_dict["properties"],
        }
        return Datasource(**props)

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
        return self.get_datasource(datasource_id)

    def load_datasource(self, datasource_id, use_dask=True):
        """Load a datasource into the work environment.
        For datasources which load into DataFrames or GeoDataFrames, this returns an in memory instance of the DataFrame.
        For datasources which load into an xarray Dataset, an open zarr backed dataset is returned.

        Args:
            datasource_id (string): Unique datasource id
            use_dask (bool, optional): Load datasource as a dask enabled datasource if possible. Defaults to True.

        Returns:
            Union[:obj:`pandas.DataFrame`, :obj:`geopandas.GeoDataFrame`, :obj:`xarray.Dataset`]: The datasource container
        """
        stage = self._stage_request(Query(datasource=datasource_id))
        if stage is None:
            warnings.warn("No data found for query")
            return None
        if stage.container == Container.Dataset:
            mapper = self._zarr_proxy(datasource_id)
            return xarray.open_zarr(
                mapper, consolidated=True, decode_coords="all", mask_and_scale=True
            )
        elif stage.container == Container.GeoDataFrame:
            tmpfile = self._data_request(datasource_id, "application/parquet")
            return geopandas.read_parquet(tmpfile)
        elif stage.container == Container.DataFrame:
            tmpfile = self._data_request(datasource_id, "application/parquet")
            return pandas.read_parquet(tmpfile)

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
        return self.load_datasource(datasource_id, use_dask)

    def query(self, query, use_dask=True):
        """Make a datamesh query

        Args:
            query (Union[:obj:`oceanum.datamesh.Query`, dict]): Datamesh query as a query object or a valid query dictionary
            use_dask (bool, optional): Load datasource as a dask enabled datasource if possible. Defaults to True.

        Returns:
            Union[:obj:`pandas.DataFrame`, :obj:`geopandas.GeoDataFrame`, :obj:`xarray.Dataset`]: The datasource container
        """
        return self._query(query, use_dask)

    @asyncwrapper
    async def query_async(self, query, use_dask=True):
        """Make a datamesh query asynchronously

        Args:
            query (Union[:obj:`oceanum.datamesh.Query`, dict]): Datamesh query as a query object or a valid query dictionary
            use_dask (bool, optional): Load datasource as a dask enabled datasource if possible. Defaults to True.
            loop: event loop. default=None will use :obj:`asyncio.get_running_loop()`
            executor: :obj:`concurrent.futures.Executor` instance. default=None will use the default executor

        Returns:
            Coroutine<Union[:obj:`pandas.DataFrame`, :obj:`geopandas.GeoDataFrame`, :obj:`xarray.Dataset`]>: The datasource container
        """
        return self._query(query, use_dask)

    def write_datasource(
        self,
        datasource_id,
        data,
        geometry=None,
        append=None,
        overwrite=False,
        **properties,
    ):
        """Write a datasource to datamesh from the work environment

        Args:
            datasource_id (string): Unique datasource id
            data (Union[:obj:`pandas.DataFrame`, :obj:`geopandas.GeoDataFrame`, :obj:`xarray.Dataset`]): The data to be written to datamesh
            geometry (:obj:`oceanum.datasource.Geometry`, optional): GeoJSON geometry of the datasource
            append (string, optional): Coordinate to append on. default=None
            overwrite (bool, optional): Overwrite existing datasource. default=False
            **properties: Additional properties for the datasource - see :obj:`oceanum.datamesh.Datasource`

        Returns:
            :obj:`oceanum.datamesh.Datasource`: The datasource instance that was written to
        """
        try:
            ds = self.get_datasource(datasource_id)
        except DatameshConnectError as e:
            overwrite = True
        with tempfile.NamedTemporaryFile("w+b", delete=False) as f:
            try:
                if isinstance(data, xarray.Dataset):
                    data.to_netcdf(f.name)
                    f.seek(0)
                    ds = self._data_write(
                        datasource_id,
                        f.read(),
                        "application/x-netcdf4",
                        append,
                        overwrite,
                    )
                else:
                    data.to_parquet(f, index=True)
                    f.seek(0)
                    ds = self._data_write(
                        datasource_id,
                        f.read(),
                        "application/parquet",
                        append,
                        overwrite,
                    )
                ds._exists = True
            except Exception as e:
                raise DatameshWriteError(e)
            finally:
                os.remove(f.name)

            for key in properties:
                setattr(ds, key, properties[key])
            if geometry:
                ds.geom = geometry
            try:
                self._metadata_write(ds)
            except:
                raise DatameshWriteError(
                    "Cannot register datasource {datasource_id}: {e}"
                )
        return ds

    @asyncwrapper
    def write_datasource_async(
        self, datasource_id, data, append=None, overwrite=False, **properties
    ):
        """Write a datasource to datamesh from the work environment asynchronously

        Args:
            datasource_id (string): Unique datasource id
            data (Union[:obj:`pandas.DataFrame`, :obj:`geopandas.GeoDataFrame`, :obj:`xarray.Dataset`]): The data to be written to datamesh
            geometry (:obj:`oceanum.datasource.Geometry`): GeoJSON geometry of the datasource
            append (string, optional): Coordinate to append on. default=None
            overwrite (bool, optional): Overwrite existing datasource. default=False
            **properties: Additional properties for the datasource - see :obj:`oceanum.datamesh.Datasource` constructor

        Returns:
            Coroutine<:obj:`oceanum.datamesh.Datasource`>: The datasource instance that was written to
        """
        return self.write_datasource(
            datasource_id, data, append, overwrite, **properties
        )

    def delete_datasource(self, datasource_id):
        """Delete a datasource from datamesh. This will delete the datamesh registration and any stored data.

        Args:
            datasource_id (string): Unique datasource id

        Returns:
            boolean: Return True for successfully deleted datasource
        """
        return self._delete(datasource_id)

    @asyncwrapper
    def delete_datasource_async(self, datasource_id):
        """Asynchronously delete a datasource from datamesh. This will delete the datamesh registration and any stored data.

        Args:
            datasource_id (string): Unique datasource id

        Returns:
            boolean: Return True for successfully deleted datasource
        """
        return self._delete(datasource_id)
