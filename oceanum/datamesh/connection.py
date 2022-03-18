import os
import io
import tempfile
import requests
import fsspec
import xarray
import geopandas
import pandas
from urllib.parse import urlparse

from ..utils.response import ResponseFile
from .datasource import Datasource
from .catalog import Catalog
from .query import Query

DEFAULT_CONFIG = {"DATAMESH_SERVICE": "https://datamesh.oceanum.io"}


class DatameshConnectError(Exception):
    pass


class DatameshQueryError(Exception):
    pass


class Connector(object):
    """Datamesh connector class.

    All datamesh operations are methods of this class
    """

    def __init__(
        self,
        token=os.environ.get("DATAMESH_TOKEN", None),
        service=os.environ.get("DATAMESH_SERVICE", DEFAULT_CONFIG["DATAMESH_SERVICE"]),
        gateway=None,
    ):
        """Datamesh connector constructor

        Args:
            token (string): Your datamesh access token. Defaults to os.environ.get("DATAMESH_TOKEN", None).
            service (string, optional): URL of datamesh service. Defaults to os.environ.get("DATAMESH_SERVICE", "https://datamesh.oceanum.io").
            gateway (string, optional): URL of gateway service. Defaults to os.environ.get("DATAMESH_GATEWAY", "https://gateway.datamesh.oceanum.io").

        Raises:
            ValueError: Missing or invalid arguments
        """
        if token is None:
            raise ValueError(
                "A valid key must be supplied as a connection constructor argument"
            )
        else:
            self._token = token
        url = urlparse(service)
        self._proto = url.scheme
        self._host = url.hostname
        self._auth_headers = {
            "Authorization": "Token " + self._token,
            "X-DATAMESH-TOKEN": self._token,
        }
        self._gateway = gateway or f"{self._proto}://gateway.{self._host}"

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

    def _data_request(self, datasource_id, data_format="application/json"):
        resp = requests.get(
            f"{self._gateway}/data/{datasource_id}",
            headers={"Accept": data_format, **self._auth_headers},
        )
        if not resp.status_code == 200:
            raise DatameshConnectError(resp.text)
        else:
            return ResponseFile(resp.content)

    def _query_request(self, query, data_format="application/json"):
        headers = {"Accept": data_format, **self._auth_headers}
        resp = requests.post(
            f"{self._gateway}/oceanql/", headers=headers, data=query.json()
        )
        if not resp.status_code == 200:
            raise DatameshQueryError(resp.text)
        else:
            return resp.content

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

    def query(self, query):
        """Make a datamesh query

        Args:
            query (Union[:obj:`oceanum.datamesh.Query`, dict]): Datamesh query as a query object or a valid query dictionary

        Returns:
            Union[:obj:`pandas.DataFrame`, :obj:`geopandas.GeoDataFrame`, :obj:`xarray.Dataset`]: The datasource container
        """

        if not isinstance(query, Query):
            query = Query(**query)
        ds = self.get_datasource(query.datasource)
        transfer_format = (
            "application/x-netcdf4"
            if ds.container == xarray.Dataset
            else "application/parquet"
        )
        resp = self._query_request(query, data_format=transfer_format)
        with io.BytesIO() as f:
            f.write(resp)
            if ds.container == xarray.Dataset:
                return xarray.open_dataset(f, engine="h5py")
            elif ds.container == geopandas.GeoDataFrame:
                return geopandas.read_parquet(f)
            else:
                return pandas.read_parquet(f)
