import os
import requests
import fsspec
from urllib.parse import urlparse

DEFAULT_CONFIG = {"DATAMESH_SERVICE": "https://datamesh.oceanum.io"}


class DatameshConnectError(Exception):
    pass


class Connector(object):
    """Datamesh connector class.

    All datamesh operations are methods of this class
    """

    def __init__(
        self,
        key=os.environ.get("DATAMESH_KEY", None),
        service=os.environ.get("DATAMESH_SERVICE", DEFAULT_CONFIG["DATAMESH_SERVICE"]),
    ):
        """Datamesh connector constructor

        Args:
            key (string): Your datamesh access key. Defaults to os.environ.get("DATAMESH_KEY", None).
            service (string, optional): URL of datamesh service. Defaults to os.environ.get("DATAMESH_KEY", "https://datamesh.oceanum.io").

        Raises:
            ValueError: Missing or invalid arguments
        """
        if key is None:
            raise ValueError(
                "A valid key must be supplied as a connection constructor argument"
            )
        else:
            self._key = key
        url = urlparse(service)
        self._proto = url.scheme
        self._host = url.hostname
        self._auth_headers = {
            "Authorization": "Token " + self._key,
            "X-DATAMESH-KEY": self._key,
        }

    @property
    def host(self):
        """Datamesh host

        Returns:
            string: Datamesh server host
        """
        return self._host

    # Check the status of the metadata server
    def _status(self):
        resp = requests.get(f"{self._proto}//{self._host}", headers=self._auth_headers)
        return rest.status_code == 200

    def _metadata_request(self, datasource_id):
        resp = requests.get(
            f"{self._proto}//{self._host}/{datasource_id}", headers=self._auth_headers
        )
        return resp

    def _zarr_proxy(self, datasource_id):
        try:
            mapper = fsspec.get_mapper(
                f"{self._proto}//gateway.{self._host}/zarr/{datasource_id}",
                headers=headers,
            )
        except Exception as e:
            raise DatameshConnectError(str(e))
        return mapper

    def _data_request(self, datasource_id, data_format="application/json"):
        resp = requests.get(
            f"{self._proto}//gateway.{self._host}/data/{datasource_id}",
            headers={"Accept": format, **self._auth_headers},
        )
        if not resp.status_code == 200:
            raise DatameshConnectError(resp.text)
        else:
            return resp.content

    def _query_request(self, query, data_format="application/json"):
        headers = {"Accept": data_format, **self._auth_headers}
        resp = requests.post(
            f"{self._proto}//gateway.{self._host}/oceanql/",
            headers=headers,
        )
        if not resp.status_code == 200:
            raise DatameshConnectError(resp.text)
        else:
            return resp.content

    def get_catalog(self, filter={}):
        """Get datamesh catalog

        Args:
            filter (dict, optional): Set of filters to apply. Defaults to {}.

        Returns:
            :obj:`oceanum.datamesh.Catalog`: A datamesh catalog instance
        """
        resp = requests.get(
            f"{self._proto}//{self._host}/datasource/", headers={**self._auth_headers}
        )
        cat = Catalog._init(self, resp.json())
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
        ds = Datasource._init(self, datasource_id)
        if not ds._exists():
            raise DatameshConnectError(
                f"Datasource {datasource_id} does not exist or is not authorized"
            )
        return ds

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
            query = Query(query)
        transfer_format = (
            "application/x-netcdf4"
            if self.container == "xarray.Dataset"
            else "application/parquet"
        )

        return self._query_request(query, data_format=transfer_format)