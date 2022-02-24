import os
import requests
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
        service=DEFAULT_CONFIG["DATAMESH_SERVICE"],
    ):
        f"""Datamesh connector constructor

        Args:
            key (string): Your datamesh access key. Defaults to os.environ.get("DATAMESH_KEY", None).
            service (_type_, optional): URL of datamesh service. Defaults to {DEFAULT_CONFIG["DATAMESH_SERVICE"]}.

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

    def _zarr_proxy(self, id):
        resp = requests.get(
            f"{self._proto}//gateway.{self._host}/zarr/{datasource_id}",
            headers=headers,
        )

    def _data_request(self, id):
        resp = requests.get(
            f"{self._proto}//gateway.{self._host}/data/{datasource_id}",
            headers=self._auth_headers,
        )
        if not resp.status_code == 200:
            raise DatameshConnectError(resp.text)

    def _query_request(self, query, data_format=None):
        headers = {**self._auth_headers}
        if data_format:
            headers["Accept"] = data_format
        resp = requests.post(
            f"{self._proto}//gateway.{self._host}/oceanql/",
            headers=headers,
        )
        if not resp.status_code == 200:
            raise DatameshConnectError(resp.text)

    def get_catalog(self, filter={}):
        """Get datamesh catalog

        Args:
            filter (dict, optional): Set of filters to apply. Defaults to {}.
        """
        pass

    def get_datasource(self, datasource_id):
        """Get a Datasource instance from the datamesh

        Args:
            datasource_id (string): Unique datasource id

        Raises:
            DatameshConnectError: Datasource cannot be found or is not authorized for the datamesh key
        """
        ds = Datasource.init(self, datasource_id)
        if not ds._exists():
            raise DatameshConnectError(
                f"Datasource {datasource_id} does not exist or is not authorized"
            )

    def load_datasource(self, datasource_id, use_dask=True):
        """Load a datasource into the work environment

        Args:
            datasource_id (string): Unique datasource id
            use_dask (bool, optional): Load datasource as a dask enabled datasource if possible. Defaults to True.
        """
        ds = self.get_datasource(datasource_id)
        if use_dask:
            try:
                return ds.to_dask()
            except:
                return ds.read()
        else:
            return ds.read()