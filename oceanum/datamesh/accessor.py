import xarray as xr
import os
import requests
import json
import time
import re

from .connection import Connector, DEFAULT_CONFIG, DatameshConnectError
#from .zarr import ZarrClient

from collections.abc import MutableMapping

from datetime import datetime
from pydantic import BaseModel


class Session(BaseModel):
    session_id: str
    user: str
    creation_time: datetime
    end_time: datetime
    write: bool
    open: bool
    verified: bool = False

    @classmethod
    def acquire(cls,
                connection: Connector):
        try:
            res = requests.get(f"{connection._gateway}/session",
                            headers=connection._auth_headers)
            if res.status_code != 200:
                raise DatameshConnectError("Failed to create session with error: " + res.text)
            session = cls(**res.json())
            session._connection = connection
            return session
        except Exception as e:
            raise e
    
    def close(self):
        res = requests.delete(f"{self._connection._gateway}/session/{self.session_id}",
                              headers={"X-DATAMESH-SESSIONID": self.session_id})
        if res.status_code != 204:
            raise DatameshConnectError("Failed to close session with error: " + res.text)
    
    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close(self._connection)

class ZarrProxyClient(MutableMapping):
    def __init__(
        self,
        connection,
        datasource,
        session,
        parameters={},
        method="post",
        retries=8,
        nocache=False,
        request_type="zarr_proxy"
    ):
        self.datasource = datasource
        self.session = session
        self.method = method
        self.request_type = request_type
        self.headers = {**connection._auth_headers}
        self.headers["X-DATAMESH-SESSIONID"] = session.session_id
        if nocache:
            self.headers["cache-control"] = "no-transform"
        if parameters:
            self.headers["X-PARAMETERS"] = json.dumps(parameters)
        if request_type == "zarr_proxy":
            self._proxy = connection._gateway + "/zarr"
        elif request_type == "query_proxy":
            self._proxy = connection._gateway + "/zarr/query"
        else:
            raise DatameshConnectError(f"Unknown request type: {request_type}")
        self.retries = retries

    def _request_session(self,
                         duration: float = 1) -> Session:
        """Return a requests session valid for the requested duration in hours.
        
        Args:
            duration: float
                The duration in hours for which the session should be valid.
        """
        res = requests.get(self._proxy + "/session",
                           headers=self.headers)
        if res.status_code != 200:
            raise DatameshConnectError("Failed to create session with error: " + res.text)
        self.session = Session(**res.json())
        self.headers["X-DATAMESH-SESSIONID"] = self.session.session_id
        print(f"Joining session {self.headers['X-DATAMESH-SESSIONID']} valid for {duration} hours.")

    def _close_session(self):
        """Close the current session."""
        res = requests.get(f"{self._proxy}/session/{self.headers['X-DATAMESH-SESSIONID']}", headers=self.headers)
        if res.status_code != 200:
            raise DatameshConnectError("Failed to close session with error: " + res.text)
        print(f"Session {self.headers.pop(['X-DATAMESH-SESSIONID'])} close successfully.")

    def _get(self, path, retrieve_data=True):
        retries = 0
        while retries < self.retries:
            try:
                if retrieve_data:
                    resp = requests.get(path, headers=self.headers)
                else:
                    resp = requests.head(path, headers=self.headers)
            except requests.RequestException:
                time.sleep(0.1 * 2**retries)
                retries += 1
            else:
                return resp

    def __getitem__(self, item):
        resp = self._get(f"{self._proxy}/{self.datasource}/{item}")
        if resp.status_code >= 300:
            raise KeyError(item)
        return resp.content

    def __contains__(self, item):
        resp = self._get(f"{self._proxy}/{self.datasource}/{item}",
                         retrieve_data=False)
        if resp.status_code != 200:
            return False
        return True

    def __setitem__(self, item, value):
        if self.request_type == "query_proxy":
            raise DatameshConnectError("Cannot write to query proxy")
        if self.method == "put":
            requests.put(
                f"{self.zarr_proxy}/{self.datasource}/{item}",
                data=value,
                headers=self.headers,
            )
        else:
            requests.post(
                f"{self.zarr_proxy}/{self.datasource}/{item}",
                data=value,
                headers=self.headers,
            )

    def __delitem__(self, item):
        requests.delete(
            f"{self.zarr_proxy}/{self.datasource}/{item}", headers=self.headers
        )

    def __iter__(self):
        resp = self._get(f"{self._proxy}/{self.datasource}")
        if not resp:
            return
        ex = re.compile(r"""<(a|A)\s+(?:[^>]*?\s+)?(href|HREF)=["'](?P<url>[^"']+)""")
        links = [u[2] for u in ex.findall(resp.text)]
        for link in links:
            yield link

    def __len__(self):
        return 0
    
#from datamesh.lakefs import write_datasource_lakefs

@xr.register_dataset_accessor("datamesh")
class DatameshAccessor:
    def __init__(self, xarray_obj):
        self._obj = xarray_obj
        self._connector =\
            Connector()
        
    def say_hello(self):
        print("Hello from the DatameshAccessor!")
        
    def _request_session(self):
        """Return a requests session."""
        return self._connector._request_session()
    
    def _close_session(self):
        """Close the requests session."""
        self._connector._close_session()

    def load_datasource(self, datasource):
        """Load the data source for this dataset."""
        
        return self._connector.load_datasource(datasource)

    def to_zarr(self, path, **kwargs):
        """Write this dataset to a Zarr store."""
        with Session.acquire(self._connector) as session:
            

            client = ZarrProxyClient(self._connector, self._obj.name, request_type="zarr_proxy")
            client._request_session()
            client[path] = self._obj.to_zarr()
            client._close_session()

    #@property
    #def center(self):
    #    """Return the geographic center point of this dataset."""
    #    if self._center is None:
    #        # we can use a cache on our accessor objects, because accessors
    #        # themselves are cached on instances that access them.
    #        lon = self._obj.latitude
    #        lat = self._obj.longitude
    #        self._center = (float(lon.mean()), float(lat.mean()))
    #    return self._center

    #def plot(self):
    #    """Plot data on a map."""
    #    return "plotting!"
