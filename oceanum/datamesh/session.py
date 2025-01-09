from pydantic import BaseModel
from datetime import datetime, timedelta
import requests
from .exceptions import DatameshConnectError, DatameshSessionError
import atexit
import os


class Session(BaseModel):
    id: str
    user: str
    creation_time: datetime
    end_time: datetime
    write: bool
    verified: bool = False

    @classmethod
    def acquire(cls,
                connection):
        """
        Acquire a session from the connection.

        Parameters
        ----------
        connection : Connection
            Connection object to acquire session from.
        """

        # Back-compatibility with beta version (returning dummy session object)
        if not connection._is_v1:
            session =\
                cls(id="dummy_session",
                    user="dummy_user",
                    creation_time=datetime.now(),
                    end_time=datetime.now()+timedelta(hours=1),
                    write=False,
                    verified=False)
            session._connection = connection
            atexit.register(session.close)
            return session
        # v1
        try:
            headers = connection._auth_headers.copy()
            headers["Cache-Control"] = "no-store"
            res = requests.get(f"{connection._gateway}/session/",
                               headers=headers)
            if res.status_code != 200:
                raise DatameshConnectError("Failed to create session with error: " + res.text)
            session = cls(**res.json())
            session._connection = connection
            atexit.register(session.close)
            return session
        except Exception as e:
            raise DatameshSessionError(f"Error when acquiring datamesh session {e}")
        
    @classmethod
    def from_proxy(cls):
        """
        Convenience constructor to acquire a session directly from the proxy.
        Uses environment variables only and used for internal purposes.
        """

        try:
            res = requests.get(f"{os.environ['DATAMESH_ZARR_PROXY']}/session/",
                               headers={"X-DATAMESH-TOKEN": os.environ['DATAMESH_TOKEN'],
                                        "USER": os.environ['DATAMESH_USER'],
                                        'Cache-Control': 'no-cache'})
            if res.status_code != 200:
                raise DatameshConnectError("Failed to create session with error: " + res.text)
            session = cls(**res.json())
            session._connection = lambda: None
            session._connection._gateway = os.environ['DATAMESH_ZARR_PROXY']
            atexit.register(session.close)
            return session
        except Exception as e:
            raise DatameshSessionError(f"Error when acquiring datamesh session from proxy {e}")

    @property
    def header(self):
        return {"X-DATAMESH-SESSIONID": self.id}

    def add_header(self, headers: dict):
        headers.update(self.header)
        return headers
    
    def close(self, finalise_write: bool = False):
        # Back-compatibility with beta version (ignoring)
        if not self._connection._is_v1:
            return
        # datamesh v1
        try:
            atexit.unregister(self.close)
        except:
            pass
        res = requests.delete(f"{self._connection._gateway}/session/{self.id}",
                              params={"finalise_write": finalise_write},
                              headers=self.header)
        if res.status_code != 204:
            if finalise_write:
                raise DatameshConnectError("Failed to finalise write with error: " + res.text)
            print("Failed to close session with error: " + res.text)
    
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # When using context manager, close the session
        # and finalise the write if no exception was raised
        self.close(finalise_write=exc_type is None)