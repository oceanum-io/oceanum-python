from pydantic import BaseModel
from typing import Optional
from datetime import datetime, timedelta
from .exceptions import DatameshConnectError, DatameshSessionError
from .utils import retried_request
import atexit
import os


class Session(BaseModel):
    id: str
    user: str
    creation_time: datetime
    end_time: datetime
    write: bool
    allow_multiwrite: bool = False
    verified: bool = False

    @classmethod
    def acquire(cls,
                connection,
                allow_multiwrite: Optional[bool]=False,):
        """
        Acquire a session from the connection.

        Parameters
        ----------
        connection : Connection
            Connection object to acquire session from.
        allow_multiwrite : bool
            Whether to allow other sessions to write to datasource
            already being written to by this session.
            Default is False.
        """

        # Back-compatibility with beta version (returning dummy session object)
        if not connection._is_v1:
            session =\
                cls(id="dummy_session",
                    user="dummy_user",
                    creation_time=datetime.now(),
                    end_time=datetime.now()+timedelta(seconds=connection._session_params.get("duration", 3600)),
                    write=False,
                    verified=False)
            session._connection = connection
            atexit.register(session.close)
            return session
        # v1
        try:
            headers = connection._auth_headers.copy()
            headers["Cache-Control"] = "no-store"
            params = connection._session_params.copy()
            params["allow_multiwrite"] = allow_multiwrite
            res = retried_request(f"{connection._gateway}/session/",
                                  params=params,
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
    def from_proxy(cls,
                   session_duration: Optional[float]=None,
                   allow_multiwrite: Optional[bool]=False):
        """
        Convenience constructor to acquire a session directly from the proxy.
        Uses environment variables only and used for internal purposes.
        Parameters
        ----------
        session_duration : float
            Duration of the session in seconds.
            Default is 3600 seconds (1 hour).
        allow_multiwrite : bool
            Whether to allow other sessions to write to datasource
            already being written to by this session.
            Default is False.
        """

        try:
            res = retried_request(f"{os.environ['DATAMESH_ZARR_PROXY']}/session/",
                                  params={'duration': session_duration or 3600,
                                          'allow_multiwrite': allow_multiwrite},
                                  headers={"X-DATAMESH-TOKEN": os.environ['DATAMESH_TOKEN'],
                                           "USER": os.environ['DATAMESH_USER'],
                                           'Cache-Control': 'no-cache'})
            if res.status_code != 200:
                raise DatameshConnectError("Failed to create session with error: " + res.text)
            session = cls(**res.json())
            session._connection = lambda: None
            session._connection._gateway = os.environ['DATAMESH_ZARR_PROXY']
            session._connection._is_v1 = True
            atexit.register(session.close)
            return session
        except Exception as e:
            raise DatameshSessionError(f"Error when acquiring datamesh session from proxy {e}")

    @classmethod
    def from_session_id(cls,
                        connection,
                        session_id: str):
        """
        Acquire a session from the connection using the session id.

        Parameters
        ----------
        connection : Connection
            Connection object to acquire session from.
        session_id : str
            Session id to acquire.
        """

        # Back-compatibility with beta version (returning dummy session object)
        if not connection._is_v1:
            raise DatameshSessionError("Cannot acquire session from id when using datamesh v0")
        # v1
        try:
            res = retried_request(f"{connection._gateway}/session/{session_id}",
                                  headers=connection._auth_headers)
            if res.status_code != 200:
                raise DatameshConnectError(f"Failed to retrieve session {session_id} with error: {res.text}")
            session = cls(**res.json())
            session._connection = connection
            return session
        except Exception as e:
            raise DatameshSessionError(f"Error when acquiring datamesh session {e}")

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
        res = retried_request(f"{self._connection._gateway}/session/{self.id}",
                              method="DELETE",
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
