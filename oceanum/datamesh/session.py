from pydantic import BaseModel
from datetime import datetime
import requests
from .exceptions import DatameshConnectError
import atexit

class Session(BaseModel):
    session_id: str
    user: str
    creation_time: datetime
    end_time: datetime
    write: bool
    verified: bool = False

    @classmethod
    def acquire(cls,
                connection,
                auto_close: bool = False):
        """
        Acquire a session from the connection.

        Parameters
        ----------
        connection : Connection
            Connection object to acquire session from.
        auto_close : bool, optional
            Automatically close the session when the program exits, by default False
            Not necessary if using the session as a context manager (with statement).
        """
        try:
            res = requests.get(f"{connection._gateway}/session",
                            headers=connection._auth_headers)
            if res.status_code != 200:
                raise DatameshConnectError("Failed to create session with error: " + res.text)
            session = cls(**res.json())
            session._connection = connection
            if auto_close:
                atexit.register(session.close)
            return session
        except Exception as e:
            raise e
    
    def close(self, finalise_write: bool = False):
        try:
            atexit.unregister(self.close)
        except:
            pass
        res = requests.delete(f"{self._connection._gateway}/session/{self.session_id}",
                              params={"finalise_write": finalise_write},
                              headers={"X-DATAMESH-SESSIONID": self.session_id})
        if res.status_code != 204:
            if finalise_write:
                raise DatameshConnectError("Failed to finalise write with error: " + res.text)
            print("Failed to close session with error: " + res.text)
    
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # Check if close is registered to be called at exit
        #try:
        #    atexit.unregister(self.close)
        #except:
        #    pass
        self.close(finalise_write=exc_type is None)
