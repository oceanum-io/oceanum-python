from pydantic import BaseModel
from datetime import datetime
import requests
from .connection import Connector, DatameshConnectError


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
    
    def close(self, finalise_write: bool = False):
        res = requests.delete(f"{self._connection._gateway}/session/{self.session_id}",
                              params={"finalise_write": finalise_write},
                              headers={"X-DATAMESH-SESSIONID": self.session_id})
        if res.status_code != 204:
            raise DatameshConnectError("Failed to close session with error: " + res.text)
    
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close(finalise_write=exc_type is None)