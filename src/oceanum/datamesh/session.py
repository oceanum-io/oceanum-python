from pydantic import BaseModel
from typing import Optional
from datetime import datetime
from .exceptions import DatameshConnectError, DatameshSessionError
from .utils import retried_request, HTTPSession
import atexit
import os
import logging
import time


logger = logging.getLogger(__name__)


class Session(BaseModel):
    id: str
    user: str
    creation_time: datetime
    end_time: datetime
    write: bool
    allow_multiwrite: bool = False
    verified: bool = False

    @classmethod
    def acquire(
        cls,
        connection,
        allow_multiwrite: Optional[bool] = False,
        duration: Optional[int] = None,
    ):
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
        duration: int
            The duration of the session in seconds. Will default to
            the connection session duration if set or 3600 (1 hour)
        """

        try:
            headers = {
                "Cache-Control": "no-store"
            }
            params = connection._session_params.copy()
            params["allow_multiwrite"] = allow_multiwrite
            if duration is not None:
                params["duration"] = duration
            res = retried_request(
                f"{connection._gateway}/session/",
                params=params,
                headers=headers,
                http_session=connection.http_session
            )
            if res.status_code != 200:
                raise DatameshConnectError(
                    "Failed to create session with error: " + res.text
                )
            session = cls(**res.json())
            session._connection = connection
            atexit.register(session.close)
            return session
        except Exception as e:
            raise DatameshSessionError(f"Error when acquiring datamesh session {e}")

    @classmethod
    def from_proxy(
        cls,
        session_duration: Optional[float] = None,
        allow_multiwrite: Optional[bool] = False,
    ):
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
            http_session = HTTPSession(headers={"X-DATAMESH-TOKEN": os.environ["DATAMESH_TOKEN"]})
            res = retried_request(
                f"{os.environ['DATAMESH_ZARR_PROXY']}/session/",
                params={
                    "duration": session_duration or 3600,
                    "allow_multiwrite": allow_multiwrite,
                },
                headers={
                    "USER": os.environ["DATAMESH_USER"],
                    "Cache-Control": "no-cache",
                },
                http_session=http_session
            )
            if res.status_code != 200:
                raise DatameshConnectError(
                    "Failed to create session with error: " + res.text
                )
            session = cls(**res.json())
            session._connection = lambda: None
            session._connection._gateway = os.environ["DATAMESH_ZARR_PROXY"]
            session._connection.http_session = http_session
            atexit.register(session.close)
            return session
        except Exception as e:
            raise DatameshSessionError(
                f"Error when acquiring datamesh session from proxy {e}"
            )

    @classmethod
    def from_session_id(cls, connection, session_id: str):
        """
        Acquire a session from the connection using the session id.

        Parameters
        ----------
        connection : Connection
            Connection object to acquire session from.
        session_id : str
            Session id to acquire.
        """

        try:
            res = retried_request(
                f"{connection._gateway}/session/{session_id}",
                http_session=connection.http_session,
            )
            if res.status_code != 200:
                raise DatameshConnectError(
                    f"Failed to retrieve session {session_id} with error: {res.text}"
                )
            session = cls(**res.json())
            session._connection = connection
            return session
        except Exception as e:
            raise DatameshSessionError(f"Error when acquiring datamesh session {e}")

    @property
    def header(self):
        return {"X-DATAMESH-SESSIONID": self.id}

    def add_header(self, headers: dict):
        return {**headers, **self.header}

    def close(self, finalise_write: bool = False):
        try:
            atexit.unregister(self.close)
        except:
            pass

        # Retry DELETE on non-204 responses with exponential backoff
        max_attempts = 4  # initial + 3 retries
        backoff_delays = [1, 2, 4]  # seconds

        for attempt in range(max_attempts):
            res = retried_request(
                f"{self._connection._gateway}/session/{self.id}",
                method="DELETE",
                params={"finalise_write": finalise_write},
                headers=self.header,
                http_session=self._connection.http_session,
            )

            if res.status_code == 204:
                return

            # Treat 404/410 as success (session already closed)
            if res.status_code in (404, 410):
                return

            # If this was the last attempt, handle the error
            if attempt == max_attempts - 1:
                if finalise_write:
                    raise DatameshConnectError(
                        "Failed to finalise write with error: " + res.text
                    )
                # Log warning instead of raising for finalise_write=False
                # (we're in __exit__ and raising would mask the original error)
                logger.warning(
                    f"Failed to close session {self.id} after {max_attempts} attempts. "
                    f"Status code: {res.status_code}. Response: {res.text}. "
                    f"Session may block writes to its datasource until it expires."
                )
                return

            # Not the last attempt and status != 204/404/410, so retry with backoff
            time.sleep(backoff_delays[attempt])

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # When using context manager, close the session
        # and finalise the write if no exception was raised
        self.close(finalise_write=exc_type is None)
