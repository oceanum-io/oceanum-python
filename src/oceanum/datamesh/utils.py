from time import sleep
import requests
from requests.adapters import HTTPAdapter
import numpy as np
from .exceptions import DatameshConnectError
import os


# Timeouts in seconds to establish connection to datamesh services
# for read types of operations
DATAMESH_CONNECT_TIMEOUT = os.getenv("DATAMESH_CONNECT_TIMEOUT", 3.05)
DATAMESH_CONNECT_TIMEOUT = (
    None if DATAMESH_CONNECT_TIMEOUT == "None" else float(DATAMESH_CONNECT_TIMEOUT)
)

# Timeout in seconds to read data from datamesh services
# for small json payloads type of operations
DATAMESH_READ_TIMEOUT = os.getenv("DATAMESH_READ_TIMEOUT", 10)
DATAMESH_READ_TIMEOUT = (
    None if DATAMESH_READ_TIMEOUT == "None" else float(DATAMESH_READ_TIMEOUT)
)

# Timeout in seconds for staging endpoint
DATAMESH_STAGE_READ_TIMEOUT = os.getenv("DATAMESH_STAGE_READ_TIMEOUT", 900)
DATAMESH_STAGE_READ_TIMEOUT = (
    None if DATAMESH_STAGE_READ_TIMEOUT == "None" else float(DATAMESH_STAGE_READ_TIMEOUT)
)

# Timeout in seconds for bulk download operations
DATAMESH_DOWNLOAD_TIMEOUT = os.getenv("DATAMESH_DOWNLOAD_TIMEOUT", 900)
DATAMESH_DOWNLOAD_TIMEOUT = (
    None if DATAMESH_DOWNLOAD_TIMEOUT == "None" else float(DATAMESH_DOWNLOAD_TIMEOUT)
)

# Timeout in seconds for bulk write operations
DATAMESH_WRITE_TIMEOUT = os.getenv("DATAMESH_WRITE_TIMEOUT", "None")
DATAMESH_WRITE_TIMEOUT = (
    None if DATAMESH_WRITE_TIMEOUT == "None" else float(DATAMESH_WRITE_TIMEOUT)
)

# Timeout in seconds for zarr chunk read operations
DATAMESH_CHUNK_READ_TIMEOUT = os.getenv("DATAMESH_CHUNK_READ_TIMEOUT", 60)
DATAMESH_CHUNK_READ_TIMEOUT = (
    None if DATAMESH_CHUNK_READ_TIMEOUT == "None" else float(DATAMESH_CHUNK_READ_TIMEOUT)
)

# Timeout in seconds for zarr chunk write operations
# much larger than for read seems to be required possibly because write acknowledgement
# occurs after the chunk has been fully written
DATAMESH_CHUNK_WRITE_TIMEOUT = os.getenv("DATAMESH_CHUNK_WRITE_TIMEOUT", 600)
DATAMESH_CHUNK_WRITE_TIMEOUT = (
    None if DATAMESH_CHUNK_WRITE_TIMEOUT == "None" else float(DATAMESH_CHUNK_WRITE_TIMEOUT)
)


class HTTPSession:
    """
    A requests.Session wrapper that is safe to use across forked processes
    Attributes
    ----------
    pool_size : int, optional
        The size of the connection pool, by default None
    headers : dict, optional
        Default headers to include in each request, by default None
    Methods
    -------
    session : requests.Session
        Returns a requests.Session object that is safe to use in the current process
    __getstate__ : dict
        Returns the state of the object for pickling
    __setstate__ : None
        Restores the state of the object from pickling
    """

    def __init__(self, pool_size=os.environ.get("DATAMESH_CONNECTION_POOL_SIZE", 100), headers=None):
        self._session = None
        self._pid = None
        self._pool_size = int(pool_size)
        self._headers = headers

    def _create_session(self):
        session = requests.Session()
        adapter = HTTPAdapter(
            pool_connections=self._pool_size,
            pool_maxsize=self._pool_size
        )
        session.mount('https://', adapter)
        session.mount('http://', adapter)
        if self._headers:
            session.headers.update(self._headers)
        return session

    @property
    def session(self):
        if self._session is None or self._pid != os.getpid():
            self._pid = os.getpid()
            self._session = self._create_session()
        return self._session

    def __getstate__(self):
        state = self.__dict__.copy()
        state["_session"] = None
        state["_pid"] = None
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)

    def request(self, method, url, *args, **kwargs):
        return self.session.request(method, url, *args, **kwargs)


def retried_request(
    url,
    method="GET",
    data=None,
    params=None,
    headers=None,
    retries=8,
    timeout=(DATAMESH_CONNECT_TIMEOUT, DATAMESH_READ_TIMEOUT),
    verify=True,
    http_session: HTTPSession = None,
):
    """
    Retried request function with exponential backoff

    Parameters
    ----------
    url : str
        URL to request
    method : str, optional
        HTTP method, by default "GET"
    data : str, optional
        Request data, by default None
    headers : dict, optional
        Request headers, by default None
    retries : int, optional
        Number of retries, by default 8
    timeout : tupe(float, float), optional
        Request connect and read timeout in seconds, by default (3.05, 10)
    http_session : HTTPSession, optional
        Session object to use for request

    Returns
    -------
    requests.Response
        Response object

    Raises
    ------
    requests.RequestException
        If request fails

    """
    requester = http_session if http_session else requests
    retried = 0
    while retried < retries:
        try:
            resp = requester.request(
                method=method,
                url=url,
                data=data,
                params=params,
                headers=headers,
                timeout=timeout,
                verify=verify,
            )
            # Bad Gateway results in waiting for 10 seconds
            # and retrying
            if resp.status_code == 502:
                sleep(30)
                raise requests.RequestException
        except (
            requests.RequestException,
            requests.ReadTimeout,
            requests.ConnectionError,
            requests.ConnectTimeout,
        ) as e:
            sleep(0.1 * 2**retried)
            retried += 1
            if retried == retries:
                raise DatameshConnectError(
                    f"Failed to connect to {url} after {retries} retries with error: {e}"
                )
        else:
            return resp
