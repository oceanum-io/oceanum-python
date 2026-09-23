from time import sleep, monotonic
from random import uniform
import requests
from requests.adapters import HTTPAdapter
import numpy as np
from .exceptions import DatameshConnectError
import os


# Platform-wide chunk generation budget in seconds. Generating one zarr chunk
# on demand can legitimately take up to this long (driver queries against slow
# upstreams); every read timeout on the chunk path across the datamesh stack
# is derived from this single anchor so the timeout ladder cannot invert.
# This constant is the reference definition of the env contract -- gateway
# services read the same DATAMESH_CHUNK_BUDGET variable (set in deployment
# config) and add their own margins.
DATAMESH_CHUNK_BUDGET = os.getenv("DATAMESH_CHUNK_BUDGET", 900)
DATAMESH_CHUNK_BUDGET = (
    None if DATAMESH_CHUNK_BUDGET == "None" else float(DATAMESH_CHUNK_BUDGET)
)


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

# Timeout in seconds for zarr chunk read operations. A chunk read may block
# for the whole server-side generation of that chunk, so the default sits one
# margin step above the platform chunk budget (the gateway waits budget+90s
# through its internal hops). The old default of 60s guaranteed a retry storm
# on any legitimately slow chunk: the client timed out and re-requested while
# generation was still running.
DATAMESH_CHUNK_READ_TIMEOUT = os.getenv(
    "DATAMESH_CHUNK_READ_TIMEOUT",
    (DATAMESH_CHUNK_BUDGET + 120) if DATAMESH_CHUNK_BUDGET is not None else "None",
)
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

# Lifetime in seconds of a connection pool before it is recreated.
# Useful to force periodic reconnection to pick up DNS changes or avoid stale connections.
# Defaults to None (no expiry).
DATAMESH_CONNECTION_POOL_LIFETIME = os.getenv("DATAMESH_CONNECTION_POOL_LIFETIME", "None")
DATAMESH_CONNECTION_POOL_LIFETIME = (
    None if DATAMESH_CONNECTION_POOL_LIFETIME == "None" else float(DATAMESH_CONNECTION_POOL_LIFETIME)
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
    pool_lifetime : float, optional
        Lifetime of the connection pool in seconds before it is recreated, by default None (no expiry)
    Methods
    -------
    session : requests.Session
        Returns a requests.Session object that is safe to use in the current process
    __getstate__ : dict
        Returns the state of the object for pickling
    __setstate__ : None
        Restores the state of the object from pickling
    """

    def __init__(
        self,
        pool_size=os.environ.get("DATAMESH_CONNECTION_POOL_SIZE", 100),
        headers=None,
        pool_lifetime=DATAMESH_CONNECTION_POOL_LIFETIME,
    ):
        self._session = None
        self._pid = None
        self._pool_size = int(pool_size)
        self._headers = headers
        self._pool_lifetime = pool_lifetime  # seconds, or None for no expiry
        self._session_created_at = None

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
        self._session_created_at = monotonic()
        return session

    def _is_session_expired(self):
        if self._pool_lifetime is None or self._session_created_at is None:
            return False
        return (monotonic() - self._session_created_at) >= self._pool_lifetime

    @property
    def session(self):
        if self._session is None or self._pid != os.getpid() or self._is_session_expired():
            self._pid = os.getpid()
            self._session = self._create_session()
        return self._session

    def __getstate__(self):
        state = self.__dict__.copy()
        state["_session"] = None
        state["_pid"] = None
        state["_session_created_at"] = None
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)

    def request(self, method, url, *args, **kwargs):
        return self.session.request(method, url, *args, **kwargs)


# Gateway statuses worth a bounded retry: the datamesh services classify 503
# as transient (Retry-After may accompany it); 502/504 cover a pod restarting
# or an ingress hop failing. Other statuses -- including 500 -- are terminal
# and returned to the caller untouched.
RETRYABLE_STATUS_CODES = (502, 503, 504)

# Methods safe to re-issue after the server may have started (or finished)
# processing the request: the RFC 7231 idempotent set. POST/PATCH are only
# retried on connection-level failures, where the request never went out.
IDEMPOTENT_METHODS = ("GET", "HEAD", "OPTIONS", "PUT", "DELETE")


def backoff_delay(attempt, resp=None):
    """Jittered exponential backoff delay, honoring a numeric Retry-After.

    Jitter matters: this client is the retry owner for the whole platform,
    and un-jittered backoff makes every client that saw the same failure
    retry in lockstep, re-creating the load spike that caused the failure.
    """
    if resp is not None:
        retry_after = resp.headers.get("Retry-After")
        if retry_after is not None:
            try:
                # Clamp: a malformed negative value must not crash sleep().
                return min(max(float(retry_after), 0.0), 120.0)
            except ValueError:
                pass  # HTTP-date form; fall through to backoff
    return min(0.5 * 2**attempt, 15.0) * uniform(0.5, 1.0)


# urllib3 / stdlib error names that mean the request never reached the server,
# so re-issuing it cannot duplicate work. Matched by NAME rather than imported:
# these have been stable across urllib3 1.x and 2.x, and a hard import would
# couple this client to a transitive dependency's layout.
_NOT_DELIVERED_ERRORS = (
    "NewConnectionError",      # never opened a socket
    "ConnectTimeoutError",     # connect phase timed out
    "NameResolutionError",     # DNS never resolved
    "ProxyError",              # never got past the proxy
)

# ... and the names that mean the connection was established, the request was
# written, and the failure happened afterwards. The server may have done the
# whole job before dying.
_DELIVERED_ERRORS = (
    "ProtocolError",           # urllib3's wrapper for a mid-stream abort
    "ConnectionResetError",    # peer sent RST
    "RemoteDisconnected",      # peer closed without responding
    "IncompleteRead",
    "ChunkedEncodingError",
)


def request_was_delivered(exc, default=True):
    """Whether a transport failure happened *after* the request was delivered.

    This is the difference between a retry that is free and a retry that
    re-runs the work that just killed a server process. A query-engine pod
    OOMKilled mid-request resets the connection with no status code and no
    response bytes -- from the client it looks exactly like a connect failure
    unless the cause chain is inspected. Retrying that lands the same killer
    query on a sibling replica: on 2026-09-22 it took both query-engine pods
    12 seconds apart.

    Note this cannot be decided by "did any response bytes arrive": the
    query-engine builds netCDF/parquet responses in full before sending a
    byte (FileResponse), so the fatal case produces zero bytes.

    `default` applies when the cause chain says nothing recognisable. It
    defaults to True -- assume delivered, so a non-idempotent request is
    surfaced rather than re-issued. Under-retrying costs one error; over-
    retrying cost a production outage.
    """
    if isinstance(exc, requests.exceptions.ConnectTimeout):
        return False
    seen = set()
    node = exc
    while node is not None and id(node) not in seen:
        seen.add(id(node))
        name = type(node).__name__
        if name in _NOT_DELIVERED_ERRORS:
            return False
        if name in _DELIVERED_ERRORS:
            return True
        nxt = node.__cause__ or node.__context__
        if nxt is None:
            args = getattr(node, "args", ())
            nxt = args[0] if args and isinstance(args[0], BaseException) else None
        node = nxt
    return default


def retried_request(
    url,
    method="GET",
    data=None,
    params=None,
    headers=None,
    retries=3,
    timeout=(DATAMESH_CONNECT_TIMEOUT, DATAMESH_READ_TIMEOUT),
    verify=True,
    http_session: HTTPSession = None,
):
    """
    Bounded, jittered retry wrapper around a single datamesh request.

    This client is the *only* layer of the datamesh platform that retries on
    status codes -- the gateway services propagate failures without retrying,
    because every internal retry re-runs potentially expensive driver work.
    Keep the budget here small and jittered, and honor Retry-After.

    Retry semantics:

    - Connect failures (including connect timeouts, DNS and proxy errors):
      always retried -- the request never reached the service.
    - Connection resets *after* the request was delivered: retried for
      idempotent methods only. The server may have completed the work, and if
      that work is what killed it, a retry kills the next replica too.
    - Read timeouts: never retried. Timeouts are sized above the platform's
      own chunk-generation budget, so hitting one means the chain failed;
      re-requesting would only duplicate work still running server-side.
    - 502/503/504: retried for idempotent methods only, honoring a numeric
      Retry-After header. POST/PATCH responses are returned/raised untouched
      so the caller can decide (e.g. Connection._query re-attempts once).
    - Every other status: returned to the caller untouched.

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
        Total number of attempts, by default 3
    timeout : tuple(float, float), optional
        Request connect and read timeout in seconds, by default (3.05, 10)
    http_session : HTTPSession, optional
        Session object to use for request

    Returns
    -------
    requests.Response
        Response object

    Raises
    ------
    DatameshConnectError
        If the request cannot be completed within the retry budget

    """
    requester = http_session if http_session else requests
    attempt = 0
    last_error = None
    while True:
        resp = None
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
        except requests.exceptions.ConnectionError as e:
            # Two very different failures arrive here. A connect failure never
            # reached the server and is free to re-issue for any method. A reset
            # *after* the request was delivered means the server may have done
            # the work -- and if that work is what killed it, retrying hands the
            # same request to a sibling replica. So delivered resets follow the
            # same rule as the mid-response branch below: idempotent only.
            if request_was_delivered(e) and method.upper() not in IDEMPOTENT_METHODS:
                raise DatameshConnectError(
                    f"Request to {url} failed after delivery: {e}"
                )
            last_error = e
        except requests.exceptions.Timeout as e:
            raise DatameshConnectError(
                f"No response from {url} within {timeout[1] if isinstance(timeout, tuple) else timeout}s: {e}"
            )
        except requests.RequestException as e:
            # Transport failure mid-response: the server processed the
            # request, so only idempotent methods may re-issue it.
            if method.upper() not in IDEMPOTENT_METHODS:
                raise DatameshConnectError(f"Request to {url} failed: {e}")
            last_error = e
        else:
            if (
                resp.status_code in RETRYABLE_STATUS_CODES
                and method.upper() in IDEMPOTENT_METHODS
            ):
                last_error = DatameshConnectError(
                    f"{url} returned {resp.status_code}"
                )
            else:
                return resp
        attempt += 1
        if attempt >= retries:
            raise DatameshConnectError(
                f"Failed request to {url} after {retries} attempts with error: {last_error}"
            )
        sleep(backoff_delay(attempt, resp))
