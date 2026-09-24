from time import sleep, monotonic
from random import uniform
import requests
from requests.adapters import HTTPAdapter
import numpy as np
from .exceptions import DatameshConnectError, DatameshUnavailableError
import os
import warnings


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

# Timeout in seconds for metadata-server requests (catalog search, datasource
# metadata). Separate from DATAMESH_READ_TIMEOUT because a catalog search is not
# the "small json payload" that constant assumes: it runs a hybrid RRF search
# with an embedding lookup, measured at 2-4 s against prod, on a server whose
# CPU sits at its autoscaling target from embedding regeneration. 10 s left
# only ~2.5x headroom and a routine spike tipped it over.
DATAMESH_METADATA_READ_TIMEOUT = os.getenv("DATAMESH_METADATA_READ_TIMEOUT", 20)
DATAMESH_METADATA_READ_TIMEOUT = (
    None if DATAMESH_METADATA_READ_TIMEOUT == "None"
    else float(DATAMESH_METADATA_READ_TIMEOUT)
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


# Base delay in seconds before re-attempting, or raising on, a gateway-level
# failure on the query path. Deliberately long.
#
# Two things justify the size. A 502 means the pod serving us died; the
# sibling needs time to pick up the slack, and a sub-second retry arrives
# while the ingress is still settling. More importantly this delay is taken
# *before raising*, so it rate-limits the caller's own retry loop: a wrapper
# that retries our call cannot iterate faster than this, whatever its own
# backoff says. That is the only lever we have over client code we do not
# own, and with AI-generated callers the norm, assuming a naive
# retry-on-any-exception wrapper is the safe default.
DATAMESH_GATEWAY_RETRY_DELAY = float(os.getenv("DATAMESH_GATEWAY_RETRY_DELAY", 30))

# Floor for a server-supplied Retry-After on the gateway path, for the same
# reason: a service that asks us to come straight back must not be able to
# switch the pacing off.
DATAMESH_GATEWAY_RETRY_MIN = float(os.getenv("DATAMESH_GATEWAY_RETRY_MIN", 5))

# Number of attempts the query path makes at a 502. 2 = one re-attempt.
# Set to 1 to disable re-attempting entirely (the query is then surfaced on
# the first gateway failure).
try:
    DATAMESH_QUERY_RETRIES = int(os.getenv("DATAMESH_QUERY_RETRIES", 2))
except ValueError:
    # A typo in an env var must not make `import oceanum` fail.
    warnings.warn(
        "DATAMESH_QUERY_RETRIES is not an integer; using the default of 2"
    )
    DATAMESH_QUERY_RETRIES = 2

# What we advertise as `retry_after` when the server gives us no guidance.
# Recovering from a dead pod, or from whatever load produced the failure,
# takes minutes rather than seconds.
DATAMESH_UNAVAILABLE_RETRY_AFTER = float(
    os.getenv("DATAMESH_UNAVAILABLE_RETRY_AFTER", 300)
)

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
    hinted = retry_after_seconds(resp)
    if hinted is not None:
        # Jitter this too. An un-jittered Retry-After is worse than none: every
        # client that saw the same overload wakes in the same instant.
        return hinted * uniform(0.75, 1.0)
    return min(0.5 * 2**attempt, 15.0) * uniform(0.5, 1.0)


def retry_after_seconds(resp):
    """Numeric Retry-After from a response, clamped, or None.

    The HTTP-date form is not decoded -- no datamesh service emits it, and
    guessing wrong is worse than falling back to our own backoff.
    """
    if resp is None:
        return None
    value = resp.headers.get("Retry-After")
    if value is None:
        return None
    try:
        # Clamp: a malformed negative value must not crash sleep().
        return min(max(float(value), 0.0), 120.0)
    except (TypeError, ValueError):
        return None  # HTTP-date form; caller falls back to backoff


def unavailable_error(url, resp, attempted=0):
    """Build the DatameshUnavailableError for a gateway-level failure.

    The message is written to be acted on by whoever reads it -- increasingly
    an agent generating or repairing client code rather than a person. So it
    says what to do, not just what happened: how long to wait, and when
    waiting cannot possibly help.
    """
    status = getattr(resp, "status_code", None)
    retry_after = retry_after_seconds(resp)
    if retry_after is None or retry_after <= 0:
        retry_after = DATAMESH_UNAVAILABLE_RETRY_AFTER
    tried = f" Re-attempted {attempted} time(s) already." if attempted else ""
    if status == 502:
        detail = (
            "the datamesh instance handling this request became unavailable "
            "before it could answer."
        )
    else:
        detail = (
            "no datamesh instance was able to serve this request. This can "
            "mean the service is restarting, a dependency is unreachable, or "
            "the request exceeded what a single worker can process -- in that "
            "last case it will fail again however long you wait, and the fix "
            "is to make the request smaller (shorter timerange, smaller area, "
            "fewer variables) rather than to repeat it unchanged."
        )
    return DatameshUnavailableError(
        f"Datamesh returned {status} for {url}: {detail}{tried} "
        f"Do not retry sooner than {retry_after:.0f}s.",
        retry_after=retry_after,
        status_code=status,
    )


def gateway_retry_delay(resp=None):
    """Delay before re-attempting, or raising on, a gateway failure.

    Honours Retry-After when the service offers one, otherwise a jittered
    delay around DATAMESH_GATEWAY_RETRY_DELAY. See that constant for why it
    is long: it paces the caller's retry loop as much as our own.
    """
    hinted = retry_after_seconds(resp)
    # A zero or negative Retry-After would cancel the pacing wait entirely,
    # which is the one thing this delay exists to provide. Treat it as absent.
    if hinted is not None and hinted > 0:
        return max(hinted, DATAMESH_GATEWAY_RETRY_MIN) * uniform(0.75, 1.0)
    return DATAMESH_GATEWAY_RETRY_DELAY * uniform(0.5, 1.0)


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

# A read timeout that happens *after* the response headers arrive does not
# surface as requests.exceptions.ReadTimeout. requests re-wraps urllib3's
# ReadTimeoutError as a ConnectionError while iterating the body
# (requests/models.py, `except ReadTimeoutError as e: raise ConnectionError(e)`),
# so it lands in the same branch as a mid-stream reset and is indistinguishable
# from one without inspecting the chain.
_READ_TIMEOUT_ERRORS = ("ReadTimeoutError",)

# RequestExceptions that mean "the response started and then broke", as opposed
# to the deterministic ones (MissingSchema, InvalidURL, InvalidHeader,
# TooManyRedirects) where a second identical attempt fails identically.
_MID_RESPONSE_EXCEPTIONS = (
    requests.exceptions.ChunkedEncodingError,
    requests.exceptions.ContentDecodingError,
)


def _walk_causes(exc):
    """Yield exc and its cause chain, once each, cycle-safe."""
    seen = set()
    node = exc
    while node is not None and id(node) not in seen:
        seen.add(id(node))
        yield node
        nxt = node.__cause__ or node.__context__
        if nxt is None:
            # urllib3's MaxRetryError keeps the real cause on `.reason`. It is
            # normally raised `from reason` so __cause__ is set too, but not
            # when it is constructed rather than raised -- follow the attribute
            # so classification never depends on that.
            reason = getattr(node, "reason", None)
            if isinstance(reason, BaseException):
                nxt = reason
        if nxt is None:
            args = getattr(node, "args", ())
            nxt = args[0] if args and isinstance(args[0], BaseException) else None
        node = nxt


# The peer's certificate being rejected is a configuration state, not a blip:
# it will fail identically forever, so it is terminal for every method.
#
# Deliberately NOT here: any attempt to decide whether a *non-certificate* TLS
# failure happened before or after the request was written. urllib3 wraps an SSL
# error from conn.getresponse() -- i.e. after the body was fully sent -- in the
# same MaxRetryError shape as a failed handshake (connectionpool.py:535 then
# :824), so the two are indistinguishable from the exception alone. Guessing
# "handshake" would re-send a delivered POST, which is the one mistake this
# module exists to avoid. Non-certificate TLS failures therefore keep the
# cautious default: idempotent methods retry, others do not.
_CERTIFICATE_ERRORS = ("SSLCertVerificationError", "CertificateError")


def certificate_verification_failed(exc):
    """Whether this failure is the peer's certificate being rejected."""
    return any(
        type(n).__name__ in _CERTIFICATE_ERRORS for n in _walk_causes(exc)
    )


def response_body_timed_out(exc):
    """Whether this failure is a read timeout *after* headers arrived.

    Distinguishing this from a mid-stream reset matters because the two want
    opposite handling. Both mean the server accepted the request and started
    answering -- but a body timeout says the response existed and we failed to
    collect it, which for a query means query-engine already built and cached
    the result. Re-requesting it is a cache hit, not a recomputation.
    """
    return any(type(n).__name__ in _READ_TIMEOUT_ERRORS for n in _walk_causes(exc))


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
    for node in _walk_causes(exc):
        name = type(node).__name__
        if name in _NOT_DELIVERED_ERRORS:
            return False
        if name in _DELIVERED_ERRORS:
            return True
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
    retry_read_timeout=False,
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
    - Connection resets *after* the request was delivered, with no response:
      never retried for non-idempotent methods. The server may have completed
      the work, and if that work is what killed it, a retry kills the next
      replica too.
    - Failures *after the response headers arrived* -- a body read timeout or
      an aborted transfer: one re-attempt, any method. The answer existed, so
      collecting it again is cheap (query-engine serves the second attempt
      from its shared cache) and the risk that motivates the rule above does
      not apply.
    - TLS certificate verification failures: never retried, and reported as
      such. The certificate will not become valid on a second attempt.
    - Other TLS failures: treated like any other transport failure -- the
      exception cannot tell a failed handshake from an SSL error while reading
      the response, so idempotent methods retry and others do not.
    - Pre-header read timeouts: not retried by default. Where the timeout is
      derived from the platform's own generation budget (chunk reads, staging,
      downloads), hitting it means the chain failed and re-requesting would
      duplicate work still running server-side.
      That reasoning does not hold for an endpoint whose timeout is simply a
      short arbitrary number -- the metadata server, where a routine latency
      spike is not a failed chain and there is no expensive in-flight work to
      duplicate. Those callers pass `retry_read_timeout=True`, which allows a
      read timeout into the normal bounded budget for idempotent methods only.
    - 502/503/504: retried for idempotent methods only, honoring a numeric
      Retry-After header. POST/PATCH responses are returned untouched so the
      caller can apply a policy this function cannot -- see
      Connection._query, which waits far longer and treats 502 and 503
      differently.
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
    retry_read_timeout : bool, optional
        Allow a pre-header read timeout to be retried, for idempotent methods
        only. Set this where the read timeout is a short arbitrary value rather
        than one derived from a server-side work budget. Default False.

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
    # A mid-response failure gets one re-attempt at most, independently of the
    # `retries` budget: the response existed, so a second collection attempt is
    # cheap (query-engine serves it from its shared cache), but repeated
    # attempts against a connection that keeps dying mid-body are just a stall.
    mid_response_attempts = 0
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
            # Several very different failures arrive here, and requests gives
            # them all the same class.
            if certificate_verification_failed(e):
                # Deterministic: the certificate will be just as invalid next
                # time. Say so, because the generic wording below would send
                # the reader looking for a transient fault.
                raise DatameshConnectError(
                    f"TLS certificate verification failed for {url}. A retry "
                    f"cannot help -- check the service certificate, the system "
                    f"CA bundle, or whether something is intercepting TLS: {e}"
                )
            if response_body_timed_out(e):
                # Headers arrived, the body did not. The server built the
                # answer; we failed to collect it. One re-attempt, any method:
                # for a query that is a cache hit rather than a recomputation.
                if mid_response_attempts >= 1:
                    raise DatameshConnectError(
                        f"Response from {url} stalled mid-body twice: {e}"
                    )
                mid_response_attempts += 1
                last_error = e
            elif request_was_delivered(e) and method.upper() not in IDEMPOTENT_METHODS:
                # A reset *after* delivery with no response at all. The server
                # may have died doing the work -- and if that work is what
                # killed it, retrying hands the same request to a sibling.
                raise DatameshConnectError(
                    f"Request to {url} may have been delivered before the "
                    f"connection failed, and {method.upper()} is not safe to "
                    f"repeat automatically. Re-run it if that is safe: {e}"
                )
            else:
                # Connect failure: never reached the server, free to re-issue.
                last_error = e
        except requests.exceptions.Timeout as e:
            # Pre-header timeout: nothing was produced. Whether that is worth
            # another attempt depends on where the timeout came from -- see the
            # retry_read_timeout note in the docstring. ConnectTimeout never
            # reaches here; it is a ConnectionError subclass caught above.
            if retry_read_timeout and method.upper() in IDEMPOTENT_METHODS:
                last_error = e
            else:
                # Report both values rather than guessing which fired: a TLS
                # handshake that hangs surfaces as a ReadTimeout carrying the
                # *connect* timeout, so naming timeout[1] alone was misleading
                # (a chunk read would claim 1020 s after 3 s).
                _c, _r = timeout if isinstance(timeout, tuple) else (None, timeout)
                raise DatameshConnectError(
                    f"No response from {url} (connect timeout {_c}s, "
                    f"read timeout {_r}s): {e}"
                )
        except _MID_RESPONSE_EXCEPTIONS as e:
            # The response body was aborted part-way through (e.g. a chunked
            # transfer cut short). Same reasoning as the body-timeout case
            # above: the answer existed, so one re-attempt, any method.
            if mid_response_attempts >= 1:
                raise DatameshConnectError(f"Request to {url} failed: {e}")
            mid_response_attempts += 1
            last_error = e
        except requests.RequestException as e:
            # Everything else reaching here is deterministic -- a malformed
            # URL, a bad header, a redirect loop. Repeating it cannot help.
            raise DatameshConnectError(f"Request to {url} failed: {e}")
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
