"""Notices about datasources, published on oceanum.io.

Notices (deprecations, changes, outages) are fetched from the oceanum.io admin
service's ``GET /notices/active?datasource=<id>`` endpoint, with the connector's
own credential. Fetching them must never break or slow down the operation that
asked: every error is swallowed (logged at debug level), there are no retries,
a request is given at most NOTICES_TIMEOUT seconds, and answers are cached per
process and datasource for NOTICES_CACHE_TTL seconds.
"""

from __future__ import annotations

import datetime
import hashlib
import logging
import os
import threading
import time
import unicodedata
import warnings
from collections import OrderedDict
from urllib.parse import urlsplit
from concurrent.futures import Future
from concurrent.futures import TimeoutError as FutureTimeoutError

import requests
from pydantic import BaseModel, ValidationError

logger = logging.getLogger(__name__)

# Seconds a notices request (and a caller waiting for one) may take at most.
NOTICES_TIMEOUT = 3.0
# Seconds an answer is kept, per datasource and credential.
NOTICES_CACHE_TTL = 600.0
# Answers kept at most, and notice ids remembered as already shown.
NOTICES_CACHE_SIZE = 256
SHOWN_NOTICES_SIZE = 1024
# Seconds no request is made to an admin service after one timed out or could not
# connect, and after a 429/503 that gave no usable Retry-After.
NOTICES_BACKOFF = 300.0
MAX_RETRY_AFTER = 3600.0

# Severities shown as a NoticeWarning; any other (change, info, or one this client
# does not know yet) is logged at info level.
WARN_SEVERITIES = frozenset({"action_required", "outage"})

# The only Datamesh hosts an admin service is derived for; any other gets none.
ADMIN_FOR_DATAMESH = {
    "datamesh.oceanum.io": "https://admin.oceanum.io",
    "datamesh.oceanum.tech": "https://admin.oceanum.tech",
}
_LOCAL_HOSTS = frozenset({"localhost", "127.0.0.1"})
_DISABLED_VALUES = frozenset({"0", "false", "no", "off"})
# Characters of a server-supplied text shown in a warning or log line at most.
MAX_SHOWN_TEXT = 200
# Limits of an answer, as the admin service sets them, enforced here too: notices
# kept (the newest), and characters of a title and of a body.
MAX_NOTICES = 20
MAX_TITLE = 200
MAX_BODY = 2000


class NoticeWarning(UserWarning):
    """A notice about a datasource that needs the user's attention
    (severity action_required or outage)."""


class Notice(BaseModel):
    """A published, unexpired notice about a datasource.

    Attributes:
        id: Notice id
        type: Notice type, e.g. deprecation
        severity: One of info, change, action_required, outage
        title: Short title
        body: Markdown body. Render it as text or safe markdown, never as raw HTML.
        publish_at: When the notice was published
        expires_at: When the notice expires, if ever
    """

    id: str
    type: str
    severity: str
    title: str
    body: str = ""
    publish_at: datetime.datetime
    expires_at: datetime.datetime | None = None


def notices_enabled(option: bool | None = None) -> bool:
    """Whether notices are checked on load: the connector option when given,
    else the OCEANUM_NOTICES environment variable (off when 0/false/no/off)."""
    if option is not None:
        return bool(option)
    return (
        os.environ.get("OCEANUM_NOTICES", "1").strip().lower() not in _DISABLED_VALUES
    )


def admin_service_url(datamesh_host: str) -> str | None:
    """The admin service url, or None for no notices (it fails closed).

    OCEANUM_ADMIN_SERVICE when set: an https url, or http on localhost or 127.0.0.1;
    any other value is None. Otherwise derived from the Datamesh service host, for
    datamesh.oceanum.io (https://admin.oceanum.io) and datamesh.oceanum.tech
    (https://admin.oceanum.tech) only: any other host is None.
    """
    env = os.environ.get("OCEANUM_ADMIN_SERVICE", "").strip()
    if env:
        try:
            url = urlsplit(env)
            host = url.hostname
        except ValueError:
            return None
        if not host or url.username or url.password or url.query or url.fragment:
            return None
        if url.scheme == "https" or (url.scheme == "http" and host in _LOCAL_HOSTS):
            return env.rstrip("/")
        return None
    return ADMIN_FOR_DATAMESH.get((datamesh_host or "").lower())


def normalise_datasource_id(datasource_id: object) -> str | None:
    """The datasource id a notices request and its cache use: stripped of
    surrounding whitespace; None if it is not a non-empty string."""
    if not isinstance(datasource_id, str):
        return None
    return datasource_id.strip() or None


def credential_headers(token: str) -> dict[str, str]:
    """The headers sending the connector's credential to the admin service:
    a bearer JWT as Authorization, a Datamesh token as X-DATAMESH-TOKEN."""
    if token.startswith("Bearer "):
        return {"Authorization": token}
    return {"X-DATAMESH-TOKEN": token}


class _NoticesState:
    """Per process state: cached answers, admin services backed off, notices shown."""

    def __init__(self) -> None:
        self.pid = os.getpid()
        self.lock = threading.Lock()
        # key -> (expiry on the monotonic clock, Future[list[Notice]])
        self.cache: OrderedDict[tuple, tuple[float, Future]] = OrderedDict()
        # admin url -> no request before this time (monotonic clock)
        self.backoff: dict[str, float] = {}
        self.shown: OrderedDict[str, None] = OrderedDict()


_state = _NoticesState()


def _get_state() -> _NoticesState:
    """The state of this process: a forked child starts afresh, since the threads
    that would complete its parent's pending answers do not exist in it."""
    global _state
    if _state.pid != os.getpid():
        _state = _NoticesState()
    return _state


def _clear() -> None:
    """Forget every cached answer, back-off and shown notice (for tests)."""
    global _state
    _state = _NoticesState()


def _clip(text: str, size: int) -> str:
    return text if len(text) <= size else text[: size - 3] + "..."


def _clean(text: str) -> str:
    """A server-supplied text made safe for one warning or log line: control and
    other non-printing characters (newlines, terminal escapes) become spaces,
    whitespace runs collapse, and it is cut to MAX_SHOWN_TEXT characters."""
    printable = "".join(
        " " if unicodedata.category(c).startswith(("C", "Z")) else c for c in str(text)
    )
    return _clip(" ".join(printable.split()), MAX_SHOWN_TEXT)


def _retry_after(resp: requests.Response) -> float:
    try:
        seconds = float(resp.headers.get("Retry-After", ""))
    except (TypeError, ValueError):
        return NOTICES_BACKOFF
    return min(max(seconds, 1.0), MAX_RETRY_AFTER)


def _publish_key(notice: Notice) -> datetime.datetime:
    """A notice's publish time, comparable whether the server sent it with a
    timezone or not (naive is taken as UTC)."""
    when = notice.publish_at
    if when.tzinfo is None:
        return when.replace(tzinfo=datetime.timezone.utc)
    return when


def _fetch(
    state: _NoticesState,
    admin_url: str,
    datasource_id: str,
    headers: dict[str, str],
    verify: bool,
) -> list[Notice]:
    """One request for a datasource's notices. Never raises: any failure is no
    notices."""
    try:
        resp = requests.get(
            f"{admin_url}/notices/active",
            params={"datasource": datasource_id},
            headers=headers,
            timeout=NOTICES_TIMEOUT,
            verify=verify,
            # A redirect would carry the credential to wherever it points.
            allow_redirects=False,
        )
    except (requests.Timeout, requests.ConnectionError) as e:
        logger.debug("Notices request to %s failed: %s", admin_url, type(e).__name__)
        with state.lock:
            state.backoff[admin_url] = time.monotonic() + NOTICES_BACKOFF
        return []
    except Exception as e:
        logger.debug("Notices request to %s failed: %s", admin_url, type(e).__name__)
        return []
    try:
        if resp.status_code in (429, 503):
            with state.lock:
                state.backoff[admin_url] = time.monotonic() + _retry_after(resp)
        if resp.status_code != 200:
            # 404 is no access, an unknown datasource or no notices: all "none".
            logger.debug(
                "Notices for datasource %s: HTTP %s", datasource_id, resp.status_code
            )
            return []
        notices = []
        for item in resp.json().get("items") or []:
            try:
                notice = Notice.model_validate(item)
                notice.title = _clip(notice.title, MAX_TITLE)
                notice.body = _clip(notice.body, MAX_BODY)
                notices.append(notice)
            except (ValidationError, TypeError, ValueError):
                logger.debug("Ignoring a malformed notice for %s", datasource_id)
        notices.sort(key=_publish_key, reverse=True)
        return notices[:MAX_NOTICES]
    except Exception as e:
        logger.debug(
            "Unreadable notices answer for %s: %s", datasource_id, type(e).__name__
        )
        return []


def request_notices(
    admin_url: str | None, datasource_id: str, token: str, verify: bool = True
) -> Future | None:
    """Start, or reuse, the fetch of a datasource's notices.

    Returns a Future of the list of notices (it never holds an exception), or None
    when there is nothing to fetch: no admin service, or it is being backed off.
    Never raises, never blocks on the network: the request runs in a daemon thread.
    """
    try:
        datasource_id = normalise_datasource_id(datasource_id)
        if not admin_url or not datasource_id or not token:
            return None
        state = _get_state()
        digest = hashlib.sha256(token.encode()).hexdigest()[:16]
        key = (admin_url, datasource_id, digest)
        now = time.monotonic()
        with state.lock:
            cached = state.cache.get(key)
            if cached is not None and cached[0] > now:
                state.cache.move_to_end(key)
                return cached[1]
            if state.backoff.get(admin_url, 0.0) > now:
                return None
            future: Future = Future()
            state.cache[key] = (now + NOTICES_CACHE_TTL, future)
            state.cache.move_to_end(key)
            while len(state.cache) > NOTICES_CACHE_SIZE:
                state.cache.popitem(last=False)
        headers = credential_headers(token)

        def run() -> None:
            notices: list[Notice] = []
            try:
                notices = _fetch(state, admin_url, datasource_id, headers, verify)
            finally:
                future.set_result(notices)

        try:
            threading.Thread(target=run, name="oceanum-notices", daemon=True).start()
        except Exception:
            # A cached answer must never stay pending: callers would wait on it.
            future.set_result([])
            raise
        return future
    except Exception as e:
        logger.debug("Could not request notices: %s", type(e).__name__)
        return None


def wait_notices(future: Future | None, timeout: float | None = None) -> list[Notice]:
    """The notices of a request, waiting at most timeout seconds (NOTICES_TIMEOUT by
    default) in all, whatever the request's own phases take; none if not there by
    then."""
    if future is None:
        return []
    if timeout is None:
        timeout = NOTICES_TIMEOUT
    try:
        # Copies: the cached answer is shared by every caller.
        return [n.model_copy() for n in future.result(timeout=max(timeout, 0.0))]
    except FutureTimeoutError:
        logger.debug("Notices not received within %.1f s", timeout)
        return []
    except Exception as e:
        logger.debug("Could not get notices: %s", type(e).__name__)
        return []


def show_notices(
    datasource_id: str, notices: list[Notice], stacklevel: int = 2
) -> None:
    """Show each notice not shown yet for this datasource in this process (only
    notices actually received count as shown): a NoticeWarning for
    action_required and outage, one info log line for any other severity."""
    state = _get_state()
    datasource_id = normalise_datasource_id(datasource_id) or ""
    for notice in notices:
        with state.lock:
            key = f"{datasource_id}\x00{notice.id}"
            if key in state.shown:
                continue
            state.shown[key] = None
            while len(state.shown) > SHOWN_NOTICES_SIZE:
                state.shown.popitem(last=False)
        severity = _clean(notice.severity.replace("_", " "))
        title = _clean(notice.title)
        if notice.severity in WARN_SEVERITIES:
            warnings.warn(
                f"Datasource '{datasource_id}' ({severity}): {title}. "
                f"See Connector.get_notices('{datasource_id}') for details.",
                NoticeWarning,
                stacklevel=stacklevel + 1,
            )
        else:
            logger.info(
                "Datasource '%s' notice (%s): %s", datasource_id, severity, title
            )
