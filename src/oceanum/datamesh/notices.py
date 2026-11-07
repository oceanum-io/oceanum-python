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
# Seconds no request is made to an admin service after any other failure (another
# status than 200/404, an unreadable answer).
NOTICES_ERROR_BACKOFF = 60.0
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
        # key -> (expiry on the monotonic clock, Future[list[Notice]]): a fetch in
        # flight, or a good answer (200 or 404) until it expires. Never a failure.
        self.cache: OrderedDict[tuple, tuple[float, Future]] = OrderedDict()
        # key -> the last good answer, kept past its expiry: returned while the
        # admin service is backed off.
        self.good: OrderedDict[tuple, list[Notice]] = OrderedDict()
        # admin url -> no request before this time (monotonic clock)
        self.backoff: dict[str, float] = {}
        self.shown: OrderedDict[str, None] = OrderedDict()

    def back_off(self, admin_url: str, seconds: float) -> None:
        with self.lock:
            until = time.monotonic() + seconds
            self.backoff[admin_url] = max(self.backoff.get(admin_url, 0.0), until)


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
    """One request for a datasource's notices: the notices of a 200, [] for a 404,
    None for any failure, after which the admin service is backed off. Never
    raises."""
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
        state.back_off(admin_url, NOTICES_BACKOFF)
        return None
    except Exception as e:
        logger.debug("Notices request to %s failed: %s", admin_url, type(e).__name__)
        state.back_off(admin_url, NOTICES_ERROR_BACKOFF)
        return None
    try:
        if resp.status_code == 404:
            # No access, an unknown datasource or no notices: all an answer of none.
            return []
        if resp.status_code != 200:
            logger.debug(
                "Notices for datasource %s: HTTP %s", datasource_id, resp.status_code
            )
            if resp.status_code in (429, 503):
                state.back_off(admin_url, _retry_after(resp))
            else:
                state.back_off(admin_url, NOTICES_ERROR_BACKOFF)
            return None
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
        state.back_off(admin_url, NOTICES_ERROR_BACKOFF)
        return None


def _remember(store: OrderedDict, key: tuple, value: object) -> None:
    """Put a value in a bounded store, newest last, dropping the oldest beyond
    NOTICES_CACHE_SIZE. The caller holds the state lock."""
    store[key] = value
    store.move_to_end(key)
    while len(store) > NOTICES_CACHE_SIZE:
        store.popitem(last=False)


def request_notices(
    admin_url: str | None, datasource_id: str, token: str, verify: bool = True
) -> Future | None:
    """Start, or reuse, the fetch of a datasource's notices.

    Returns a Future of the list of notices (it never holds an exception), or None
    when there is nothing to fetch: no admin service, or it is backed off with no
    earlier good answer (with one, a Future of that answer). Only a 200 or a 404 is
    cached, for NOTICES_CACHE_TTL; a failure gives the callers waiting [], drops the
    fetch from the cache and backs the admin service off. Never raises, never blocks
    on the network: the request runs in a daemon thread.
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
            if cached is not None and (not cached[1].done() or cached[0] > now):
                state.cache.move_to_end(key)
                return cached[1]
            if state.backoff.get(admin_url, 0.0) > now:
                good = state.good.get(key)
                if good is None:
                    return None
                answer: Future = Future()
                answer.set_result(good)
                return answer
            future: Future = Future()
            _remember(state.cache, key, (now + NOTICES_CACHE_TTL, future))
        headers = credential_headers(token)

        def settle(notices: list[Notice] | None) -> None:
            with state.lock:
                if notices is not None:
                    _remember(
                        state.cache,
                        key,
                        (time.monotonic() + NOTICES_CACHE_TTL, future),
                    )
                    _remember(state.good, key, notices)
                else:
                    cached = state.cache.get(key)
                    if cached is not None and cached[1] is future:
                        del state.cache[key]
            future.set_result(notices if notices is not None else [])

        def run() -> None:
            notices: list[Notice] | None = None
            try:
                notices = _fetch(state, admin_url, datasource_id, headers, verify)
            finally:
                settle(notices)

        try:
            threading.Thread(target=run, name="oceanum-notices", daemon=True).start()
        except Exception:
            # A fetch that never ran must not stay pending: callers would wait on it.
            state.back_off(admin_url, NOTICES_ERROR_BACKOFF)
            settle(None)
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
        key = f"{datasource_id}\x00{notice.id}"
        with state.lock:
            if key in state.shown:
                continue
        severity = _clean(notice.severity.replace("_", " "))
        title = _clean(notice.title)
        if notice.severity in WARN_SEVERITIES:
            # Raises under an "error" warnings filter: the notice then stays unshown.
            warnings.warn(
                f"Datasource '{datasource_id}' ({severity}): {title}. "
                f"See Connector.get_notices('{datasource_id}') for details.",
                NoticeWarning,
                stacklevel=stacklevel + 1,
            )
        elif logger.isEnabledFor(logging.INFO):
            logger.info(
                "Datasource '%s' notice (%s): %s", datasource_id, severity, title
            )
        else:
            # Not displayed, so not shown: it is shown once INFO logging is on.
            continue
        with state.lock:
            state.shown[key] = None
            state.shown.move_to_end(key)
            while len(state.shown) > SHOWN_NOTICES_SIZE:
                state.shown.popitem(last=False)
