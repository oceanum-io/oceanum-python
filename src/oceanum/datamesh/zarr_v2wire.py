"""Zarr **v2-wire** Store adapter for the Datamesh under zarr-python 3.

This is the drop-in replacement for the legacy ``zarr.ZarrClient``
(``collections.abc.MutableMapping``), which no longer functions as a store
under zarr-python 3. ``DatameshV2WireStore`` is a real
``zarr.abc.store.Store`` that speaks the **exact** HTTP protocol the old
``ZarrClient`` spoke against the existing gateway ``/zarr`` and
``/zarr/query`` endpoints:

* URLs: ``{gateway}/zarr[/query]/{datasource}/{quoted_key}``
* verbs: ``GET`` (read), ``HEAD`` (exists), ``POST`` (default write verb),
  ``DELETE`` (delete / clear)
* headers: ``connection._auth_headers`` + session header
  (``X-DATAMESH-SESSIONID``) + optional ``cache-control: no-transform,no-cache``
  (nocache), ``X-PARAMETERS`` (JSON), ``X-DATAMESH-STORAGE-BACKEND``
* error mapping (``_request``): 401 → auth error, 410 → session-deleted,
  >=500 → server error
* directory listing by scraping the gateway HTML autoindex ``<a href>`` links.

Server bytes/keys/``.zmetadata`` are untouched — this is used with
``xarray.open_zarr(store, zarr_format=2, consolidated=True)`` so zarr-python 3
reads and writes the same v2-format stores the server already understands.

Transport is ``utils.retried_request`` over a fork-safe ``utils.HTTPSession``.
Async store methods wrap the sync transport with ``asyncio.to_thread`` (same
pattern as ``zarr_v3.DatameshV3Store``).
"""

import asyncio
import datetime
import json
import re
import urllib.parse
from typing import Dict, Optional

import requests

from .exceptions import DatameshConnectError, DatameshWriteError
from .utils import (
    retried_request,
    HTTPSession,
    DATAMESH_CONNECT_TIMEOUT,
    DATAMESH_CHUNK_READ_TIMEOUT,
    DATAMESH_CHUNK_WRITE_TIMEOUT,
)

__all__ = ["make_v2wire_store", "build_v2wire_headers"]

# href scraper for the gateway HTML autoindex (identical regex to the legacy
# ZarrClient.__iter__).
_HREF_RE = re.compile(
    r"""<(a|A)\s+(?:[^>]*?\s+)?(href|HREF)=["'](?P<url>[^"']+)"""
)


def _json_serial(obj):
    """JSON serializer for objects not serializable by default json code."""
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


def build_v2wire_headers(
    connection,
    session,
    parameters=None,
    nocache=False,
    storage_backend=None,
):
    """Assemble the exact header set the legacy ``ZarrClient`` used."""
    headers = {**connection._auth_headers}
    headers = session.add_header(headers)
    if nocache:
        headers["cache-control"] = "no-transform,no-cache"
    if parameters:
        headers["X-PARAMETERS"] = json.dumps(parameters, default=_json_serial)
    if storage_backend is not None:
        headers["X-DATAMESH-STORAGE-BACKEND"] = storage_backend
    return headers


def _make_store_class():
    """Build the Store subclass lazily so the module imports cleanly.

    (zarr-python 3 is required to *use* the class; importing the module does
    not require it, mirroring ``zarr_v3._make_store_class``.)
    """
    from zarr.abc.store import (
        ByteRequest,
        OffsetByteRequest,
        RangeByteRequest,
        Store,
        SuffixByteRequest,
    )
    from zarr.core.buffer import Buffer, BufferPrototype

    class DatameshV2WireStore(Store):
        """zarr v3 ``Store`` speaking the legacy v2 gateway wire protocol."""

        def __init__(
            self,
            proxy_base: str,
            datasource: str,
            headers: Dict[str, str],
            http_session: HTTPSession,
            *,
            api: str = "zarr",
            method: str = "post",
            read_only: bool = False,
            retries: int = 10,
            verify: bool = True,
            connect_timeout=DATAMESH_CONNECT_TIMEOUT,
            read_timeout=DATAMESH_CHUNK_READ_TIMEOUT,
            write_timeout=DATAMESH_CHUNK_WRITE_TIMEOUT,
        ):
            # query api is inherently read-only.
            if api == "query":
                read_only = True
            super().__init__(read_only=read_only)
            self.datasource = datasource
            self.api = api
            self.method = method.upper()
            self._base = f"{proxy_base.rstrip('/')}/{datasource}"
            self._headers = headers
            self._http = http_session
            self.retries = retries
            self.verify = verify
            self.connect_timeout = connect_timeout
            self.read_timeout = read_timeout
            self.write_timeout = write_timeout

        def __eq__(self, other):
            return self is other

        __hash__ = object.__hash__

        def with_read_only(self, read_only: bool = False):
            """Return a shallow clone with a different read_only flag.

            zarr-python opens stores read-only for read paths via this hook.
            The query api stays read-only regardless.
            """
            clone = self.__class__(
                self._base.rsplit("/", 1)[0],
                self.datasource,
                self._headers,
                self._http,
                api=self.api,
                method=self.method,
                read_only=read_only,
                retries=self.retries,
                verify=self.verify,
                connect_timeout=self.connect_timeout,
                read_timeout=self.read_timeout,
                write_timeout=self.write_timeout,
            )
            return clone

        def __repr__(self):
            return (
                f"DatameshV2WireStore(api={self.api!r}, "
                f"datasource={self.datasource!r})"
            )

        # -- transport ---------------------------------------------------------

        def _request(
            self,
            path,
            method="GET",
            data=None,
            extra_headers=None,
            connect_timeout=None,
            read_timeout=None,
        ):
            """Perform a retried request and apply the legacy error mapping."""
            headers = self._headers
            if extra_headers:
                headers = {**self._headers, **extra_headers}
            try:
                resp = retried_request(
                    url=path,
                    method=method,
                    data=data,
                    headers=headers,
                    retries=self.retries,
                    timeout=(
                        connect_timeout or self.connect_timeout,
                        read_timeout or self.read_timeout,
                    ),
                    verify=self.verify,
                    http_session=self._http,
                )
            except requests.RequestException as e:
                raise DatameshConnectError(str(e))

            if resp.status_code == 401:
                raise DatameshConnectError(f"Not Authorized {resp.text}")
            if resp.status_code == 410:
                raise DatameshConnectError(
                    "Datasource no longer exists or was deleted within "
                    "your session"
                )
            if resp.status_code >= 500:
                raise DatameshConnectError(
                    f"Server error {resp.status_code}: {resp.text}"
                )
            return resp

        def _url(self, key: str) -> str:
            encoded = urllib.parse.quote(key, safe="/")
            return f"{self._base}/{encoded}"

        @staticmethod
        def _slice_bytes(
            content: bytes, byte_range: Optional[ByteRequest]
        ) -> bytes:
            """Slice full-object bytes client-side.

            The legacy v2 gateway does not honour HTTP Range, so (as the old
            ZarrClient did) we always fetch the whole object and slice locally
            — correct regardless of server Range support.
            """
            if byte_range is None:
                return content
            if isinstance(byte_range, RangeByteRequest):
                return content[byte_range.start:byte_range.end]
            if isinstance(byte_range, OffsetByteRequest):
                return content[byte_range.offset:]
            if isinstance(byte_range, SuffixByteRequest):
                return content[-byte_range.suffix:]
            raise TypeError(f"Unsupported byte range: {byte_range!r}")

        # -- reads -------------------------------------------------------------

        async def get(
            self,
            key: str,
            prototype: BufferPrototype,
            byte_range: Optional[ByteRequest] = None,
        ) -> Optional[Buffer]:
            resp = await asyncio.to_thread(
                self._request,
                self._url(key),
                connect_timeout=self.connect_timeout,
                read_timeout=self.read_timeout,
            )
            # Missing key (or any non-success short of the raised error codes)
            # is a cache/key miss -> None (legacy __getitem__ raised KeyError).
            if resp.status_code >= 300:
                return None
            return prototype.buffer.from_bytes(
                self._slice_bytes(resp.content, byte_range)
            )

        async def get_partial_values(self, prototype, key_ranges):
            return [
                await self.get(key, prototype, rng) for key, rng in key_ranges
            ]

        async def exists(self, key: str) -> bool:
            resp = await asyncio.to_thread(
                self._request,
                self._url(key),
                method="HEAD",
                connect_timeout=self.connect_timeout,
                read_timeout=self.read_timeout,
            )
            return resp.status_code == 200

        # -- writes ------------------------------------------------------------

        @property
        def supports_writes(self) -> bool:
            return not self.read_only

        @property
        def supports_partial_writes(self) -> bool:
            return False

        async def set(self, key: str, value: Buffer) -> None:
            if self.api == "query":
                raise DatameshConnectError(
                    "Query api does not support write operations"
                )
            self._check_writable()
            resp = await asyncio.to_thread(
                self._request,
                self._url(key),
                method=self.method,
                data=value.to_bytes(),
                connect_timeout=self.write_timeout,
                read_timeout=self.write_timeout,
            )
            if resp.status_code >= 300:
                raise DatameshWriteError(
                    f"Failed to write {key}: {resp.status_code} - {resp.text}"
                )

        async def set_if_not_exists(self, key: str, value: Buffer) -> None:
            if not await self.exists(key):
                await self.set(key, value)

        async def set_partial_values(self, key_start_values):
            raise NotImplementedError

        # -- deletes -----------------------------------------------------------

        @property
        def supports_deletes(self) -> bool:
            return not self.read_only

        async def delete(self, key: str) -> None:
            if self.api == "query":
                raise DatameshConnectError(
                    "Query api does not support delete operations"
                )
            await asyncio.to_thread(
                self._request,
                self._url(key),
                method="DELETE",
                connect_timeout=self.connect_timeout,
                read_timeout=10,
            )

        async def clear(self) -> None:
            """Wipe the whole store.

            Matches the legacy ``ZarrClient.clear()`` which issued a single
            ``DELETE`` on the empty key (``{proxy}/{datasource}/``); the
            gateway interprets that as "clear the datasource store".
            """
            if self.api == "query":
                raise DatameshConnectError(
                    "Query api does not support delete operations"
                )
            await asyncio.to_thread(
                self._request,
                self._url(""),
                method="DELETE",
                connect_timeout=self.connect_timeout,
                read_timeout=10,
            )

        # -- listing -----------------------------------------------------------

        @property
        def supports_listing(self) -> bool:
            return True

        def _scrape_dir(self, prefix: str):
            """Return (child_names, subdir_names) for an autoindex directory.

            Scrapes ``<a href>`` links from the gateway HTML autoindex for the
            directory at ``prefix`` (identical link extraction to the legacy
            ZarrClient.__iter__). Entries ending in ``/`` are subdirectories.
            """
            url = self._url(prefix)
            if not url.endswith("/"):
                url += "/"
            resp = self._request(
                url,
                connect_timeout=self.connect_timeout,
                read_timeout=self.read_timeout,
            )
            if resp.status_code >= 300 or not resp.text:
                return [], []
            files, dirs = [], []
            for match in _HREF_RE.findall(resp.text):
                href = match[2]
                # Skip parent/self navigation links.
                if href in ("..", "../", ".", "./") or href.startswith("?"):
                    continue
                # Normalise to a bare child name.
                name = href.rstrip("/")
                name = name.rsplit("/", 1)[-1]
                if not name:
                    continue
                if href.endswith("/"):
                    dirs.append(name)
                else:
                    files.append(name)
            return files, dirs

        async def list_dir(self, prefix: str):
            prefix = prefix.rstrip("/")
            files, dirs = await asyncio.to_thread(self._scrape_dir, prefix)
            for name in files:
                yield name
            for name in dirs:
                yield name

        async def list_prefix(self, prefix: str):
            """Recursively walk the autoindex under ``prefix``.

            Yields full keys (relative to the store root). Used by
            ``consolidate_metadata`` when writing v2-format stores.
            """
            prefix = prefix.rstrip("/")
            stack = [prefix]
            while stack:
                current = stack.pop()
                files, dirs = await asyncio.to_thread(
                    self._scrape_dir, current
                )
                base = f"{current}/" if current else ""
                for name in files:
                    yield f"{base}{name}"
                for name in dirs:
                    stack.append(f"{base}{name}")

        def list(self):
            return self.list_prefix("")

    return DatameshV2WireStore


# Cache the built class so repeated construction is cheap.
_STORE_CLASS = None


def make_v2wire_store(
    connection,
    datasource,
    session,
    *,
    parameters=None,
    method="post",
    api="zarr",
    nocache=False,
    storage_backend=None,
    read_only=False,
    retries=10,
    verify=True,
):
    """Construct a :class:`DatameshV2WireStore` from a live connection/session.

    Mirrors the legacy ``ZarrClient`` constructor surface so call sites port
    with minimal change.
    """
    global _STORE_CLASS
    if _STORE_CLASS is None:
        _STORE_CLASS = _make_store_class()

    if api == "zarr":
        proxy_base = connection._gateway + "/zarr"
    elif api == "query":
        proxy_base = connection._gateway + "/zarr/query"
    else:
        raise DatameshConnectError(f"Unknown api: {api}")

    headers = build_v2wire_headers(
        connection,
        session,
        parameters=parameters,
        nocache=nocache,
        storage_backend=storage_backend,
    )
    http_session = HTTPSession(headers=headers)
    return _STORE_CLASS(
        proxy_base,
        datasource,
        headers,
        http_session,
        api=api,
        method=method,
        read_only=read_only,
        retries=retries,
        verify=verify,
    )
