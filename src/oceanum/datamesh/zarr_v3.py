"""Zarr **v3** per-datasource Store for the Datamesh (izarr / Icechunk wire).

This is the v3 counterpart of :mod:`oceanum.datamesh.zarr_v2wire`. It exposes a
single public surface — :class:`DatameshV3Store` plus the
:func:`make_v3_store` factory — mirroring ``make_v2wire_store``. Like the v2
wire store it is a **per-datasource** ``zarr.abc.store.Store``: one store == one
datasource == one session.

Security model (owner's design review — non-negotiable):

* the wire surface is **data-plane only** — ``GET/HEAD/PUT/DELETE`` on
  ``{proxy}/secure/zarr/{datasource}/{key}`` plus the two control POSTs
  ``{proxy}/secure/zarr/_finalise/{datasource}`` and ``.../_abort/{datasource}``;
* there is **no** ``/info`` / ``register_read`` / ``register_write`` on the wire
  and no ``driver`` / ``driver_args`` ever leaves the client — the proxy
  auto-registers reads on the first ``GET``/``HEAD`` and writes on the first
  ``PUT``/``DELETE``, resolving the driver args server-side from the metadata
  record;
* selectors / variables / downsampling are **not** part of the public surface.

Consolidated metadata is synthesized by the proxy into the root ``zarr.json``
(inline ``consolidated_metadata``), so the client just opens with consolidated
semantics working — no client-side consolidation.

Transport is native async ``httpx`` (:class:`httpx.AsyncClient`, created lazily
inside the running event loop), with retries on connect errors and 5xx honouring
the retry constants in :mod:`oceanum.datamesh.utils`. Range requests are used for
partial reads, with a fetch-and-slice fallback when the server ignores ``Range``.

This module REQUIRES zarr-python >= 3 to be *used*; importing it is safe under a
``zarr<3`` pin because the Store subclass is built lazily (mirroring
``zarr_v2wire._make_store_class``).
"""

import asyncio
import json
import os
from time import sleep
from typing import Dict, Optional

import httpx

from .exceptions import DatameshConnectError, DatameshWriteError
from .session import Session
from .utils import (
    DATAMESH_CONNECT_TIMEOUT,
    DATAMESH_CHUNK_READ_TIMEOUT,
    DATAMESH_CHUNK_WRITE_TIMEOUT,
)

__all__ = ["make_v3_store", "DatameshV3Store"]

# Default retry budget (mirrors the v2 wire store's ``retries=10``); the
# per-attempt exponential backoff matches ``utils.retried_request``.
_DEFAULT_RETRIES = 10


def _make_store_class():
    """Build the Store subclass lazily so the module imports cleanly.

    (zarr-python 3 is required to *use* the class; importing the module does
    not require it, mirroring ``zarr_v2wire._make_store_class``.)
    """
    from zarr.abc.store import (
        ByteRequest,
        OffsetByteRequest,
        RangeByteRequest,
        Store,
        SuffixByteRequest,
    )
    from zarr.core.buffer import Buffer, BufferPrototype

    class DatameshV3Store(Store):
        """zarr v3 ``Store`` over the proxy ``/secure/zarr`` data plane.

        Owns exactly one datamesh session for its whole lifetime: the session
        header ``X-DATAMESH-SESSIONID`` rides on every request, and
        :meth:`close` releases it. The first ``GET``/``HEAD`` auto-registers a
        read server-side and the first ``PUT``/``DELETE`` auto-registers a
        write — the client never sends driver/driver_args.
        """

        def __init__(
            self,
            root_url: str,
            datasource: str,
            session,
            headers: Dict[str, str],
            *,
            read_only: bool = True,
            verify: bool = True,
            retries: int = _DEFAULT_RETRIES,
            connect_timeout=DATAMESH_CONNECT_TIMEOUT,
            read_timeout=DATAMESH_CHUNK_READ_TIMEOUT,
            write_timeout=DATAMESH_CHUNK_WRITE_TIMEOUT,
            owns_session: bool = True,
        ):
            super().__init__(read_only=read_only)
            self.datasource = datasource
            # ``root_url`` is ``{proxy}/secure/zarr``; data-plane keys hang off
            # ``{root}/{datasource}/{key}`` and the control POSTs off
            # ``{root}/_finalise|_abort/{datasource}``.
            self._root = root_url.rstrip("/")
            self._base = f"{self._root}/{datasource}"
            self._session = session
            self._headers = dict(headers)
            self.verify = verify
            self.retries = retries
            self.connect_timeout = connect_timeout
            self.read_timeout = read_timeout
            self.write_timeout = write_timeout
            self._owns_session = owns_session
            # httpx client is created lazily inside the running loop (and
            # recreated if the loop changes) — see ``_client_for_loop``.
            self._client = None
            self._client_loop = None

        def __eq__(self, other):
            return self is other

        __hash__ = object.__hash__

        def __repr__(self):
            return f"DatameshV3Store(datasource={self.datasource!r})"

        def with_read_only(self, read_only: bool = False):
            """Return a shallow clone with a different read_only flag.

            zarr-python opens stores read-only for read paths via this hook.
            The clone shares this store's session (one session per datasource)
            and does **not** own it, so a clone's ``close()`` never releases
            the parent's session.
            """
            return self.__class__(
                self._root,
                self.datasource,
                self._session,
                self._headers,
                read_only=read_only,
                verify=self.verify,
                retries=self.retries,
                connect_timeout=self.connect_timeout,
                read_timeout=self.read_timeout,
                write_timeout=self.write_timeout,
                owns_session=False,
            )

        # -- transport ---------------------------------------------------------

        def _client_for_loop(self):
            """Lazily create (or recreate) the httpx client for this loop."""
            loop = asyncio.get_running_loop()
            if (
                self._client is None
                or self._client.is_closed
                or self._client_loop is not loop
            ):
                self._client = httpx.AsyncClient(
                    headers=self._headers,
                    verify=self.verify,
                    follow_redirects=True,
                )
                self._client_loop = loop
            return self._client

        def _timeout(self, write: bool = False):
            read = self.write_timeout if write else self.read_timeout
            return httpx.Timeout(
                connect=self.connect_timeout,
                read=read,
                write=self.write_timeout,
                pool=self.connect_timeout,
            )

        async def _request(
            self,
            method: str,
            url: str,
            *,
            headers: Optional[Dict[str, str]] = None,
            content: Optional[bytes] = None,
            params: Optional[dict] = None,
            write: bool = False,
        ) -> httpx.Response:
            """Async request with retries on connect errors and 5xx.

            Retries honour ``utils.retried_request`` semantics: a bounded retry
            count with exponential backoff. Non-5xx responses (including 404)
            are returned to the caller to interpret.
            """
            client = self._client_for_loop()
            retried = 0
            while True:
                try:
                    resp = await client.request(
                        method,
                        url,
                        headers=headers,
                        content=content,
                        params=params,
                        timeout=self._timeout(write=write),
                    )
                except httpx.TransportError as e:
                    retried += 1
                    if retried >= self.retries:
                        raise DatameshConnectError(
                            f"Failed to connect to {url} after {self.retries} "
                            f"retries with error: {e}"
                        )
                    await asyncio.sleep(0.1 * 2**retried)
                    continue
                if resp.status_code >= 500:
                    retried += 1
                    if retried >= self.retries:
                        raise DatameshConnectError(
                            f"Server error {resp.status_code}: {resp.text}"
                        )
                    await asyncio.sleep(0.1 * 2**retried)
                    continue
                if resp.status_code == 401:
                    raise DatameshConnectError(f"Not Authorized {resp.text}")
                if resp.status_code == 410:
                    raise DatameshConnectError(
                        "Datasource no longer exists or was deleted within "
                        "your session"
                    )
                return resp

        @staticmethod
        def _range_header(byte_range: Optional[ByteRequest]) -> Dict[str, str]:
            if byte_range is None:
                return {}
            if isinstance(byte_range, RangeByteRequest):
                return {"Range": f"bytes={byte_range.start}-{byte_range.end - 1}"}
            if isinstance(byte_range, OffsetByteRequest):
                return {"Range": f"bytes={byte_range.offset}-"}
            if isinstance(byte_range, SuffixByteRequest):
                return {"Range": f"bytes=-{byte_range.suffix}"}
            raise TypeError(f"Unsupported byte range: {byte_range!r}")

        @staticmethod
        def _slice_bytes(
            content: bytes, byte_range: Optional[ByteRequest]
        ) -> bytes:
            """Fetch-and-slice fallback for a server that ignored ``Range``."""
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
            range_headers = self._range_header(byte_range)
            resp = await self._request(
                "GET",
                self._url(key),
                headers=range_headers or None,
            )
            if resp.status_code == 404:
                return None
            if resp.status_code >= 400:
                raise DatameshConnectError(
                    f"Failed to read {key}: {resp.status_code} - {resp.text}"
                )
            content = resp.content
            # 206 => the server honoured Range and already sliced; on a plain
            # 200 with a range requested, slice client-side (fallback).
            if byte_range is not None and resp.status_code != 206:
                content = self._slice_bytes(content, byte_range)
            return prototype.buffer.from_bytes(content)

        async def get_partial_values(self, prototype, key_ranges):
            return [
                await self.get(key, prototype, rng) for key, rng in key_ranges
            ]

        async def exists(self, key: str) -> bool:
            resp = await self._request("HEAD", self._url(key))
            return resp.status_code == 200

        def _url(self, key: str) -> str:
            return f"{self._base}/{key}"

        # -- writes ------------------------------------------------------------

        @property
        def supports_writes(self) -> bool:
            return not self.read_only

        @property
        def supports_partial_writes(self) -> bool:
            return False

        async def set(self, key: str, value: Buffer) -> None:
            self._check_writable()
            resp = await self._request(
                "PUT",
                self._url(key),
                content=value.to_bytes(),
                write=True,
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
            resp = await self._request("DELETE", self._url(key))
            if resp.status_code not in (200, 202, 204, 404):
                raise DatameshWriteError(
                    f"Failed to delete {key}: {resp.status_code} - {resp.text}"
                )

        # -- listing -----------------------------------------------------------

        @property
        def supports_listing(self) -> bool:
            return True

        async def _list_op(self, op: str, prefix: str):
            resp = await self._request(
                "GET", self._base, params={"op": op, "prefix": prefix}
            )
            if resp.status_code >= 400:
                return
            for key in resp.json().get("keys", []):
                yield key

        def list(self):
            return self._list_op("list", "")

        def list_prefix(self, prefix: str):
            return self._list_op("list", prefix)

        def list_dir(self, prefix: str):
            return self._list_op("list_dir", prefix)

        # -- write lifecycle (private control plane) ---------------------------

        def _control_post(self, url: str, message: Optional[str] = None):
            """Synchronous control-plane POST with retries.

            Used for ``_finalise``/``_abort``, which are invoked from the
            synchronous ``write_datasource`` path (no running event loop), so a
            one-off synchronous ``httpx`` request is the natural fit.
            """
            headers = dict(self._headers)
            content = None
            if message is not None:
                content = json.dumps({"message": message}).encode()
                headers["Content-Type"] = "application/json"
            retried = 0
            while True:
                try:
                    with httpx.Client(
                        verify=self.verify,
                        timeout=httpx.Timeout(
                            connect=self.connect_timeout,
                            read=self.write_timeout,
                            write=self.write_timeout,
                            pool=self.connect_timeout,
                        ),
                    ) as client:
                        resp = client.post(url, headers=headers, content=content)
                except httpx.TransportError as e:
                    retried += 1
                    if retried >= self.retries:
                        raise DatameshConnectError(
                            f"Failed to connect to {url} after {self.retries} "
                            f"retries with error: {e}"
                        )
                    sleep(0.1 * 2**retried)
                    continue
                if resp.status_code >= 500:
                    retried += 1
                    if retried >= self.retries:
                        raise DatameshConnectError(
                            f"Server error {resp.status_code}: {resp.text}"
                        )
                    sleep(0.1 * 2**retried)
                    continue
                if resp.status_code >= 400:
                    raise DatameshWriteError(
                        f"{url} -> {resp.status_code}: {resp.text}"
                    )
                return resp

        def _finalise(self, message: Optional[str] = None):
            """Commit the write session (izarr: one atomic Icechunk commit)."""
            return self._control_post(
                f"{self._root}/_finalise/{self.datasource}", message
            )

        def _abort(self):
            """Discard the in-flight write session."""
            return self._control_post(f"{self._root}/_abort/{self.datasource}")

        # -- lifecycle ---------------------------------------------------------

        def close(self, finalise_write: bool = False):
            client = self._client
            self._client = None
            loop = self._client_loop
            self._client_loop = None
            if client is not None and not client.is_closed:
                try:
                    if (
                        loop is not None
                        and not loop.is_closed()
                        and not loop.is_running()
                    ):
                        loop.run_until_complete(client.aclose())
                except Exception:
                    pass
            if self._owns_session and self._session is not None:
                self._session.close(finalise_write=finalise_write)
                self._session = None

    return DatameshV3Store


# Cache the built class so repeated construction is cheap.
_STORE_CLASS = None


def _resolve_proxy(connection) -> str:
    """The v3 proxy base — ``DATAMESH_ZARR_PROXY_ZARR3`` else the gateway.

    External clients hit the QE ``/zarr3`` endpoints (the gateway);
    ``DATAMESH_ZARR_PROXY_ZARR3`` opts into direct-to-proxy for internal use.
    """
    proxy = os.environ.get("DATAMESH_ZARR_PROXY_ZARR3") or connection._gateway
    return proxy.rstrip("/")


def make_v3_store(connection, datasource, *, read_only: bool = True,
                  allow_multiwrite: bool = False):
    """Construct a :class:`DatameshV3Store` for one datasource.

    Mirrors :func:`oceanum.datamesh.zarr_v2wire.make_v2wire_store`. Acquires a
    single session synchronously and hands it to the store, which sends the
    session header on every request and releases it in :meth:`close`. No
    registration call is made — the proxy auto-registers server-side on the
    first data-plane request.
    """
    global _STORE_CLASS
    if _STORE_CLASS is None:
        _STORE_CLASS = _make_store_class()

    root_url = f"{_resolve_proxy(connection)}/secure/zarr"
    # allow_multiwrite rides on the SESSION: the proxy's implicit write
    # registration reads it from the authenticated session record and puts
    # concurrent writers on one shared icechunk branch.
    session = Session.acquire(connection, allow_multiwrite=allow_multiwrite)
    headers = {
        **getattr(connection, "_auth_headers", {}),
        "X-DATAMESH-SESSIONID": session.id,
    }
    return _STORE_CLASS(
        root_url,
        datasource,
        session,
        headers,
        read_only=read_only,
        verify=getattr(connection, "_verify", True),
    )
