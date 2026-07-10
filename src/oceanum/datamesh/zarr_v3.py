"""Zarr **v3** client for the Datamesh — the successor to `zarr.ZarrClient`.

Talks to `datamesh-zarr-proxy-v3` using v3 conventions: per-node ``zarr.json``
documents, ``c/``-prefixed chunk keys, HTTP Range requests, and the explicit
write lifecycle (register → raw PUTs → finalise). Supports the ``izarr``
(Icechunk, versioned) and ``onzarr`` (plain bucket, non-versioned) backends.

This module REQUIRES zarr-python >= 3 and is therefore fully self-contained
with lazy imports: importing it is safe under the package's current
``zarr<3`` pin, but using it needs an environment with zarr >= 3 installed
(the rest of oceanum-python is not yet compatible with such an environment —
this module is the first step of that migration and can be loaded standalone
via importlib in the meantime).

Example
-------
    client = ZarrClientV3(gateway="https://gateway...", proxy="https://proxy-v3...")
    with client:
        client.write_dataset(ds, "my-datasource", driver="izarr",
                             driver_args={"repository": "s3://bucket/izarr/my-datasource"},
                             append_dims=["time"])
        roundtrip = client.open_dataset("my-datasource",
                                        driver="izarr",
                                        driver_args={"repository": "s3://bucket/izarr/my-datasource"})
"""

import asyncio
import os
from typing import Any, Dict, Iterable, List, Optional

import requests

__all__ = ["ZarrClientV3", "DatameshZarrV3Error"]


class DatameshZarrV3Error(Exception):
    pass


def _check_zarr3():
    import zarr

    major = int(str(zarr.__version__).split(".")[0])
    if major < 3:
        raise DatameshZarrV3Error(
            f"oceanum.datamesh.zarr_v3 requires zarr-python >= 3 "
            f"(found {zarr.__version__})"
        )


def _make_store_class():
    """Build the Store subclass lazily so the module imports under zarr 2."""
    import zarr
    from zarr.abc.store import (
        ByteRequest,
        OffsetByteRequest,
        RangeByteRequest,
        Store,
        SuffixByteRequest,
    )
    from zarr.core.buffer import Buffer, BufferPrototype

    class DatameshV3Store(Store):
        """zarr v3 Store over the proxy's /secure/zarr contract."""

        def __init__(
            self,
            base_url: str,
            datasource: str,
            session_id: str,
            *,
            read_only: bool = True,
            headers: Optional[Dict[str, str]] = None,
            http_session: Optional[requests.Session] = None,
        ):
            super().__init__(read_only=read_only)
            self._base = f"{base_url.rstrip('/')}/secure/zarr/{datasource}"
            self._headers = {"X-DATAMESH-SESSIONID": session_id, **(headers or {})}
            self._http = http_session or requests.Session()

        def __eq__(self, other):
            return self is other

        def _request(self, method: str, url: str, **kwargs):
            return self._http.request(
                method, url, headers={**self._headers, **kwargs.pop("headers", {})},
                timeout=300, **kwargs,
            )

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

        async def get(
            self,
            key: str,
            prototype: BufferPrototype,
            byte_range: Optional[ByteRequest] = None,
        ) -> Optional[Buffer]:
            response = await asyncio.to_thread(
                self._request, "GET", f"{self._base}/{key}",
                headers=self._range_header(byte_range),
            )
            if response.status_code == 404:
                return None
            response.raise_for_status()
            return prototype.buffer.from_bytes(response.content)

        async def get_partial_values(self, prototype, key_ranges):
            return [await self.get(key, prototype, rng) for key, rng in key_ranges]

        async def exists(self, key: str) -> bool:
            response = await asyncio.to_thread(
                self._request, "HEAD", f"{self._base}/{key}"
            )
            return response.status_code == 200

        @property
        def supports_writes(self) -> bool:
            return not self.read_only

        async def set(self, key: str, value: Buffer):
            response = await asyncio.to_thread(
                self._request, "PUT", f"{self._base}/{key}",
                data=value.to_bytes(),
            )
            response.raise_for_status()

        @property
        def supports_deletes(self) -> bool:
            return not self.read_only

        async def delete(self, key: str):
            response = await asyncio.to_thread(
                self._request, "DELETE", f"{self._base}/{key}"
            )
            if response.status_code not in (204, 404):
                response.raise_for_status()

        @property
        def supports_partial_writes(self) -> bool:
            return False

        async def set_partial_values(self, key_start_values):
            raise NotImplementedError

        @property
        def supports_listing(self) -> bool:
            return True

        async def _list_op(self, op: str, prefix: str):
            response = await asyncio.to_thread(
                self._request, "GET", self._base, params={"op": op, "prefix": prefix}
            )
            response.raise_for_status()
            for key in response.json()["keys"]:
                yield key

        def list(self):
            return self._list_op("list", "")

        def list_prefix(self, prefix: str):
            return self._list_op("list", prefix)

        def list_dir(self, prefix: str):
            return self._list_op("list_dir", prefix)

    return DatameshV3Store


class ZarrClientV3:
    """Session-scoped client for the v3 zarr proxy.

    Parameters
    ----------
    gateway : str
        Datamesh gateway (query-engine) URL — used to create/close sessions.
        Default: ``$DATAMESH_GATEWAY``.
    proxy : str
        v3 zarr proxy URL. Default: ``$DATAMESH_ZARR_PROXY_V3``.
    token : str
        Datamesh token. Default: ``$DATAMESH_TOKEN``.
    """

    def __init__(
        self,
        gateway: Optional[str] = None,
        proxy: Optional[str] = None,
        token: Optional[str] = None,
        session_duration: float = 3600,
    ):
        _check_zarr3()
        self.gateway = (gateway or os.environ.get("DATAMESH_GATEWAY", "")).rstrip("/")
        self.proxy = (proxy or os.environ.get("DATAMESH_ZARR_PROXY_V3", "")).rstrip("/")
        self.token = token or os.environ.get("DATAMESH_TOKEN")
        if not self.gateway or not self.proxy:
            raise DatameshZarrV3Error(
                "gateway and proxy URLs are required (or set DATAMESH_GATEWAY "
                "and DATAMESH_ZARR_PROXY_V3)"
            )
        self._http = requests.Session()
        self._http.headers["X-DATAMESH-TOKEN"] = self.token or ""
        self._store_class = _make_store_class()

        response = self._http.get(
            f"{self.gateway}/session/", params={"duration": session_duration},
            timeout=30,
        )
        if response.status_code != 200:
            raise DatameshZarrV3Error(
                f"Failed to create session: {response.status_code} {response.text}"
            )
        self.session = response.json()
        self.session_id = self.session["id"]

    # -- lifecycle -------------------------------------------------------------

    def close(self):
        try:
            self._http.delete(
                f"{self.gateway}/session/{self.session_id}",
                headers={"X-DATAMESH-SESSIONID": self.session_id}, timeout=30,
            )
        except requests.RequestException:
            pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def _proxy_request(self, method: str, path: str, **kwargs):
        response = self._http.request(
            method, f"{self.proxy}{path}",
            headers={"X-DATAMESH-SESSIONID": self.session_id,
                     **kwargs.pop("headers", {})},
            timeout=300, **kwargs,
        )
        if response.status_code >= 400:
            raise DatameshZarrV3Error(
                f"{method} {path} → {response.status_code}: {response.text}"
            )
        return response

    # -- reads -----------------------------------------------------------------

    def register_read(
        self,
        datasource: str,
        driver: str = "izarr",
        driver_args: Optional[dict] = None,
    ) -> dict:
        """Establish (or fetch) the read registration for this session.
        driver_args may be omitted when the datasource record in the metadata
        server carries them (the proxy resolves them itself)."""
        return self._proxy_request(
            "POST", f"/zarr/info/{datasource}",
            json={"driver": driver, "driver_args": driver_args or {}},
        ).json()

    def store(
        self,
        datasource: str,
        *,
        read_only: bool = True,
        selector: Optional[Dict[str, tuple]] = None,
        downsample: Optional[Dict[str, int]] = None,
        variables: Optional[Iterable[str]] = None,
    ):
        """A zarr v3 Store for the datasource (register first). Optional
        server-side processing via the X-DATAMESH-* headers."""
        headers = {}
        if selector:
            headers["X-DATAMESH-SELECTOR"] = ",".join(
                f"{dim}={start}:{stop}" for dim, (start, stop) in selector.items()
            )
        if downsample:
            headers["X-DATAMESH-DOWNSAMPLE"] = ",".join(
                f"{dim}={factor}" for dim, factor in downsample.items()
            )
        if variables:
            headers["X-DATAMESH-VARIABLES"] = ",".join(variables)
        return self._store_class(
            self.proxy, datasource, self.session_id,
            read_only=read_only, headers=headers,
        )

    def open_dataset(
        self,
        datasource: str,
        driver: str = "izarr",
        driver_args: Optional[dict] = None,
        **store_kwargs: Any,
    ):
        """Register + open as an xarray Dataset (v3 wire)."""
        import xarray as xr

        self.register_read(datasource, driver, driver_args)
        return xr.open_zarr(
            self.store(datasource, **store_kwargs),
            consolidated=False, zarr_format=3,
        )

    # -- writes ----------------------------------------------------------------

    def register_write(
        self,
        datasource: str,
        driver: str,
        driver_args: dict,
        *,
        append_dims: Optional[List[str]] = None,
        codec_profile: Optional[str] = None,
        allow_multiwrite: bool = False,
    ) -> dict:
        return self._proxy_request(
            "POST", f"/zarr/write/{datasource}",
            json={"driver": driver, "driver_args": driver_args,
                  "append_dims": append_dims, "codec_profile": codec_profile,
                  "allow_multiwrite": allow_multiwrite},
        ).json()

    def finalise_write(self, datasource: str) -> dict:
        return self._proxy_request(
            "POST", f"/secure/zarr/_finalise/{datasource}"
        ).json()

    def abort_write(self, datasource: str) -> dict:
        return self._proxy_request(
            "POST", f"/secure/zarr/_abort/{datasource}"
        ).json()

    def write_dataset(
        self,
        dataset,
        datasource: str,
        driver: str,
        driver_args: dict,
        *,
        mode: str = "w",
        append_dims: Optional[List[str]] = None,
        codec_profile: Optional[str] = None,
        encoding: Optional[dict] = None,
        finalise: bool = True,
    ) -> dict:
        """Register → stream the dataset through the proxy → finalise.

        izarr: one atomic Icechunk commit at finalise (returns snapshot_id).
        onzarr: plain PUTs, durable immediately; finalise releases the
        single-writer registration.
        """
        registration = self.register_write(
            datasource, driver, driver_args,
            append_dims=append_dims, codec_profile=codec_profile,
        )
        try:
            dataset.to_zarr(
                self.store(datasource, read_only=False),
                mode=mode, consolidated=False, zarr_format=3,
                encoding=encoding,
            )
        except Exception:
            self.abort_write(datasource)
            raise
        if not finalise:
            return registration
        return self.finalise_write(datasource)
