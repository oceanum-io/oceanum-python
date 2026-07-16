"""Self-contained unit tests for the v3-wire Store adapter under zarr-python 3.

Everything runs against an in-process threaded fake proxy (``_fakewire``'s
``FakeV3Gateway``); no live datamesh services are required. The fake serves the
data-plane-only surface the security review pins: ``GET/HEAD/PUT/DELETE`` on
``/secure/zarr/{ds}/{key}`` plus the ``_finalise``/``_abort`` control POSTs —
there is no ``/info`` or register endpoint anywhere.
"""
import asyncio

import numpy as np
import pytest
import xarray as xr
from unittest.mock import patch

from oceanum.datamesh.zarr_v3 import make_v3_store
from tests._fakewire import FakeConnection, FakeSession, start_v3_gateway


@pytest.fixture(autouse=True)
def _no_proxy_env(monkeypatch):
    # Force make_v3_store to fall back to connection._gateway (the fake).
    monkeypatch.delenv("DATAMESH_ZARR_PROXY_ZARR3", raising=False)


@pytest.fixture
def v3gateway():
    gw, server, url = start_v3_gateway()
    try:
        yield gw, url
    finally:
        server.shutdown()


@pytest.fixture
def v3gateway_norange():
    gw, server, url = start_v3_gateway(ignore_range=True)
    try:
        yield gw, url
    finally:
        server.shutdown()


def _sample_dataset():
    ds = xr.Dataset(
        {
            "temp": (("time", "x"), np.arange(20, dtype="f4").reshape(5, 4)),
        },
        coords={"time": np.arange(5), "x": np.arange(4)},
    )
    ds["temp"].attrs["units"] = "K"
    ds.attrs["title"] = "sample"
    return ds


def _store(conn, ds_id, *, read_only=True, session=None):
    sess = session or FakeSession()
    with patch("oceanum.datamesh.zarr_v3.Session.acquire", return_value=sess):
        return make_v3_store(conn, ds_id, read_only=read_only)


# --------------------------------------------------------------------------- #
# write -> finalise -> read roundtrip
# --------------------------------------------------------------------------- #

def test_v3_write_finalise_read_roundtrip(v3gateway):
    gw, url = v3gateway
    conn = FakeConnection(url)
    ds = _sample_dataset()

    wstore = _store(conn, "myds", read_only=False)
    ds.to_zarr(wstore, mode="w", zarr_format=3, consolidated=False)

    # v3-format keys landed on the fake proxy.
    keys = gw._store("myds")
    assert "zarr.json" in keys
    assert "temp/c/0/0" in keys

    # _finalise is a private control POST to /secure/zarr/_finalise/{ds}.
    wstore._finalise()
    assert ("_finalise", "myds") in gw.control
    wstore.close()

    rstore = _store(conn, "myds", read_only=True)
    back = xr.open_zarr(rstore, zarr_format=3, consolidated=False)
    xr.testing.assert_identical(ds, back)
    rstore.close()


def test_v3_abort_posts_control(v3gateway):
    gw, url = v3gateway
    conn = FakeConnection(url)
    store = _store(conn, "myds", read_only=False)
    store._abort()
    assert ("_abort", "myds") in gw.control
    store.close()


# --------------------------------------------------------------------------- #
# consolidated read: root zarr.json fetched once, no per-array zarr.json
# --------------------------------------------------------------------------- #

def test_v3_consolidated_open_reads_root_once(v3gateway):
    gw, url = v3gateway
    conn = FakeConnection(url)
    gw.seed_consolidated("myds", _sample_dataset())

    store = _store(conn, "myds", read_only=True)
    gw.requests.clear()
    back = xr.open_zarr(store, zarr_format=3, consolidated=True)
    # Realise metadata access.
    assert set(back.data_vars) == {"temp"}

    gets = [k for (m, k) in gw.requests if m == "GET"]
    # Root document fetched exactly once...
    assert gets.count("zarr.json") == 1
    # ...and no per-array zarr.json is ever fetched (consolidated metadata is
    # served inline in the root doc by the proxy).
    assert not any(k.endswith("/zarr.json") for k in gets)
    store.close()


# --------------------------------------------------------------------------- #
# Range handling: honoured (206) and fetch-and-slice fallback (server ignores)
# --------------------------------------------------------------------------- #

def _range_req():
    from zarr.abc.store import RangeByteRequest

    return RangeByteRequest(2, 5)


def test_v3_range_honoured(v3gateway):
    gw, url = v3gateway
    conn = FakeConnection(url)
    gw._store("blobds")["blob"] = b"0123456789"
    store = _store(conn, "blobds", read_only=True)

    from zarr.core.buffer import default_buffer_prototype

    got = asyncio.run(
        store.get("blob", default_buffer_prototype(), _range_req())
    )
    assert got.to_bytes() == b"234"
    store.close()


def test_v3_range_fallback_when_ignored(v3gateway_norange):
    gw, url = v3gateway_norange
    conn = FakeConnection(url)
    gw._store("blobds")["blob"] = b"0123456789"
    store = _store(conn, "blobds", read_only=True)

    from zarr.core.buffer import default_buffer_prototype

    # Server returns the full object (200); the store slices client-side.
    got = asyncio.run(
        store.get("blob", default_buffer_prototype(), _range_req())
    )
    assert got.to_bytes() == b"234"
    store.close()


# --------------------------------------------------------------------------- #
# misc store semantics
# --------------------------------------------------------------------------- #

def test_v3_missing_key_returns_none(v3gateway):
    _gw, url = v3gateway
    conn = FakeConnection(url)
    store = _store(conn, "myds", read_only=True)

    from zarr.core.buffer import default_buffer_prototype

    assert asyncio.run(store.get("nope", default_buffer_prototype())) is None
    assert asyncio.run(store.exists("nope")) is False
    store.close()


def test_v3_sends_session_and_auth_headers(v3gateway):
    gw, url = v3gateway
    conn = FakeConnection(url)
    store = _store(conn, "myds", read_only=True)

    from zarr.core.buffer import default_buffer_prototype

    asyncio.run(store.get("whatever", default_buffer_prototype()))
    assert gw.last_headers.get("X-DATAMESH-SESSIONID") == FakeSession.id
    assert gw.last_headers.get("Authorization") == "Token test"
    store.close()


def test_v3_close_releases_session(v3gateway):
    _gw, url = v3gateway
    conn = FakeConnection(url)
    sess = FakeSession()
    store = _store(conn, "myds", read_only=True, session=sess)

    assert sess.closed is False
    store.close(finalise_write=True)
    assert sess.closed is True
    assert sess.finalised is True
