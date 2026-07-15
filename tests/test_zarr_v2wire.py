"""Self-contained unit tests for the v2-wire Store adapter under zarr-python 3.

Everything runs against an in-process threaded fake gateway (``_fakewire``);
no live datamesh services are required.
"""
import asyncio

import numpy as np
import pytest
import xarray as xr

from oceanum.datamesh.exceptions import DatameshConnectError
from oceanum.datamesh.zarr_v2wire import make_v2wire_store
from tests._fakewire import FakeConnection, FakeSession, start_gateway


@pytest.fixture
def gateway():
    gw, server, url = start_gateway()
    try:
        yield gw, url
    finally:
        server.shutdown()


def _sample_dataset():
    ds = xr.Dataset(
        {
            "temp": (("time", "x"), np.arange(20, dtype="f4").reshape(5, 4)),
            "label": (("x",), np.array(["a", "bb", "ccc", "d"], dtype=object)),
        },
        coords={"time": np.arange(5), "x": np.arange(4)},
    )
    ds["temp"].attrs["units"] = "K"
    ds.attrs["title"] = "sample"
    return ds


def test_v2wire_write_read_roundtrip(gateway):
    gw, url = gateway
    conn, sess = FakeConnection(url), FakeSession()
    ds = _sample_dataset()

    wstore = make_v2wire_store(conn, "myds", sess, api="zarr", nocache=True)
    ds.to_zarr(wstore, mode="w", zarr_format=2, consolidated=True)

    # A v2-format consolidated store was produced on the server.
    keys = gw._store("zarr", "myds")
    assert ".zmetadata" in keys
    assert "temp/0.0" in keys

    rstore = make_v2wire_store(conn, "myds", sess, api="zarr", read_only=True)
    back = xr.open_zarr(
        rstore, zarr_format=2, consolidated=True,
        decode_coords="all", mask_and_scale=True,
    )
    xr.testing.assert_identical(ds, back)


def test_v2wire_overwrite_clear(gateway):
    gw, url = gateway
    conn, sess = FakeConnection(url), FakeSession()
    ds = _sample_dataset()

    wstore = make_v2wire_store(conn, "myds", sess, api="zarr", nocache=True)
    ds.to_zarr(wstore, mode="w", zarr_format=2, consolidated=True)
    assert gw._store("zarr", "myds")

    # clear() must issue the single DELETE-on-empty-key the gateway understands.
    asyncio.run(wstore.clear())
    assert gw._store("zarr", "myds") == {}


def test_v2wire_listing(gateway):
    gw, url = gateway
    conn, sess = FakeConnection(url), FakeSession()
    _sample_dataset().to_zarr(
        make_v2wire_store(conn, "myds", sess, api="zarr", nocache=True),
        mode="w", zarr_format=2, consolidated=True,
    )
    store = make_v2wire_store(conn, "myds", sess, api="zarr", read_only=True)

    async def _list_prefix():
        return [k async for k in store.list_prefix("")]

    async def _list_dir():
        return [k async for k in store.list_dir("")]

    keys = asyncio.run(_list_prefix())
    assert ".zmetadata" in keys
    assert "temp/0.0" in keys
    assert "temp/.zarray" in keys

    top = asyncio.run(_list_dir())
    assert ".zmetadata" in top
    assert "temp" in top  # immediate child directory name only
    assert "temp/0.0" not in top


def test_v2wire_query_api_is_read_only(gateway):
    gw, url = gateway
    conn, sess = FakeConnection(url), FakeSession()
    store = make_v2wire_store(conn, "qhash", sess, api="query")

    assert store.read_only is True
    assert store.supports_writes is False

    from zarr.core.buffer import default_buffer_prototype

    buf = default_buffer_prototype().buffer.from_bytes(b"x")
    with pytest.raises(DatameshConnectError):
        asyncio.run(store.set("k", buf))
    with pytest.raises(DatameshConnectError):
        asyncio.run(store.delete("k"))


def test_v2wire_headers(gateway):
    _gw, url = gateway
    conn, sess = FakeConnection(url), FakeSession()
    store = make_v2wire_store(
        conn, "myds", sess, api="zarr", nocache=True,
        parameters={"a": 1}, storage_backend="lakefs",
    )
    assert store._headers["Authorization"] == "Token test"
    assert store._headers["X-DATAMESH-SESSIONID"] == sess.id
    assert store._headers["cache-control"] == "no-transform,no-cache"
    assert store._headers["X-PARAMETERS"] == '{"a": 1}'
    assert store._headers["X-DATAMESH-STORAGE-BACKEND"] == "lakefs"


def test_v2wire_missing_key_returns_none(gateway):
    _gw, url = gateway
    conn, sess = FakeConnection(url), FakeSession()
    store = make_v2wire_store(conn, "myds", sess, api="zarr", read_only=True)

    from zarr.core.buffer import default_buffer_prototype

    got = asyncio.run(store.get("nope", default_buffer_prototype()))
    assert got is None
    assert asyncio.run(store.exists("nope")) is False
