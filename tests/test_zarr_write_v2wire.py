"""End-to-end ``zarr_write`` tests over the v2 wire (fake gateway, zarr 3)."""
from unittest.mock import patch

import numpy as np
import pytest
import xarray as xr

from oceanum.datamesh.zarr import zarr_write
from oceanum.datamesh.zarr_v2wire import make_v2wire_store
from tests._fakewire import FakeConnection, FakeSession, start_gateway


@pytest.fixture
def gateway():
    gw, server, url = start_gateway()
    try:
        yield gw, url
    finally:
        server.shutdown()


def _mk(times, vals):
    return xr.Dataset(
        {"v": (("time",), np.asarray(vals, dtype="f8"))},
        coords={"time": np.asarray(times)},
    )


def _read(conn, ds_id):
    store = make_v2wire_store(conn, ds_id, FakeSession(), api="zarr", read_only=True)
    return xr.open_zarr(store, zarr_format=2, consolidated=True)


class _ExistingDS:
    _exists = True

    class dataschema:
        coords = {"time": None}


def test_zarr_write_overwrite_creates_store(gateway):
    gw, url = gateway
    conn = FakeConnection(url)
    conn.get_datasource = lambda _id: _ExistingDS()

    base = _mk([1, 2, 3, 4, 5], [10, 20, 30, 40, 50])
    with patch("oceanum.datamesh.zarr.Session.acquire", return_value=FakeSession()):
        zarr_write(conn, "myds", base, overwrite=True)

    back = _read(conn, "myds")
    assert back["v"].values.tolist() == [10, 20, 30, 40, 50]


def test_zarr_write_append_overlap_replace(gateway):
    gw, url = gateway
    conn = FakeConnection(url)
    conn.get_datasource = lambda _id: _ExistingDS()

    # Seed the store.
    with patch("oceanum.datamesh.zarr.Session.acquire", return_value=FakeSession()):
        zarr_write(conn, "myds", _mk([1, 2, 3, 4, 5], [10, 20, 30, 40, 50]),
                   overwrite=True)

    existing = _read(conn, "myds")
    # New [4,5,6,7]: overlap 4,5 replaced, 6,7 appended.
    incoming = _mk([4, 5, 6, 7], [400, 500, 600, 700])
    with patch("oceanum.datamesh.zarr.Session.acquire", return_value=FakeSession()):
        with patch("oceanum.datamesh.zarr.xarray.open_zarr", return_value=existing):
            zarr_write(conn, "myds", incoming, append="time")

    final = _read(conn, "myds")
    assert final["time"].values.tolist() == [1, 2, 3, 4, 5, 6, 7]
    assert final["v"].values.tolist() == [10, 20, 30, 400, 500, 600, 700]


def test_zarr_write_append_pure_extend(gateway):
    gw, url = gateway
    conn = FakeConnection(url)
    conn.get_datasource = lambda _id: _ExistingDS()

    with patch("oceanum.datamesh.zarr.Session.acquire", return_value=FakeSession()):
        zarr_write(conn, "myds", _mk([1, 2, 3], [1, 2, 3]), overwrite=True)

    existing = _read(conn, "myds")
    incoming = _mk([4, 5], [4, 5])  # no overlap -> pure append
    with patch("oceanum.datamesh.zarr.Session.acquire", return_value=FakeSession()):
        with patch("oceanum.datamesh.zarr.xarray.open_zarr", return_value=existing):
            zarr_write(conn, "myds", incoming, append="time")

    final = _read(conn, "myds")
    assert final["time"].values.tolist() == [1, 2, 3, 4, 5]
    assert final["v"].values.tolist() == [1, 2, 3, 4, 5]
