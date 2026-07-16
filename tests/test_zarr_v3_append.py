"""Parity tests for the v3-wire overlap-replace append algorithm.

``Connector._zarr_v3_overlap_append`` ports the v2 overlap-replace append from
``oceanum.datamesh.zarr.zarr_write`` onto the v3 (izarr) store. These tests
exercise it directly against the in-process ``FakeV3Gateway`` (no live
services): the existing dataset is seeded with consolidated metadata (proxy
behaviour), the algorithm reads it back through a read-only clone of the write
store, region-writes the overlap and appends the remainder — all on the one
session/store the real caller finalises once.
"""
import numpy as np
import pytest
import xarray as xr
from unittest.mock import patch

import oceanum.datamesh.connection as conn_mod
from oceanum.datamesh.connection import Connector
from oceanum.datamesh.exceptions import DatameshWriteError
from oceanum.datamesh.zarr_v3 import make_v3_store
from tests._fakewire import FakeConnection, FakeSession, start_v3_gateway


@pytest.fixture(autouse=True)
def _no_proxy_env(monkeypatch):
    monkeypatch.delenv("DATAMESH_ZARR_PROXY_ZARR3", raising=False)


@pytest.fixture
def v3gateway():
    gw, server, url = start_v3_gateway()
    try:
        yield gw, url
    finally:
        server.shutdown()


def _write_store(conn, ds_id):
    with patch("oceanum.datamesh.zarr_v3.Session.acquire", return_value=FakeSession()):
        return make_v3_store(conn, ds_id, read_only=False)


def _read_store(conn, ds_id):
    with patch("oceanum.datamesh.zarr_v3.Session.acquire", return_value=FakeSession()):
        return make_v3_store(conn, ds_id, read_only=True)


def _existing(times):
    n = len(times)
    return xr.Dataset(
        {"temp": (("time", "x"), np.arange(n * 4, dtype="f8").reshape(n, 4))},
        coords={"time": np.array(times), "x": np.arange(4)},
    )


def _incoming(times, base=100.0):
    n = len(times)
    return xr.Dataset(
        {"temp": (("time", "x"), base + np.arange(n * 4, dtype="f8").reshape(n, 4))},
        coords={"time": np.array(times), "x": np.arange(4)},
    )


def _run_append(conn, ds_id, data, append="time"):
    connector = object.__new__(Connector)
    store = _write_store(conn, ds_id)
    try:
        connector._zarr_v3_overlap_append(store, data, append)
    finally:
        store.close()


def _read_back(conn, ds_id):
    store = _read_store(conn, ds_id)
    return xr.open_zarr(store, zarr_format=3, consolidated=False)


# --------------------------------------------------------------------------- #
# happy paths
# --------------------------------------------------------------------------- #

def test_pure_extend_append(v3gateway):
    gw, url = v3gateway
    conn = FakeConnection(url)
    gw.seed_consolidated("ds", _existing([0, 1, 2]))

    new = _incoming([3, 4, 5])
    _run_append(conn, "ds", new)

    back = _read_back(conn, "ds")
    assert list(back["time"].values) == [0, 1, 2, 3, 4, 5]
    # Existing rows preserved, new rows appended.
    np.testing.assert_array_equal(back["temp"].values[:3], _existing([0, 1, 2])["temp"].values)
    np.testing.assert_array_equal(back["temp"].values[3:], new["temp"].values)


def test_overlap_tail_replace_and_extend(v3gateway):
    gw, url = v3gateway
    conn = FakeConnection(url)
    gw.seed_consolidated("ds", _existing([0, 1, 2, 3, 4]))

    # overlap on 3,4 (replaced) and extend with 5,6
    new = _incoming([3, 4, 5, 6])
    _run_append(conn, "ds", new)

    back = _read_back(conn, "ds")
    assert list(back["time"].values) == [0, 1, 2, 3, 4, 5, 6]
    # rows 0..2 unchanged
    np.testing.assert_array_equal(back["temp"].values[:3], _existing([0, 1, 2, 3, 4])["temp"].values[:3])
    # rows 3..6 come from the incoming data
    np.testing.assert_array_equal(back["temp"].values[3:], new["temp"].values)


def test_full_overlap_replace_only(v3gateway):
    gw, url = v3gateway
    conn = FakeConnection(url)
    gw.seed_consolidated("ds", _existing([0, 1, 2, 3, 4]))

    # incoming exactly covers existing tail 2,3,4 -> replace only, no extend
    new = _incoming([2, 3, 4])
    _run_append(conn, "ds", new)

    back = _read_back(conn, "ds")
    assert list(back["time"].values) == [0, 1, 2, 3, 4]
    np.testing.assert_array_equal(back["temp"].values[:2], _existing([0, 1, 2, 3, 4])["temp"].values[:2])
    np.testing.assert_array_equal(back["temp"].values[2:], new["temp"].values)


# --------------------------------------------------------------------------- #
# error cases (all DatameshWriteError, parity with v2)
# --------------------------------------------------------------------------- #

def test_append_coord_absent(v3gateway):
    gw, url = v3gateway
    conn = FakeConnection(url)
    gw.seed_consolidated("ds", _existing([0, 1, 2]))
    new = _incoming([3, 4])
    with pytest.raises(DatameshWriteError, match="not in existing zarr"):
        _run_append(conn, "ds", new, append="nope")


def test_incoming_non_monotonic(v3gateway):
    gw, url = v3gateway
    conn = FakeConnection(url)
    gw.seed_consolidated("ds", _existing([0, 1, 2]))
    new = _incoming([5, 3, 4])  # not monotonic non-decreasing
    with pytest.raises(DatameshWriteError, match="monotonic non-decreasing"):
        _run_append(conn, "ds", new)


def test_non_contiguous_existing_overlap(v3gateway):
    gw, url = v3gateway
    conn = FakeConnection(url)
    # existing time non-monotonic so overlap indices are non-contiguous:
    # [1, 5, 2] with incoming window [1, 3] hits existing indices 0 and 2.
    gw.seed_consolidated("ds", _existing([1, 5, 2]))
    new = _incoming([1, 2, 3])
    with pytest.raises(DatameshWriteError, match="non-contiguous"):
        _run_append(conn, "ds", new)


def test_overlap_longer_than_incoming(v3gateway):
    gw, url = v3gateway
    conn = FakeConnection(url)
    gw.seed_consolidated("ds", _existing([0, 1, 2, 3, 4]))
    # incoming [1,4] spans existing indices 1,2,3,4 (overlap 4) but only 2 rows
    new = _incoming([1, 4])
    with pytest.raises(DatameshWriteError, match="region that would be smaller"):
        _run_append(conn, "ds", new)


def test_mismatched_overlap_timestamps(v3gateway):
    gw, url = v3gateway
    conn = FakeConnection(url)
    gw.seed_consolidated("ds", _existing([0, 2, 4, 6]))
    # incoming [2,3,4] : existing overlap is [2,4], incoming section [2,3] -> mismatch
    new = _incoming([2, 3, 4])
    with pytest.raises(DatameshWriteError, match="overlap timestamps do not match"):
        _run_append(conn, "ds", new)
