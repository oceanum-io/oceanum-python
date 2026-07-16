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
    # staged_writes=True gives the fake the proxy's real write-session
    # read-visibility semantics (chunk PUTs invisible to reads until finalise,
    # metadata eager) so a write ordering that reads back a chunk it wrote this
    # session is caught offline — see FakeV3Gateway.
    gw, server, url = start_v3_gateway(staged_writes=True)
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


def _existing_bigchunk(times, chunk):
    """Existing dataset whose ``temp`` chunk along ``time`` is ``chunk`` long.

    A chunk wider than the array lets a later overlap AND its extension land in
    the SAME store chunk — the layout that trips the write-ordering bug live.
    """
    ds = _existing(times)
    ds["temp"].encoding["chunks"] = (chunk, 4)
    return ds


def _run_append(conn, ds_id, data, append="time"):
    connector = object.__new__(Connector)
    store = _write_store(conn, ds_id)
    try:
        connector._zarr_v3_overlap_append(store, data, append)
        # Finalise so the staged chunk PUTs commit to the branch, exactly as
        # ``_zarr_write_v3`` does on success — reads only see them afterwards.
        store._finalise()
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


def test_inner_overlap_replace_only(v3gateway):
    gw, url = v3gateway
    conn = FakeConnection(url)
    gw.seed_consolidated("ds", _existing([0, 1, 2, 3, 4]))

    # incoming replaces an INNER contiguous section 1,2,3 (matching timestamps),
    # no extension; tail 4 must survive.
    new = _incoming([1, 2, 3])
    _run_append(conn, "ds", new)

    back = _read_back(conn, "ds")
    assert list(back["time"].values) == [0, 1, 2, 3, 4]
    existing = _existing([0, 1, 2, 3, 4])["temp"].values
    np.testing.assert_array_equal(back["temp"].values[0], existing[0])
    np.testing.assert_array_equal(back["temp"].values[1:4], new["temp"].values)
    np.testing.assert_array_equal(back["temp"].values[4], existing[4])


def test_overlap_and_extension_in_same_store_chunk(v3gateway):
    """Regression for the live proxy failure (no read-your-writes for chunks).

    Existing length 24 with a store chunk of 24 wide enough that the array can
    grow within one chunk; append 12 overlapping 6, so the 6 overlapped indices
    (18..23) AND the 6-row extension (24..29) both live in the SAME ``temp``
    chunk. Under the proxy's staged write-session semantics, an ordering that
    region-writes the overlap and THEN read-modify-writes the shared chunk for
    the extension reads the stale branch copy and clobbers the overlap back to
    its old values. The fixed ordering (extend first, then region-write the full
    span) keeps every overlap index at the new values.
    """
    gw, url = v3gateway
    conn = FakeConnection(url)
    # chunk (24) wider than needed keeps overlap+extension in one store chunk.
    gw.seed_consolidated("ds", _existing_bigchunk(list(range(24)), chunk=24 + 12))

    # overlap on 18..23 (replaced) and extend with 24..29 -> one shared chunk.
    new = _incoming(list(range(18, 30)))
    assert len(new["time"]) == 12  # 6 overlap + 6 extension
    _run_append(conn, "ds", new)

    back = _read_back(conn, "ds")
    assert list(back["time"].values) == list(range(30))
    existing = _existing(list(range(24)))["temp"].values
    # rows 0..17 untouched
    np.testing.assert_array_equal(back["temp"].values[:18], existing[:18])
    # rows 18..29 all come from the incoming data (overlap NOT clobbered)
    np.testing.assert_array_equal(back["temp"].values[18:], new["temp"].values)


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
