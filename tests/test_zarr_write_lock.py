"""zarr_write takes the datasource's write lock before its append reads.

The overlap check reads the existing coordinate; reading before the lock let
another writer commit in between, so the append was computed on a stale
version. ZarrClient.acquire_write_lock calls PATCH <gateway>/zarr/_write_lock/<id>:
405 means an older gateway without the endpoint (the lock is taken at the
first write, as before), 409 that another session is writing.
"""
from unittest.mock import MagicMock, Mock, call, patch

import numpy as np
import pytest
import xarray as xr

from oceanum.datamesh.exceptions import DatameshConnectError, DatameshWriteError
from oceanum.datamesh.zarr import ZarrClient, zarr_write


def _client(api="zarr"):
    conn = Mock(_gateway="http://gw", _auth_headers={})
    session = Mock()
    session.add_header = lambda h: h
    return ZarrClient(conn, "my_ds", session, api=api, nocache=True)


def _response(status, text=""):
    return Mock(status_code=status, text=text)


def test_it_patches_the_lock_route():
    with patch("oceanum.datamesh.zarr.retried_request", return_value=_response(200)) as req:
        assert _client().acquire_write_lock() is True
    assert req.call_args.kwargs["method"] == "PATCH"
    assert req.call_args.kwargs["url"] == "http://gw/zarr/_write_lock/my_ds"


def test_an_older_gateway_without_the_route_falls_back():
    with patch("oceanum.datamesh.zarr.retried_request", return_value=_response(405)):
        assert _client().acquire_write_lock() is False


def test_another_writer_is_a_write_error_mentioning_the_write_session():
    with patch("oceanum.datamesh.zarr.retried_request",
               return_value=_response(409, "Datasource my_ds already has a write session abc: def")):
        with pytest.raises(DatameshWriteError, match="write session"):
            _client().acquire_write_lock()


def test_a_lock_not_yet_visible_is_retried():
    with patch("oceanum.datamesh.zarr.retried_request", side_effect=[_response(503), _response(503), _response(200)]) as req, \
         patch("oceanum.datamesh.zarr.time.sleep") as sleep:
        assert _client().acquire_write_lock() is True
    assert req.call_count == 3 and sleep.call_count == 2


def test_a_persistent_server_error_is_a_connect_error_like_other_server_errors():
    with patch("oceanum.datamesh.zarr.retried_request", return_value=_response(503)), \
         patch("oceanum.datamesh.zarr.time.sleep"):
        with pytest.raises(DatameshConnectError, match="503"):
            _client().acquire_write_lock()


def test_a_409_that_is_not_a_lock_conflict_keeps_its_own_reason():
    reason = "Cannot lock for writing my_ds: stored with the 'onsql' driver"
    with patch("oceanum.datamesh.zarr.retried_request", return_value=_response(409, reason)):
        with pytest.raises(DatameshWriteError) as e:
            _client().acquire_write_lock()
    assert reason in str(e.value) and "already has a write session" not in str(e.value)


def test_unauthorized_is_a_connect_error():
    with patch("oceanum.datamesh.zarr.retried_request", return_value=_response(401)):
        with pytest.raises(DatameshConnectError):
            _client().acquire_write_lock()


def test_the_query_api_cannot_lock():
    with pytest.raises(DatameshConnectError):
        _client(api="query").acquire_write_lock()


# ---- zarr_write ----

def _conn(ds_exists=True):
    conn = Mock(_gateway="http://gw", _auth_headers={}, _is_v1=True)
    ds = Mock(_exists=ds_exists)
    ds.dataschema = Mock(coords={"time": {}})
    conn.get_datasource = Mock(return_value=ds)
    return conn


def _session():
    s = MagicMock()
    s.__enter__ = Mock(return_value=s)
    s.__exit__ = Mock(return_value=False)
    return s


def _run_append(conn, append="time", lock_error=None, lock_result=True, session=None):
    order = MagicMock()
    store = order.store
    store.acquire_write_lock.side_effect = lock_error
    store.acquire_write_lock.return_value = lock_result
    existing = xr.Dataset({"v": ("time", np.arange(3))}, coords={"time": np.array([1, 2, 3])})
    incoming = xr.Dataset({"v": ("time", np.arange(2))}, coords={"time": np.array([4, 5])})

    def open_zarr(*a, **k):
        order.open_zarr()
        return existing

    with patch("oceanum.datamesh.zarr.Session.acquire", return_value=session or _session()), \
         patch("oceanum.datamesh.zarr.ZarrClient", return_value=store), \
         patch("oceanum.datamesh.zarr.xarray.open_zarr", side_effect=open_zarr), \
         patch("oceanum.datamesh.zarr._to_zarr", side_effect=lambda *a, **k: order.to_zarr()):
        zarr_write(conn, "my_ds", incoming, append=append)
    return order


def test_an_append_locks_before_it_reads_the_existing_coordinate():
    order = _run_append(_conn())
    names = [c[0] for c in order.mock_calls if c[0] in ("store.acquire_write_lock", "open_zarr")]
    assert names[:2] == ["store.acquire_write_lock", "open_zarr"]


def test_a_concurrent_writer_stops_the_append_before_any_read_and_closes_the_session():
    session = _session()
    with pytest.raises(DatameshWriteError, match="write session"):
        _run_append(_conn(), session=session,
                    lock_error=DatameshWriteError("Cannot write to my_ds: already has a write session"))
    exc_type = session.__exit__.call_args[0][0]
    assert exc_type is DatameshWriteError  # the session closes without finalising, releasing anything taken


def test_an_older_gateway_without_the_lock_still_appends():
    order = _run_append(_conn(), lock_result=False)
    assert call.to_zarr() in order.mock_calls


def test_a_write_that_does_not_append_takes_no_explicit_lock():
    order = _run_append(_conn(), append=None)
    assert call.store.acquire_write_lock() not in order.mock_calls
