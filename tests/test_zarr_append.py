"""Unit tests for zarr_write append validation."""
import pytest
from unittest.mock import Mock, patch, MagicMock
import numpy as np
import xarray as xr

from oceanum.datamesh.zarr import zarr_write
from oceanum.datamesh.exceptions import DatameshWriteError


def _append_conn(ds_exists=True):
    conn = Mock(_gateway="http://test", _auth_headers={}, _is_v1=True)
    ds = Mock(_exists=ds_exists)
    ds.dataschema = Mock(coords={"time": {}})
    conn.get_datasource = Mock(return_value=ds)
    return conn


def _session_mock():
    session_mock = MagicMock()
    session_mock.__enter__ = Mock(return_value=session_mock)
    session_mock.__exit__ = Mock(return_value=False)
    session_mock.add_header = lambda h: h
    return session_mock


def test_zarr_write_append_rejects_non_monotonic_incoming_coordinate():
    conn = _append_conn(ds_exists=True)
    session_mock = _session_mock()

    existing = xr.Dataset(
        data_vars={"incli": ("time", np.arange(5))},
        coords={"time": np.array([1, 2, 3, 4, 5])},
    )
    incoming = xr.Dataset(
        data_vars={"incli": ("time", np.arange(4))},
        coords={"time": np.array([3, 5, 4, 6])},
    )

    with patch("oceanum.datamesh.zarr.Session.acquire", return_value=session_mock):
        with patch("oceanum.datamesh.zarr.ZarrClient"):
            with patch("oceanum.datamesh.zarr.xarray.open_zarr", return_value=existing):
                with pytest.raises(DatameshWriteError, match="must be monotonic non-decreasing"):
                    zarr_write(conn, "test-ds", incoming, append="time")


def test_zarr_write_append_rejects_non_contiguous_overlap_indices():
    conn = _append_conn(ds_exists=True)
    session_mock = _session_mock()

    # Existing time is non-monotonic, causing overlap indices to be non-contiguous.
    existing = xr.Dataset(
        data_vars={"incli": ("time", np.arange(6))},
        coords={"time": np.array([1, 2, 3, 10, 4, 5])},
    )
    incoming = xr.Dataset(
        data_vars={"incli": ("time", np.arange(4))},
        coords={"time": np.array([3, 4, 5, 6])},
    )

    with patch("oceanum.datamesh.zarr.Session.acquire", return_value=session_mock):
        with patch("oceanum.datamesh.zarr.ZarrClient"):
            with patch("oceanum.datamesh.zarr.xarray.open_zarr", return_value=existing):
                with pytest.raises(DatameshWriteError, match="non-contiguous"):
                    zarr_write(conn, "test-ds", incoming, append="time")


def test_zarr_write_append_rejects_overlap_timestamp_mismatch():
    conn = _append_conn(ds_exists=True)
    session_mock = _session_mock()

    existing = xr.Dataset(
        data_vars={"incli": ("time", np.arange(6))},
        coords={"time": np.array([1, 2, 3, 4, 5, 6])},
    )
    # Overlap bounds [3, 9] exist, but first overlap timestamps do not match [3,4,5,6].
    incoming = xr.Dataset(
        data_vars={"incli": ("time", np.arange(7))},
        coords={"time": np.array([3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5])},
    )

    with patch("oceanum.datamesh.zarr.Session.acquire", return_value=session_mock):
        with patch("oceanum.datamesh.zarr.ZarrClient"):
            with patch("oceanum.datamesh.zarr.xarray.open_zarr", return_value=existing):
                with pytest.raises(DatameshWriteError, match="overlap timestamps do not match"):
                    zarr_write(conn, "test-ds", incoming, append="time")
