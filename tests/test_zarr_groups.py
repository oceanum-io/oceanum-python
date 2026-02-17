import pytest
from unittest.mock import Mock, patch, MagicMock
import sys

sys.path.insert(0, "/home/tdurrant/source/oceanum.io/oceanum-python/src")

from oceanum.datamesh.zarr import zarr_write
import xarray as xr


def test_zarr_write_passes_group_to_to_zarr():
    """Test that zarr_write passes group to _to_zarr."""
    conn = Mock(_gateway="http://test", _auth_headers={}, _is_v1=True)
    conn.get_datasource = Mock(return_value=Mock(_exists=False))

    session_mock = MagicMock()
    session_mock.__enter__ = Mock(return_value=session_mock)
    session_mock.__exit__ = Mock(return_value=False)
    session_mock.add_header = lambda h: h

    data = xr.Dataset({"temp": (["time"], [1, 2, 3])})

    with patch("oceanum.datamesh.zarr.Session.acquire", return_value=session_mock):
        with patch("oceanum.datamesh.zarr.ZarrClient"):
            with patch("oceanum.datamesh.zarr._to_zarr") as mock_to_zarr:
                zarr_write(conn, "test-ds", data, group="cycle/001")
                # Verify _to_zarr called with group
                to_zarr_kwargs = mock_to_zarr.call_args.kwargs
                assert "group" in to_zarr_kwargs
                assert to_zarr_kwargs["group"] == "cycle/001"


def test_zarr_write_without_group():
    """Test backward compatibility - zarr_write works without group."""
    conn = Mock(_gateway="http://test", _auth_headers={}, _is_v1=True)
    conn.get_datasource = Mock(return_value=Mock(_exists=False))

    session_mock = MagicMock()
    session_mock.__enter__ = Mock(return_value=session_mock)
    session_mock.__exit__ = Mock(return_value=False)
    session_mock.add_header = lambda h: h

    data = xr.Dataset({"temp": (["time"], [1, 2, 3])})

    with patch("oceanum.datamesh.zarr.Session.acquire", return_value=session_mock):
        with patch("oceanum.datamesh.zarr.ZarrClient"):
            with patch("oceanum.datamesh.zarr._to_zarr"):
                # Should not raise error
                zarr_write(conn, "test-ds", data)
