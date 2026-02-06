import json
import pytest
from unittest.mock import Mock, patch, MagicMock
import sys

sys.path.insert(0, "/home/tdurrant/source/oceanum.io/oceanum-python/src")

from oceanum.datamesh.zarr import ZarrClient, zarr_write
import xarray as xr


def test_zarr_client_group_in_headers():
    """Test that ZarrClient includes group in X-PARAMETERS header."""
    conn = Mock(_gateway="http://test", _auth_headers={}, _is_v1=True)
    session = Mock()
    # Ensure header mutation returns the header name as in the real client
    session.add_header = lambda header: header

    client = ZarrClient(conn, "test-ds", session, group="cycle/001")

    assert "X-PARAMETERS" in client.headers
    params = json.loads(client.headers["X-PARAMETERS"])
    assert params["group"] == "cycle/001"


def test_zarr_write_with_group():
    """Test that zarr_write passes group to ZarrClient and _to_zarr."""
    conn = Mock(_gateway="http://test", _auth_headers={}, _is_v1=True)
    conn.get_datasource = Mock(return_value=Mock(_exists=False))

    session_mock = MagicMock()
    session_mock.__enter__ = Mock(return_value=session_mock)
    session_mock.__exit__ = Mock(return_value=False)
    session_mock.add_header = lambda h: h

    data = xr.Dataset({"temp": (["time"], [1, 2, 3])})

    with patch("oceanum.datamesh.zarr.Session.acquire", return_value=session_mock):
        with patch("oceanum.datamesh.zarr.ZarrClient") as MockZarrClient:
            with patch("oceanum.datamesh.zarr._to_zarr") as mock_to_zarr:
                zarr_write(conn, "test-ds", data, group="cycle/001")
                # Verify ZarrClient called with group
                call_kwargs = MockZarrClient.call_args.kwargs
                assert "group" in call_kwargs
                assert call_kwargs["group"] == "cycle/001"
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
