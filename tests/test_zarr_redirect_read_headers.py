from unittest.mock import Mock, patch

import pytest

from oceanum.datamesh.zarr import ZarrClient
from oceanum.datamesh.exceptions import DatameshConnectError


def _make_client():
    conn = Mock(
        _gateway="http://test",
        _auth_headers={"Authorization": "Token secret-token", "X-DATAMESH-TOKEN": "secret-token"},
    )
    session = Mock()
    session.add_header = lambda h: {**h, "X-DATAMESH-SESSIONID": "session-123"}
    return ZarrClient(conn, "test-datasource", session, api="zarr")


def test_get_item_follows_redirect_without_leaking_datamesh_headers():
    client = _make_client()

    gateway_resp = Mock(status_code=302, headers={"location": "https://storage.googleapis.com/bucket/chunk"})
    storage_resp = Mock(status_code=200, content=b"chunk-bytes")

    with patch.object(client, "_retried_request", return_value=gateway_resp) as mock_gateway_request, \
            patch("oceanum.datamesh.zarr.retried_request", return_value=storage_resp) as mock_storage_request:
        data = client._get_item("some/key")

    assert data == b"chunk-bytes"
    # The initial request to the gateway is unaffected -- redirects just
    # aren't auto-followed through the datamesh-headers-bearing session.
    _, gateway_kwargs = mock_gateway_request.call_args
    assert gateway_kwargs["allow_redirects"] is False

    # The redirect target is fetched with retry/backoff (retried_request)
    # but no http_session/headers -- no Authorization/X-DATAMESH-* headers,
    # no shared session, reach the storage host.
    mock_storage_request.assert_called_once()
    _, storage_kwargs = mock_storage_request.call_args
    assert storage_kwargs["url"] == "https://storage.googleapis.com/bucket/chunk"
    assert "headers" not in storage_kwargs
    assert "http_session" not in storage_kwargs
    assert storage_kwargs["retries"] == client.retries


def test_get_item_redirect_404_raises_keyerror():
    client = _make_client()
    gateway_resp = Mock(status_code=302, headers={"location": "https://storage.googleapis.com/bucket/missing"})
    storage_resp = Mock(status_code=404, content=b"")

    with patch.object(client, "_retried_request", return_value=gateway_resp), \
            patch("oceanum.datamesh.zarr.retried_request", return_value=storage_resp):
        with pytest.raises(KeyError):
            client._get_item("some/key")


def test_get_item_redirect_without_location_raises():
    client = _make_client()
    gateway_resp = Mock(status_code=302, headers={})

    with patch.object(client, "_retried_request", return_value=gateway_resp):
        with pytest.raises(DatameshConnectError):
            client._get_item("some/key")


def test_get_item_non_redirect_response_unaffected():
    client = _make_client()
    gateway_resp = Mock(status_code=200, content=b"direct-bytes")

    with patch.object(client, "_retried_request", return_value=gateway_resp) as mock_gateway_request, \
            patch("oceanum.datamesh.zarr.retried_request") as mock_storage_request:
        data = client._get_item("some/key")

    assert data == b"direct-bytes"
    mock_storage_request.assert_not_called()
    _, gateway_kwargs = mock_gateway_request.call_args
    assert gateway_kwargs["allow_redirects"] is False
