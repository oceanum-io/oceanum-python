from unittest.mock import Mock, patch

from oceanum.datamesh.zarr import ZarrClient
from oceanum.datamesh.connection import Connector


def _make_client(presigned_writes=False):
    conn = Mock(_gateway="http://test", _auth_headers={})
    session = Mock()
    session.add_header = lambda h: h
    return ZarrClient(
        conn, "test-datasource", session, api="zarr", presigned_writes=presigned_writes
    )


def test_setitem_defaults_to_classic_put_path():
    client = _make_client()
    with patch.object(client, "_presigned_set_item") as mock_presigned, \
            patch.object(client, "_retried_request") as mock_retried:
        mock_retried.return_value = Mock(status_code=200)
        client["0.0.0"] = b"some bytes"

    mock_presigned.assert_not_called()
    mock_retried.assert_called_once()


def test_setitem_uses_presigned_path_when_opted_in():
    client = _make_client(presigned_writes=True)
    with patch.object(client, "_presigned_set_item") as mock_presigned, \
            patch.object(client, "_retried_request") as mock_retried:
        client["0.0.0"] = b"some bytes"

    mock_presigned.assert_called_once_with("0.0.0", b"some bytes")
    mock_retried.assert_not_called()


def test_connector_defaults_presigned_writes_to_false():
    with patch.object(Connector, "_check_info", return_value=None):
        conn = Connector(token="dummy-token")
    assert conn._presigned_writes is False


def test_connector_respects_explicit_presigned_writes_flag():
    with patch.object(Connector, "_check_info", return_value=None):
        conn = Connector(token="dummy-token", presigned_writes=True)
    assert conn._presigned_writes is True


def test_connector_respects_env_var(monkeypatch):
    monkeypatch.setenv("DATAMESH_PRESIGNED_WRITES", "true")
    with patch.object(Connector, "_check_info", return_value=None):
        conn = Connector(token="dummy-token")
    assert conn._presigned_writes is True
