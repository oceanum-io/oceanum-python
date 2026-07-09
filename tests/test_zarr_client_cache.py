from unittest.mock import Mock, patch

from oceanum.datamesh.zarr import ZarrClient
from oceanum.datamesh.exceptions import DatameshConnectError


def _make_client():
    conn = Mock(_gateway="http://test", _auth_headers={})
    session = Mock()
    session.add_header = lambda h: h
    return ZarrClient(conn, "test-datasource", session, api="zarr")


def test_chunk_key_get_does_not_populate_cache():
    client = _make_client()
    with patch.object(client, "_get_item", return_value=b"chunk-bytes") as mock_get:
        data = client["0.0.0"]

    assert data == b"chunk-bytes"
    assert "0.0.0" not in client._data_cache
    mock_get.assert_called_once()


def test_zmetadata_key_get_populates_cache():
    client = _make_client()
    with patch.object(client, "_get_item", return_value=b"{}") as mock_get:
        data = client[".zmetadata"]

    assert data == b"{}"
    assert ".zmetadata" in client._data_cache
    mock_get.assert_called_once()

    # Second access is served from cache, no further network call.
    with patch.object(client, "_get_item") as mock_get2:
        data2 = client[".zmetadata"]
    assert data2 == b"{}"
    mock_get2.assert_not_called()


def test_nested_zarray_key_is_cached_by_basename():
    client = _make_client()
    with patch.object(client, "_get_item", return_value=b"{}"):
        client["some/var/.zarray"]

    assert "some/var/.zarray" in client._data_cache


def test_cache_never_exceeds_maxsize():
    client = _make_client()
    client._data_cache_maxsize = 4
    with patch.object(client, "_get_item", return_value=b"{}"):
        for i in range(10):
            client[f"var{i}/.zattrs"]

    assert len(client._data_cache) == 4
    # Most recently inserted keys survive (LRU eviction of oldest).
    assert "var9/.zattrs" in client._data_cache
    assert "var0/.zattrs" not in client._data_cache


def test_contains_403_is_not_misread_as_absent():
    client = _make_client()
    with patch.object(client, "_get_item", side_effect=DatameshConnectError("403 forbidden")):
        try:
            "some/var/.zarray" in client
            assert False, "expected DatameshConnectError to propagate"
        except DatameshConnectError:
            pass


def test_contains_404_returns_false():
    client = _make_client()
    with patch.object(client, "_get_item", side_effect=KeyError("some/var/.zarray")):
        assert ("some/var/.zarray" in client) is False
