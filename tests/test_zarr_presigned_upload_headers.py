from unittest.mock import Mock, patch

from oceanum.datamesh.zarr import ZarrClient


def _make_client():
    conn = Mock(
        _gateway="http://test",
        _auth_headers={"Authorization": "Token secret-token", "X-DATAMESH-TOKEN": "secret-token"},
    )
    session = Mock()
    session.add_header = lambda h: {**h, "X-DATAMESH-SESSIONID": "session-123"}
    return ZarrClient(conn, "test-datasource", session, api="zarr", presigned_writes=True)


def test_presigned_upload_strips_datamesh_auth_headers():
    client = _make_client()
    assert client.headers == {
        "Authorization": "Token secret-token",
        "X-DATAMESH-TOKEN": "secret-token",
        "X-DATAMESH-SESSIONID": "session-123",
    }

    presign_response = Mock(status_code=200)
    presign_response.json.return_value = {
        "url": "https://storage.googleapis.com/bucket/staging/abc",
        "physical_address": "gs://bucket/staging/abc",
    }
    confirm_response = Mock(status_code=200)

    with patch.object(client, "_retried_request", side_effect=[presign_response]), \
            patch("oceanum.datamesh.zarr.retried_request") as mock_retried_request:
        # module-level retried_request is used for both the storage PUT and
        # the confirm POST -- return the upload response first, then confirm.
        mock_retried_request.side_effect = [Mock(status_code=200), confirm_response]
        client["some/key"] = b"payload"

    assert mock_retried_request.call_count == 2
    upload_kwargs = mock_retried_request.call_args_list[0].kwargs
    assert upload_kwargs["url"] == "https://storage.googleapis.com/bucket/staging/abc"
    # Every key baked into the datamesh-facing session headers must be
    # explicitly nulled out for the direct-to-storage PUT, or requests will
    # merge them in from the pooled session and leak them to the presigned
    # URL's host.
    assert upload_kwargs["headers"] == {
        "Authorization": None,
        "X-DATAMESH-TOKEN": None,
        "X-DATAMESH-SESSIONID": None,
    }
    assert upload_kwargs["http_session"] is client.http_session
