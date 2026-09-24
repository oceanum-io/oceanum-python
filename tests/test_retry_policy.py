"""Retry policy tests that pin behaviour rather than mocks.

The companion file test_retried_request.py builds exception chains from
fabricated classes named after urllib3's. That is fast but it cannot catch a
urllib3 rename, because the production classifier also matches by name -- both
sides would agree on a name that no longer exists. The classification tests
here use the *real* urllib3 classes for that reason.
"""

import pytest
import requests
import urllib3.exceptions
from unittest.mock import patch, Mock

from oceanum.datamesh.exceptions import (
    DatameshError,
    DatameshConnectError,
    DatameshUnavailableError,
    DatameshQueryError,
)
from oceanum.datamesh.utils import (
    retried_request,
    request_was_delivered,
    response_body_timed_out,
    gateway_retry_delay,
    unavailable_error,
    DATAMESH_GATEWAY_RETRY_DELAY,
)


def _response(status, headers=None, text="", json_body=None):
    resp = Mock(spec=requests.Response)
    resp.status_code = status
    resp.headers = headers or {}
    resp.text = text
    if json_body is None:
        resp.json.side_effect = ValueError("not json")
    else:
        resp.json.return_value = json_body
    return resp


# --------------------------------------------------------------------------
# The real shapes, built from real urllib3 classes
# --------------------------------------------------------------------------


def _body_read_timeout():
    """What requests raises when the body stalls after headers arrive.

    requests/models.py: `except ReadTimeoutError as e: raise ConnectionError(e)`
    -- so this is a ConnectionError, NOT a ReadTimeout, and it is the shape the
    'read timeouts are never retried' rule used to miss entirely.
    """
    inner = urllib3.exceptions.ReadTimeoutError(None, "/chunk", "Read timed out.")
    return requests.exceptions.ConnectionError(inner)


def _connect_refused():
    reason = urllib3.exceptions.NewConnectionError(
        None, "Failed to establish a new connection"
    )
    reason.__cause__ = ConnectionRefusedError(111, "Connection refused")
    err = urllib3.exceptions.MaxRetryError(None, "/x", reason=reason)
    # urllib3 raises this `from reason`; mirror that so the chain is realistic.
    err.__cause__ = reason
    return requests.exceptions.ConnectionError(err)


def _reset_after_delivery():
    inner = urllib3.exceptions.ProtocolError(
        "Connection aborted.", ConnectionResetError(104, "Connection reset by peer")
    )
    return requests.exceptions.ConnectionError(inner)


def test_real_urllib3_chains_classify_correctly():
    """Guards against a urllib3 rename silently defaulting every decision."""
    assert request_was_delivered(_connect_refused()) is False
    assert request_was_delivered(_reset_after_delivery()) is True
    assert response_body_timed_out(_body_read_timeout()) is True
    assert response_body_timed_out(_connect_refused()) is False
    assert response_body_timed_out(_reset_after_delivery()) is False


def test_max_retry_error_reason_is_followed_without_raise_from():
    """Classification must not depend on `raise ... from` having been used.

    MaxRetryError.args[0] is a formatted string, so if __cause__ is unset the
    cause walk dead-ends and falls back to the 'assume delivered' default --
    turning a connect failure, which is always safe to retry, into a refusal.
    Following `.reason` removes that dependency.
    """
    reason = urllib3.exceptions.NewConnectionError(None, "refused")
    constructed = urllib3.exceptions.MaxRetryError(None, "/x", reason=reason)
    assert constructed.__cause__ is None  # constructed, not raised
    assert request_was_delivered(requests.exceptions.ConnectionError(constructed)) is False


# --------------------------------------------------------------------------
# Mid-response failures: one re-attempt, any method
# --------------------------------------------------------------------------


@patch("oceanum.datamesh.utils.sleep")
@patch("oceanum.datamesh.utils.requests.request")
def test_body_timeout_is_retried_once_for_post(mock_request, mock_sleep):
    """Headers arrived, so the answer exists and is cached -- collect it again.

    This is the case the previous code got wrong in both directions at once:
    it fell through to the 'delivered' default, so a POST raised (losing a
    completed result) and a GET retried three times (a 51-minute stall).
    """
    mock_request.side_effect = [_body_read_timeout(), _response(200)]
    resp = retried_request("http://gateway/oceanql/", method="POST", retries=3)
    assert resp.status_code == 200
    assert mock_request.call_count == 2


@patch("oceanum.datamesh.utils.sleep")
@patch("oceanum.datamesh.utils.requests.request")
def test_body_timeout_is_retried_at_most_once(mock_request, mock_sleep):
    """Bounded independently of `retries`: a connection dying mid-body twice
    is a stall, not bad luck. Three attempts at a 1020s chunk read is 51
    minutes of a blocked dask worker."""
    mock_request.side_effect = [_body_read_timeout()] * 5
    with pytest.raises(DatameshConnectError, match="stalled mid-body twice"):
        retried_request("http://gateway/zarr/x", method="GET", retries=5)
    assert mock_request.call_count == 2


@patch("oceanum.datamesh.utils.sleep")
@patch("oceanum.datamesh.utils.requests.request")
def test_reset_with_no_response_still_blocks_post(mock_request, mock_sleep):
    """The distinction that matters: no response at all is not mid-response."""
    mock_request.side_effect = _reset_after_delivery()
    with pytest.raises(DatameshConnectError, match="not safe to repeat"):
        retried_request("http://gateway/oceanql/", method="POST", retries=3)
    assert mock_request.call_count == 1


@patch("oceanum.datamesh.utils.sleep")
@patch("oceanum.datamesh.utils.requests.request")
def test_pre_header_timeout_is_not_retried(mock_request, mock_sleep):
    """Nothing was produced, so there is nothing to collect."""
    mock_request.side_effect = requests.exceptions.ReadTimeout("timed out")
    with pytest.raises(DatameshConnectError, match="No response from"):
        retried_request("http://gateway/oceanql/", method="GET", retries=3)
    assert mock_request.call_count == 1


# --------------------------------------------------------------------------
# Method handling
# --------------------------------------------------------------------------


@patch("oceanum.datamesh.utils.sleep")
@patch("oceanum.datamesh.utils.requests.request")
def test_lowercase_method_is_treated_as_idempotent(mock_request, mock_sleep):
    """ZarrClient passes method="put" in lower case. Dropping the .upper()
    would silently make every chunk write non-idempotent."""
    mock_request.side_effect = [_response(502), _response(200)]
    resp = retried_request("http://gateway/zarr/x", method="put", retries=3)
    assert resp.status_code == 200
    assert mock_request.call_count == 2


@patch("oceanum.datamesh.utils.sleep")
@patch("oceanum.datamesh.utils.requests.request")
def test_chunk_write_502_is_retried(mock_request, mock_sleep):
    """A rolling restart of zarr-proxy produces one 502 among thousands of
    chunk writes. Failing the whole store write on it is not acceptable."""
    from oceanum.datamesh.zarr import ZarrClient
    import inspect

    assert inspect.signature(ZarrClient.__init__).parameters["method"].default == "put"
    mock_request.side_effect = [_response(502), _response(200)]
    resp = retried_request("http://gateway/zarr/ds/0.0", method="put", retries=3)
    assert resp.status_code == 200
    assert mock_request.call_count == 2


# --------------------------------------------------------------------------
# The gateway delay, and what it is for
# --------------------------------------------------------------------------


def test_gateway_delay_is_long_and_jittered():
    """Long because it paces the caller's retry loop, not only ours: the delay
    is taken before raising, so a wrapper retrying our call cannot iterate
    faster than this however short its own backoff is."""
    samples = [gateway_retry_delay() for _ in range(200)]
    assert min(samples) >= DATAMESH_GATEWAY_RETRY_DELAY * 0.5
    assert max(samples) <= DATAMESH_GATEWAY_RETRY_DELAY
    assert len(set(round(s, 3) for s in samples)) > 100  # actually jittered
    assert min(samples) >= 10  # long enough to matter against a tight loop


def test_gateway_delay_honours_retry_after():
    samples = [gateway_retry_delay(_response(503, {"Retry-After": "12"})) for _ in range(50)]
    assert min(samples) >= 9.0 and max(samples) <= 12.0


def test_unavailable_error_carries_machine_readable_guidance():
    err = unavailable_error("http://gateway/oceanql/", _response(503))
    assert isinstance(err, DatameshUnavailableError)
    assert isinstance(err, DatameshConnectError)  # existing handlers keep working
    assert isinstance(err, DatameshError)
    assert err.retry_after and err.retry_after >= 60
    assert err.status_code == 503
    # The 503 message must not invite an unchanged repeat -- that is what took
    # out both replicas on 2026-09-22.
    assert "smaller" in str(err)
    assert "Do not retry sooner than" in str(err)


def test_unavailable_error_prefers_server_retry_after():
    err = unavailable_error("http://gateway/oceanql/", _response(503, {"Retry-After": "45"}))
    assert err.retry_after == 45.0


# --------------------------------------------------------------------------
# Exception surface: catchable, categorisable, exported
# --------------------------------------------------------------------------


def test_exceptions_share_a_base_and_are_exported():
    """A caller -- human or generated -- must be able to catch a category
    without enumerating classes or importing a private module."""
    import oceanum.datamesh as pkg

    for name in (
        "DatameshError",
        "DatameshConnectError",
        "DatameshUnavailableError",
        "DatameshQueryError",
        "DatameshWriteError",
        "DatameshSessionError",
    ):
        assert hasattr(pkg, name), f"{name} is not importable from oceanum.datamesh"
        assert issubclass(getattr(pkg, name), DatameshError) or name == "DatameshError"

    # retry_after is present on every datamesh error, so a caller can read it
    # unconditionally; None means "we have no opinion", not "retry now".
    assert DatameshQueryError("x").retry_after is None


# --------------------------------------------------------------------------
# Pre-header read timeouts: terminal where the timeout is a work budget,
# retryable where it is a short arbitrary number
# --------------------------------------------------------------------------


@patch("oceanum.datamesh.utils.sleep")
@patch("oceanum.datamesh.utils.requests.request")
def test_read_timeout_is_terminal_by_default(mock_request, mock_sleep):
    """The chunk/download path keeps its protection: those timeouts are derived
    from the platform's generation budget, so hitting one means the chain failed
    and the work may still be running server-side."""
    mock_request.side_effect = requests.exceptions.ReadTimeout("timed out")
    with pytest.raises(DatameshConnectError, match="No response from"):
        retried_request("http://gateway/zarr/ds/0.0", method="GET", retries=3)
    assert mock_request.call_count == 1


@patch("oceanum.datamesh.utils.sleep")
@patch("oceanum.datamesh.utils.requests.request")
def test_read_timeout_is_retried_when_opted_in(mock_request, mock_sleep):
    """A catalog search timing out at 20s is a metadata latency spike, not a
    failed chain. CI hit exactly this and failed a live test."""
    mock_request.side_effect = [
        requests.exceptions.ReadTimeout("timed out"),
        _response(200),
    ]
    resp = retried_request(
        "http://host/datasource/", method="GET", retries=3, retry_read_timeout=True
    )
    assert resp.status_code == 200
    assert mock_request.call_count == 2


@patch("oceanum.datamesh.utils.sleep")
@patch("oceanum.datamesh.utils.requests.request")
def test_opted_in_read_timeout_is_still_bounded(mock_request, mock_sleep):
    mock_request.side_effect = requests.exceptions.ReadTimeout("timed out")
    with pytest.raises(DatameshConnectError, match="after 3 attempts"):
        retried_request(
            "http://host/datasource/", method="GET", retries=3, retry_read_timeout=True
        )
    assert mock_request.call_count == 3


@patch("oceanum.datamesh.utils.sleep")
@patch("oceanum.datamesh.utils.requests.request")
def test_opt_in_does_not_override_idempotency(mock_request, mock_sleep):
    """Even opted in, a non-idempotent method is never repeated."""
    mock_request.side_effect = requests.exceptions.ReadTimeout("timed out")
    with pytest.raises(DatameshConnectError, match="No response from"):
        retried_request(
            "http://host/datasource/", method="POST", retries=3, retry_read_timeout=True
        )
    assert mock_request.call_count == 1


def test_metadata_path_opts_in_and_the_download_path_does_not():
    """Pin the wiring by the kwargs actually passed, not by reading source text.

    A blanket opt-in on Connector._retried_request would have covered
    _data_request, which is a GET carrying the 900s download budget -- the exact
    case the default protects.
    """
    from unittest.mock import ANY
    from oceanum.datamesh.connection import Connector
    from oceanum.datamesh.utils import DATAMESH_METADATA_READ_TIMEOUT

    with patch.object(Connector, "_check_info", return_value=None):
        conn = Connector(token="dummy-not-a-real-token", service="https://gateway")

    def kwargs_for(call):
        with patch.object(Connector, "_retried_request") as rr:
            rr.return_value = _response(200, json_body={})
            try:
                call(conn)
            except Exception:
                pass
        return rr.call_args.kwargs if rr.call_args else {}

    meta = kwargs_for(lambda c: c._metadata_request("dsx"))
    assert meta.get("retry_read_timeout") is True
    assert meta.get("timeout")[1] == DATAMESH_METADATA_READ_TIMEOUT

    data = kwargs_for(lambda c: c._data_request("dsx", "application/parquet"))
    assert data.get("retry_read_timeout") in (None, False), (
        "the download path must not opt in: its read timeout is a work budget"
    )


def test_metadata_timeout_is_above_measured_latency():
    """Measured against prod 2026-09-24: catalog search median 2.9s, max 3.9s.
    10s left ~2.5x headroom and a routine spike tipped it over."""
    from oceanum.datamesh.utils import (
        DATAMESH_METADATA_READ_TIMEOUT,
        DATAMESH_READ_TIMEOUT,
    )

    assert DATAMESH_METADATA_READ_TIMEOUT > DATAMESH_READ_TIMEOUT
    assert DATAMESH_METADATA_READ_TIMEOUT >= 15


# --------------------------------------------------------------------------
# TLS: a certificate is not a blip, a dropped handshake is
# --------------------------------------------------------------------------


def _tls_error(inner):
    """The real shape: requests.SSLError -> MaxRetryError -> SSLError -> inner.

    Verified against live TLS servers 2026-09-24. Note this shape is produced by
    a failed *handshake* AND by an SSL error raised from conn.getresponse() after
    the request body was written (urllib3 connectionpool.py:535 then :824), so it
    does not identify the connect phase -- see
    test_non_certificate_tls_is_not_assumed_to_predate_delivery.
    """
    u_ssl = urllib3.exceptions.SSLError("tls failed")
    u_ssl.__cause__ = inner
    mre = urllib3.exceptions.MaxRetryError(None, "/x", reason=u_ssl)
    mre.__cause__ = u_ssl
    return requests.exceptions.SSLError(mre)


def _cert_error():
    import ssl

    return _tls_error(
        ssl.SSLCertVerificationError(1, "[SSL: CERTIFICATE_VERIFY_FAILED] self-signed")
    )


def _handshake_eof():
    import ssl

    return _tls_error(ssl.SSLEOFError("EOF occurred in violation of protocol"))


@patch("oceanum.datamesh.utils.sleep")
@patch("oceanum.datamesh.utils.requests.request")
def test_certificate_failure_is_terminal_and_says_so(mock_request, mock_sleep):
    """A certificate will not become valid on a second attempt, and the generic
    wording would point the reader at a transient fault that isn't there."""
    for method in ("GET", "POST"):
        mock_request.reset_mock()
        mock_request.side_effect = _cert_error()
        with pytest.raises(DatameshConnectError, match="certificate verification failed"):
            retried_request("https://gateway/oceanql/", method=method, retries=3)
        assert mock_request.call_count == 1, f"{method} retried a bad certificate"


@patch("oceanum.datamesh.utils.sleep")
@patch("oceanum.datamesh.utils.requests.request")
def test_non_certificate_tls_is_not_assumed_to_predate_delivery(mock_request, mock_sleep):
    """The load-bearing one: a POST must not be re-sent on a TLS failure.

    An earlier version of this code inferred "the handshake failed, so nothing
    was sent" from MaxRetryError plus an SSL class name, and retried POSTs on
    that basis. urllib3 produces the identical chain for an SSL error raised by
    conn.getresponse() -- after the request body was fully written -- so that
    inference could re-deliver a query. If that query is the one that exhausts a
    worker's memory, it is the 2026-09-22 cascade.

    Non-certificate TLS failures therefore get the ordinary cautious treatment.
    """
    mock_request.side_effect = _handshake_eof()
    with pytest.raises(DatameshConnectError):
        retried_request("https://gateway/oceanql/", method="POST", retries=3)
    assert mock_request.call_count == 1, "a POST was re-sent after a TLS failure"


@patch("oceanum.datamesh.utils.sleep")
@patch("oceanum.datamesh.utils.requests.request")
def test_non_certificate_tls_still_retries_for_idempotent_methods(mock_request, mock_sleep):
    mock_request.side_effect = [_handshake_eof(), _response(200)]
    resp = retried_request("https://gateway/zarr/x", method="GET", retries=3)
    assert resp.status_code == 200
    assert mock_request.call_count == 2


def test_certificate_helper_matches_only_certificates():
    from oceanum.datamesh.utils import certificate_verification_failed

    assert certificate_verification_failed(_cert_error()) is True
    assert certificate_verification_failed(_handshake_eof()) is False
    assert certificate_verification_failed(
        requests.exceptions.SSLError("decrypt error during read")
    ) is False
