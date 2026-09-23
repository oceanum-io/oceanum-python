"""Retry semantics of oceanum.datamesh.utils.retried_request.

This client is the only layer of the datamesh platform that retries on
status codes, so these contracts are load-bearing for the whole gateway
chain (see the 2026-08 timeout/retry redesign).
"""

from unittest.mock import Mock, patch

import pytest
import requests

from oceanum.datamesh.exceptions import DatameshConnectError
from oceanum.datamesh.utils import (
    backoff_delay,
    request_was_delivered,
    retried_request,
)


def _response(status_code, headers=None):
    resp = Mock(spec=requests.Response)
    resp.status_code = status_code
    resp.headers = headers or {}
    resp.text = f"status {status_code}"
    return resp


@patch("oceanum.datamesh.utils.sleep")
@patch("oceanum.datamesh.utils.requests.request")
def test_get_retries_retryable_statuses_then_raises(mock_request, mock_sleep):
    mock_request.return_value = _response(503)
    with pytest.raises(DatameshConnectError):
        retried_request("http://gateway/x", retries=3)
    assert mock_request.call_count == 3


@patch("oceanum.datamesh.utils.sleep")
@patch("oceanum.datamesh.utils.requests.request")
def test_get_recovers_after_transient_503(mock_request, mock_sleep):
    mock_request.side_effect = [_response(503), _response(200)]
    resp = retried_request("http://gateway/x", retries=3)
    assert resp.status_code == 200
    assert mock_request.call_count == 2


@patch("oceanum.datamesh.utils.sleep")
@patch("oceanum.datamesh.utils.requests.request")
def test_post_is_not_status_retried(mock_request, mock_sleep):
    mock_request.return_value = _response(502)
    resp = retried_request("http://gateway/x", method="POST", retries=3)
    # Returned untouched: the caller owns the decision for non-idempotent
    # methods (Connection._query re-attempts once).
    assert resp.status_code == 502
    assert mock_request.call_count == 1


@patch("oceanum.datamesh.utils.sleep")
@patch("oceanum.datamesh.utils.requests.request")
def test_read_timeout_is_never_retried(mock_request, mock_sleep):
    mock_request.side_effect = requests.exceptions.ReadTimeout("slow")
    with pytest.raises(DatameshConnectError):
        retried_request("http://gateway/x", retries=3)
    assert mock_request.call_count == 1


@patch("oceanum.datamesh.utils.sleep")
@patch("oceanum.datamesh.utils.requests.request")
def test_connect_errors_are_retried_for_any_method(mock_request, mock_sleep):
    """A connect failure never reached the server, so any method may re-issue."""
    mock_request.side_effect = [
        requests.exceptions.ConnectTimeout("no route"),
        _response(200),
    ]
    resp = retried_request("http://gateway/x", method="POST", retries=3)
    assert resp.status_code == 200
    assert mock_request.call_count == 2


def _reset_after_delivery(msg="server died mid-request"):
    """A reset raised after the request was written, as an OOMKilled pod gives.

    requests wraps urllib3's ProtocolError, which itself wraps the OS error.
    """
    inner = ConnectionResetError(104, "Connection reset by peer")
    protocol = type("ProtocolError", (Exception,), {})(msg)
    protocol.__cause__ = inner
    err = requests.exceptions.ConnectionError(msg)
    err.__cause__ = protocol
    return err


@patch("oceanum.datamesh.utils.sleep")
@patch("oceanum.datamesh.utils.requests.request")
def test_reset_after_delivery_is_not_retried_for_post(mock_request, mock_sleep):
    """The 2026-09-22 cascade: an OOMKilled pod must not hand the query on.

    A POST that was delivered and then reset may have done all of its work --
    and if that work killed the pod, retrying kills the next replica too.
    """
    mock_request.side_effect = _reset_after_delivery()
    with pytest.raises(DatameshConnectError, match="after delivery"):
        retried_request("http://gateway/oceanql/", method="POST", retries=3)
    assert mock_request.call_count == 1


@patch("oceanum.datamesh.utils.sleep")
@patch("oceanum.datamesh.utils.requests.request")
def test_reset_after_delivery_is_still_retried_for_get(mock_request, mock_sleep):
    """Idempotent methods keep their retry: re-reading a chunk is harmless."""
    mock_request.side_effect = [_reset_after_delivery(), _response(200)]
    resp = retried_request("http://gateway/x", method="GET", retries=3)
    assert resp.status_code == 200
    assert mock_request.call_count == 2


@pytest.mark.parametrize(
    "name,expected",
    [
        ("NewConnectionError", False),
        ("ConnectTimeoutError", False),
        ("NameResolutionError", False),
        ("ProxyError", False),
        ("ProtocolError", True),
        ("RemoteDisconnected", True),
        ("ChunkedEncodingError", True),
    ],
)
def test_request_was_delivered_reads_the_cause_chain(name, expected):
    cause = type(name, (Exception,), {})("x")
    err = requests.exceptions.ConnectionError("x")
    err.__cause__ = cause
    assert request_was_delivered(err) is expected


def test_request_was_delivered_defaults_to_delivered_when_unknown():
    """Unrecognised shape: assume delivered, so a POST surfaces instead of
    re-issuing. Under-retrying costs one error; over-retrying cost an outage."""
    assert request_was_delivered(requests.exceptions.ConnectionError("?")) is True
    assert request_was_delivered(
        requests.exceptions.ConnectionError("?"), default=False
    ) is False


def test_request_was_delivered_survives_a_cause_cycle():
    a = requests.exceptions.ConnectionError("a")
    b = Exception("b")
    a.__cause__ = b
    b.__cause__ = a
    assert request_was_delivered(a) is True


@patch("oceanum.datamesh.utils.sleep")
@patch("oceanum.datamesh.utils.requests.request")
def test_terminal_statuses_return_untouched(mock_request, mock_sleep):
    mock_request.return_value = _response(500)
    resp = retried_request("http://gateway/x", retries=3)
    assert resp.status_code == 500
    assert mock_request.call_count == 1


def test_backoff_honors_numeric_retry_after():
    assert backoff_delay(1, _response(503, {"Retry-After": "7"})) == 7.0
    # Capped so a hostile/buggy header cannot park the client for an hour.
    assert backoff_delay(1, _response(503, {"Retry-After": "3600"})) == 120.0
    # A malformed negative value must clamp to zero, not crash time.sleep().
    assert backoff_delay(1, _response(503, {"Retry-After": "-5"})) == 0.0


def test_backoff_is_jittered_and_capped():
    delays = {backoff_delay(10) for _ in range(20)}
    assert all(d <= 15.0 for d in delays)
    assert len(delays) > 1  # jitter present
