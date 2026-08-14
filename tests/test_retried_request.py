"""Retry semantics of oceanum.datamesh.utils.retried_request.

This client is the only layer of the datamesh platform that retries on
status codes, so these contracts are load-bearing for the whole gateway
chain (see the 2026-08 timeout/retry redesign).
"""

from unittest.mock import Mock, patch

import pytest
import requests

from oceanum.datamesh.exceptions import DatameshConnectError
from oceanum.datamesh.utils import backoff_delay, retried_request


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
    mock_request.side_effect = [
        requests.exceptions.ConnectTimeout("no route"),
        _response(200),
    ]
    resp = retried_request("http://gateway/x", method="POST", retries=3)
    assert resp.status_code == 200
    assert mock_request.call_count == 2


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


def test_backoff_is_jittered_and_capped():
    delays = {backoff_delay(10) for _ in range(20)}
    assert all(d <= 15.0 for d in delays)
    assert len(delays) > 1  # jitter present
