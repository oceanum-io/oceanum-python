#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for Session.close() retry logic."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

from oceanum.datamesh.session import Session
from oceanum.datamesh.exceptions import DatameshConnectError


@pytest.fixture
def mock_connection():
    """Create a mock connection object."""
    conn = Mock()
    conn._gateway = "http://test-gateway"
    conn.http_session = Mock()
    return conn


@pytest.fixture
def test_session(mock_connection):
    """Create a test session."""
    session = Session(
        id="test-session-123",
        user="testuser",
        creation_time=datetime.now(),
        end_time=datetime.now(),
        write=True,
        allow_multiwrite=False,
        verified=False,
    )
    session._connection = mock_connection
    return session


class TestSessionCloseRetry:
    """Test suite for Session.close() retry behavior."""

    def test_close_204_first_try_no_retry(self, test_session):
        """Test that 204 response on first attempt requires no retry."""
        mock_response = Mock()
        mock_response.status_code = 204

        with patch("oceanum.datamesh.session.retried_request") as mock_request:
            with patch("oceanum.datamesh.session.time.sleep") as mock_sleep:
                mock_request.return_value = mock_response
                test_session.close(finalise_write=False)

                # Should only be called once
                assert mock_request.call_count == 1
                # No sleep should be called
                mock_sleep.assert_not_called()

    def test_close_500_then_204_with_retry(self, test_session):
        """Test that 500 error triggers retry, and 204 succeeds on retry."""
        mock_response_500 = Mock()
        mock_response_500.status_code = 500
        mock_response_500.text = "Internal Server Error"

        mock_response_204 = Mock()
        mock_response_204.status_code = 204

        with patch("oceanum.datamesh.session.retried_request") as mock_request:
            with patch("oceanum.datamesh.session.time.sleep") as mock_sleep:
                # First call returns 500, second call returns 204
                mock_request.side_effect = [mock_response_500, mock_response_204]

                test_session.close(finalise_write=False)

                # Should be called twice
                assert mock_request.call_count == 2
                # Sleep should be called once with 1 second delay
                mock_sleep.assert_called_once_with(1)

    def test_close_all_non_204_finalise_write_false_logs_warning(
        self, test_session, caplog
    ):
        """Test that all non-204 responses log warning when finalise_write=False."""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"

        with patch("oceanum.datamesh.session.retried_request") as mock_request:
            with patch("oceanum.datamesh.session.time.sleep"):
                mock_request.return_value = mock_response

                # Should not raise exception
                test_session.close(finalise_write=False)

                # Should log a warning
                assert len(caplog.records) == 1
                assert caplog.records[0].levelname == "WARNING"
                assert "Failed to close session" in caplog.text
                assert "test-session-123" in caplog.text
                assert "500" in caplog.text

    def test_close_all_non_204_finalise_write_true_raises(self, test_session):
        """Test that all non-204 responses raise when finalise_write=True."""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"

        with patch("oceanum.datamesh.session.retried_request") as mock_request:
            with patch("oceanum.datamesh.session.time.sleep"):
                mock_request.return_value = mock_response

                # Should raise exception
                with pytest.raises(DatameshConnectError) as exc_info:
                    test_session.close(finalise_write=True)

                assert "Failed to finalise write" in str(exc_info.value)
                assert "Internal Server Error" in str(exc_info.value)

    def test_close_404_treated_as_success(self, test_session):
        """Test that 404 response is treated as success."""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.text = "Not Found"

        with patch("oceanum.datamesh.session.retried_request") as mock_request:
            with patch("oceanum.datamesh.session.time.sleep") as mock_sleep:
                mock_request.return_value = mock_response

                # Should not raise exception and not log warning
                test_session.close(finalise_write=False)

                # Should only be called once
                assert mock_request.call_count == 1
                # No sleep should be called
                mock_sleep.assert_not_called()

    def test_close_410_treated_as_success(self, test_session):
        """Test that 410 Gone response is treated as success."""
        mock_response = Mock()
        mock_response.status_code = 410
        mock_response.text = "Gone"

        with patch("oceanum.datamesh.session.retried_request") as mock_request:
            with patch("oceanum.datamesh.session.time.sleep") as mock_sleep:
                mock_request.return_value = mock_response

                # Should not raise exception
                test_session.close(finalise_write=False)

                # Should only be called once
                assert mock_request.call_count == 1
                # No sleep should be called
                mock_sleep.assert_not_called()

    def test_close_retries_with_exponential_backoff(self, test_session):
        """Test that retries use exponential backoff (1s, 2s, 4s)."""
        mock_response_500 = Mock()
        mock_response_500.status_code = 500
        mock_response_500.text = "Server Error"

        with patch("oceanum.datamesh.session.retried_request") as mock_request:
            with patch("oceanum.datamesh.session.time.sleep") as mock_sleep:
                # Return 500 for all 4 attempts (initial + 3 retries)
                mock_request.return_value = mock_response_500

                test_session.close(finalise_write=False)

                # Should be called 4 times
                assert mock_request.call_count == 4
                # Sleep should be called 3 times with correct delays
                assert mock_sleep.call_count == 3
                sleep_calls = [call[0][0] for call in mock_sleep.call_args_list]
                assert sleep_calls == [1, 2, 4]

    def test_close_503_then_204_with_backoff(self, test_session):
        """Test retry sequence with 503 then 204 success."""
        mock_response_503 = Mock()
        mock_response_503.status_code = 503
        mock_response_503.text = "Service Unavailable"

        mock_response_204 = Mock()
        mock_response_204.status_code = 204

        with patch("oceanum.datamesh.session.retried_request") as mock_request:
            with patch("oceanum.datamesh.session.time.sleep") as mock_sleep:
                # First call 503, second call 204
                mock_request.side_effect = [mock_response_503, mock_response_204]

                test_session.close(finalise_write=False)

                # Should be called twice
                assert mock_request.call_count == 2
                # Sleep should be called once with 1 second
                mock_sleep.assert_called_once_with(1)

    def test_close_retries_exhaust_then_log_with_response_text(
        self, test_session, caplog
    ):
        """Test that final warning includes response text."""
        mock_response = Mock()
        mock_response.status_code = 502
        mock_response.text = "Bad Gateway - upstream service down"

        with patch("oceanum.datamesh.session.retried_request") as mock_request:
            with patch("oceanum.datamesh.session.time.sleep"):
                mock_request.return_value = mock_response

                test_session.close(finalise_write=False)

                assert len(caplog.records) == 1
                assert "Bad Gateway - upstream service down" in caplog.text

    def test_close_atexit_unregistration_before_retry_logic(self, test_session):
        """Test that atexit unregistration happens before retry logic."""
        mock_response = Mock()
        mock_response.status_code = 204

        with patch("oceanum.datamesh.session.retried_request") as mock_request:
            with patch("atexit.unregister") as mock_unregister:
                mock_request.return_value = mock_response

                test_session.close(finalise_write=False)

                # atexit.unregister should have been called
                mock_unregister.assert_called_once_with(test_session.close)

    def test_close_atexit_unregistration_handles_exception(self, test_session):
        """Test that atexit.unregister exception is silently ignored."""
        mock_response = Mock()
        mock_response.status_code = 204

        with patch("oceanum.datamesh.session.retried_request") as mock_request:
            with patch("atexit.unregister") as mock_unregister:
                # Make atexit.unregister raise an exception
                mock_unregister.side_effect = ValueError("Not registered")
                mock_request.return_value = mock_response

                # Should not raise exception
                test_session.close(finalise_write=False)

                # retried_request should still be called
                assert mock_request.call_count == 1
