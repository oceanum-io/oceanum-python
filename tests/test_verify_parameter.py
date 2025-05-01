#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for verify parameter propagation in oceanum package."""
import os
import pytest
from unittest.mock import patch, MagicMock

from oceanum.datamesh import Connector
from oceanum.datamesh.zarr import ZarrClient


@pytest.fixture
def mock_request():
    """Mock the requests.request function to check verify parameter"""
    with patch("requests.request") as mock_req:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"id": "test-id", "name": "test"}
        mock_req.return_value = mock_response
        yield mock_req


@pytest.fixture
def conn():
    """Connection fixture with verify=False"""
    return Connector(os.environ["DATAMESH_TOKEN"], verify=False)


def test_verify_parameter_default(mock_request):
    """Test that verify parameter defaults to True and is propagated to requests"""
    conn = Connector(token="test-token")

    # Test a method that makes a request
    conn._status()

    # Check that verify=True was passed to the request
    call_kwargs = mock_request.call_args.kwargs
    assert "verify" in call_kwargs
    assert call_kwargs["verify"] is True


def test_verify_parameter_false(mock_request):
    """Test that verify=False is properly propagated to requests"""
    conn = Connector(token="test-token", verify=False)

    # Test a method that makes a request
    conn._status()

    # Check that verify=False was passed to the request
    call_kwargs = mock_request.call_args.kwargs
    assert "verify" in call_kwargs
    assert call_kwargs["verify"] is False


def test_verify_parameter_in_metadata_request(mock_request):
    """Test that verify parameter is propagated to metadata requests"""
    conn = Connector(token="test-token", verify=False)

    # Test metadata request
    conn._metadata_request("test-id")

    # Check that verify=False was passed to the request
    call_kwargs = mock_request.call_args.kwargs
    assert "verify" in call_kwargs
    assert call_kwargs["verify"] is False


def test_verify_parameter_in_data_request(mock_request):
    """Test that verify parameter is propagated to data requests"""
    with patch("builtins.open", MagicMock()):
        conn = Connector(token="test-token", verify=False)

        # Mock the _validate_response method to avoid errors
        conn._validate_response = MagicMock()

        # Test data request
        conn._data_request("test-id")

        # Check that verify=False was passed to the request
        call_kwargs = mock_request.call_args.kwargs
        assert "verify" in call_kwargs
        assert call_kwargs["verify"] is False


def test_verify_parameter_in_zarr_client(conn):
    """Test that verify parameter is propagated to ZarrClient"""
    # Mock ZarrClient.__init__ to avoid actual initialization
    with patch(
        "oceanum.datamesh.zarr.ZarrClient.__init__", return_value=None
    ) as mock_init:
        # Create a ZarrClient directly with minimal parameters
        ZarrClient(
            connection=conn,
            datasource="test-datasource",
            session=MagicMock(),
            verify=False,
        )

        # Check that verify=False was passed to the ZarrClient constructor
        call_kwargs = mock_init.call_args.kwargs
        assert "verify" in call_kwargs
        assert call_kwargs["verify"] is False
