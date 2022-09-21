#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `oceanum` package."""
import os
import pytest
import pandas
import geopandas
import xarray

from click.testing import CliRunner

from oceanum.datamesh import Connector
from oceanum import cli


@pytest.fixture
def conn():
    """Connection fixture"""
    return Connector(os.environ["DATAMESH_TOKEN"])


def test_query_features(conn):
    ds = conn.query({"datasource": "oceanum-sizing_giants"})
    assert isinstance(ds, geopandas.GeoDataFrame)


def test_query_dataset(conn):
    ds = conn.query({"datasource": "era5_wind10m"})
    assert isinstance(ds, xarray.Dataset)


def test_query_table(conn):
    ds = conn.query({"datasource": "oceanum-sea-level-rise"})
    assert isinstance(ds, pandas.DataFrame)


def test_command_line_interface():
    """Test the CLI."""
    runner = CliRunner()
    result = runner.invoke(cli.main)
    assert result.exit_code == 0
    assert "Oceanum.io CLI" in result.output
    help_result = runner.invoke(cli.main, ["--help"])
    assert help_result.exit_code == 0
    assert "--help  Show this message and exit." in help_result.output
