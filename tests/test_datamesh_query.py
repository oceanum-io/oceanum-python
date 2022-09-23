#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `oceanum` package."""
import os
import pytest
import pandas
import geopandas
import xarray
import numpy

from click.testing import CliRunner

from oceanum.datamesh import Connector, Query
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


def test_query_timefilter(conn):
    tstart = pandas.Timestamp("2000-01-01T00:00:00")
    tend = pandas.Timestamp("2001-01-01T00:00:00Z")
    q = Query(
        datasource="oceanum-sea-level-rise",
        timefilter={"times": [tstart, tend]},
    )
    assert q.timefilter.times[0] == numpy.datetime64("2000-01-01")
    assert q.timefilter.times[1] == numpy.datetime64("2001-01-01")
    q = Query(
        datasource="oceanum-sea-level-rise",
        timefilter={"times": [str(tstart), str(tend)]},
    )
    assert q.timefilter.times[0] == numpy.datetime64("2000-01-01")
    assert q.timefilter.times[1] == numpy.datetime64("2001-01-01")
    q = Query(
        datasource="oceanum-sea-level-rise",
        timefilter={"times": [tstart.to_pydatetime(), tend.to_pydatetime()]},
    )
    assert q.timefilter.times[0] == numpy.datetime64("2000-01-01")
    assert q.timefilter.times[1] == numpy.datetime64("2001-01-01")
    ds = conn.query(q)
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
