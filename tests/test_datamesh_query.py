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
from oceanum.datamesh.cache import LocalCache
from oceanum import cli


@pytest.fixture
def conn():
    """Connection fixture"""
    return Connector(os.environ["DATAMESH_TOKEN"])


def test_query_features(conn):
    ds = conn.query({"datasource": "oceanum-sizing_giants"})
    assert isinstance(ds, geopandas.GeoDataFrame)


def test_query_features_cache(conn):
    q = Query(**{"datasource": "oceanum-sizing_giants"})
    cache = LocalCache(cache_timeout=600)
    cached_file = cache._cachepath(q) + ".gpq"
    if os.path.exists(cached_file):
        os.remove(cached_file)
    ds0 = conn.query(q, cache_timeout=600)
    assert os.path.exists(cached_file)
    ds1 = conn.query(q, use_dask=False, cache_timeout=600)
    assert isinstance(ds1, geopandas.GeoDataFrame)
    pandas.testing.assert_frame_equal(ds0, ds1)


def test_query_table(conn):
    ds = conn.query({"datasource": "oceanum-sea-level-rise"})
    assert isinstance(ds, pandas.DataFrame)


def test_query_table_cache(conn):
    q = Query(**{"datasource": "oceanum-sea-level-rise"})
    cache = LocalCache(cache_timeout=600)
    cached_file = cache._cachepath(q) + ".pq"
    if os.path.exists(cached_file):
        os.remove(cached_file)
    ds0 = conn.query(q, cache_timeout=600)
    assert os.path.exists(cached_file)
    ds1 = conn.query(q, use_dask=False, cache_timeout=600)
    assert isinstance(ds1, pandas.DataFrame)
    pandas.testing.assert_frame_equal(ds0, ds1)


def test_query_dataset_lazy(conn):
    ds = conn.query({"datasource": "era5_wind10m"})
    assert isinstance(ds, xarray.Dataset) and len(ds.chunks) == 3


def test_query_dataset(conn):
    tstart = pandas.Timestamp("2000-01-01T00:00:00")
    tend = pandas.Timestamp("2001-01-01T00:00:00Z")
    q = Query(
        datasource="era5_wind10m",
        timefilter={"times": [tstart, tend]},
        geofilter={"type": "bbox", "geom": [174, -34, 175, -30]},
    )
    ds = conn.query(q, use_dask=False)
    assert isinstance(ds, xarray.Dataset) and len(ds.chunks) == 0


def test_query_dataset_cache(conn):
    tstart = pandas.Timestamp("2000-01-01T00:00:00")
    tend = pandas.Timestamp("2001-01-01T00:00:00Z")
    q = Query(
        datasource="era5_wind10m",
        timefilter={"times": [tstart, tend]},
        geofilter={"type": "bbox", "geom": [174, -34, 175, -30]},
    )

    cache = LocalCache(cache_timeout=600)
    cached_file = cache._cachepath(q) + ".nc"
    if os.path.exists(cached_file):
        os.remove(cached_file)
    ds0 = conn.query(q, use_dask=False, cache_timeout=600)
    assert os.path.exists(cached_file)
    ds1 = conn.query(q, use_dask=False, cache_timeout=600)
    assert isinstance(ds1, xarray.Dataset)
    assert ds0 == ds1


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
