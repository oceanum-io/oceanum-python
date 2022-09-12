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
    return Connector(os.environ["DATAMESH_TOKEN"], gateway="http://localhost:8000")


def test_query_features(conn):
    cat = conn.get_catalog()
    for dataset in cat:
        if dataset.container == geopandas.GeoDataFrame:
            ds = conn.query({"datasource": dataset.id})
            assert isinstance(ds, geopandas.GeoDataFrame)
            break


# def test_query_dataset(conn):
#     cat = conn.get_catalog()
#     for dataset in cat:
#         if dataset.container == xarray.Dataset:
#             ds = dataset.load()
#             assert isinstance(ds, xarray.Dataset)
#             break


def test_query_table(conn):
    cat = conn.get_catalog()
    for dataset in cat:
        if dataset.container == pandas.DataFrame:
            ds = conn.query({"datasource": dataset.id})
            assert isinstance(ds, pandas.DataFrame)
            break


def test_command_line_interface():
    """Test the CLI."""
    runner = CliRunner()
    result = runner.invoke(cli.main)
    assert result.exit_code == 0
    assert "Oceanum.io CLI" in result.output
    help_result = runner.invoke(cli.main, ["--help"])
    assert help_result.exit_code == 0
    assert "--help  Show this message and exit." in help_result.output
