#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `oceanum` package."""
import os
import pytest
import pandas
import geopandas
import xarray

from click.testing import CliRunner

from oceanum.datamesh import Connector, Datasource
from oceanum import cli

HERE = os.path.dirname(__file__)


@pytest.fixture
def conn():
    """Connection fixture"""
    return Connector()


@pytest.fixture
def dataframe():
    df = pandas.read_csv(
        os.path.join(HERE, "data", "point_data_1.csv"),
        parse_dates=True,
        index_col=0,
    )
    return df


@pytest.fixture
def dataset():
    ds = xarray.open_dataset(os.path.join(HERE, "data", "grid_data_1.nc"))
    return ds


def test_write_dataframe(conn, dataframe):
    datasource_id = "test-write-dataframe"
    conn.write_datasource(
        datasource_id, dataframe, {"type": "Point", "coordinates": [174, -39]}
    )
    df = conn.load_datasource(datasource_id)
    assert (df == dataframe).all().all()
    conn.delete_datasource(datasource_id)


def test_write_dataset(conn, dataset):
    datasource_id = "test-write-dataset"
    conn.write_datasource(datasource_id, dataset, overwrite=True)
    ds = conn.load_datasource(datasource_id)
    assert (ds == dataset).all()["u10"]
    conn.delete_datasource(datasource_id)


def test_write_metadata(conn, dataframe):
    datasource_id = "test-write-dataframe"
    conn.write_datasource(
        datasource_id,
        None,
        name=datasource_id,
        coordinates={},
        driver="null",
        geometry={"type": "Point", "coordinates": [174, -39]},
        schema={"attrs": {}, "dims": {}, "coords": {}, "data_vars": {}},
        tstart="2020-01-01T00:00:00Z",
    )
    ds = conn.get_datasource(datasource_id)
    assert ds.name == datasource_id
    conn.delete_datasource(datasource_id)


def test_update_metadata(conn, dataframe):
    datasource_id = "test-write-dataframe2"
    conn.write_datasource(
        datasource_id, dataframe, {"type": "Point", "coordinates": [174, -39]}
    )
    conn.write_datasource(
        datasource_id,
        None,
        name="new name",
    )
    ds = conn.get_datasource(datasource_id)
    assert ds.name == "new name"
    conn.delete_datasource(datasource_id)
