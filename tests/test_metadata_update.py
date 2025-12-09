#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `oceanum` package."""
import os
import pytest
import pandas
import geopandas
import xarray
import numpy

from oceanum.datamesh import Connector, Datasource
from oceanum.datamesh.exceptions import DatameshWriteError

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


def test_update_metadata(conn, dataframe):
    datasource_id = "test-write-dataframe"
    conn.write_datasource(
        datasource_id, dataframe, geom={"type": "Point", "coordinates": [174, -39]}
    )
    conn.update_metadata(datasource_id, name="new name", coordinates={"t": "time"})
    ds = conn.get_datasource(datasource_id)
    assert ds.name == "new name"
    assert ds.coordinates == {"t": "time"}
    conn.delete_datasource(datasource_id)


def test_fail_driverargs(conn, dataframe):
    datasource_id = "test-write-dataframe-fail"
    conn.write_datasource(
        datasource_id, dataframe, geom={"type": "Point", "coordinates": [174, -39]}
    )
    df = conn.load_datasource(datasource_id)
    assert (df == dataframe).all().all()
    conn.update_metadata(
        datasource_id,
        driver="null",
        driver_args={"test": "test"},
    )
    ds = conn.get_datasource(datasource_id)
    assert ds.driver == "onsql"
    assert ds.driver_args != {"test": "test"}
    conn.delete_datasource(datasource_id)
