#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `oceanum` package."""
from pathlib import Path
import json
import os
import pytest
import shapely
import xarray as xr
from uuid import uuid4
from click.testing import CliRunner

from oceanum.datamesh import Connector, Datasource
from oceanum.datamesh.query import GeoFilter, TimeFilter
from oceanum.datamesh.utils import retried_request
from oceanum import cli


HERE = Path(__file__).parent


@pytest.fixture
def conn():
    """Connection fixture"""
    return Connector(os.environ["DATAMESH_TOKEN"])


@pytest.fixture
def dataset():
    ds = xr.open_dataset(HERE / "data/grid_data_1.nc")
    return ds.isel(longitude=slice(0, 2), latitude=slice(0, 2), time=slice(0, 2))


def test_catalog_ignore_broken(conn, dataset):
    tag = str(uuid4())[:8]
    datasource_id = f"test-dataset-catalog-{tag}"
    try:
        x0, x1 = dataset.longitude.values[[0, -1]]
        y0, y1 = dataset.latitude.values[[0, -1]]
        conn.write_datasource(
            datasource_id,
            dataset,
            overwrite=True,
            geom=shapely.geometry.box(x0, y0, x1, y1),
            tags=[tag, "test", "dataset", "catalog"],
        )
        # Corrupt the metadata
        ds = conn.get_datasource(datasource_id)
        data = json.loads(
            ds.model_dump_json(by_alias=True, warnings=False).encode("utf-8", "ignore")
        )
        data["coordinates"] = {"dummy_key": "dummy_val"}
        data = json.dumps(data).encode("utf-8", "ignore")
        headers = {**conn._auth_headers, "Content-Type": "application/json"}
        resp = retried_request(
            f"{conn._proto}://{conn._host}/datasource/{datasource_id}/",
            method="PATCH",
            data=data,
            headers=headers,
        )
        # The catalog should return None for this datasource
        cat = conn.get_catalog(search=datasource_id)
        assert None in list(cat)
    finally:
        data = json.loads(
            ds.model_dump_json(by_alias=True, warnings=False).encode("utf-8", "ignore")
        )
        data["coordinates"] = {}
        data = json.dumps(data).encode("utf-8", "ignore")
        resp = retried_request(
            f"{conn._proto}://{conn._host}/datasource/{datasource_id}/",
            method="PATCH",
            data=data,
            headers=headers,
        )
        conn.delete_datasource(datasource_id)


def test_catalog_search(conn):
    cat = conn.get_catalog(search="wave")
    ds0 = cat.ids[0]
    assert ds0 in str(cat)
    assert isinstance(cat[ds0], Datasource)
    assert len(cat)


def test_catalog_timefilter(conn):
    cat = conn.get_catalog(timefilter=TimeFilter(times=["2010-01-01", "2020-01-01"]))
    ds0 = cat.ids[0]
    assert ds0 in str(cat)
    assert isinstance(cat[ds0], Datasource)
    assert len(cat)


def test_catalog_timefilter_none(conn):
    cat = conn.get_catalog(timefilter=["2010-01-01", None])
    ds0 = cat.ids[0]
    assert ds0 in str(cat)
    assert isinstance(cat[ds0], Datasource)
    assert len(cat)


def test_catalog_geofilter_shapely(conn):
    bbox = shapely.geometry.box(0, 0, 10, 10)
    cat = conn.get_catalog(geofilter=bbox)
    ds0 = cat.ids[0]
    assert ds0 in str(cat)
    assert isinstance(cat[ds0], Datasource)
    assert len(cat)


def test_catalog_geofilter_bbox(conn):
    geofilter = GeoFilter(type="bbox", geom=[0, 0, 10, 10])
    cat = conn.get_catalog(geofilter=geofilter)
    ds0 = cat.ids[0]
    assert ds0 in str(cat)
    assert isinstance(cat[ds0], Datasource)
    assert len(cat)


def test_catalog_geofilter_feature(conn):
    geofilter = GeoFilter(
        type="feature",
        geom={
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[0, 0], [0, 10], [10, 10], [10, 0], [0, 0]]],
            },
        },
    )
    cat = conn.get_catalog(geofilter=geofilter)
    ds0 = cat.ids[0]
    assert ds0 in str(cat)
    assert isinstance(cat[ds0], Datasource)
    assert len(cat)
