#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `oceanum` package."""
import os
import pytest
import shapely

from click.testing import CliRunner

from oceanum.datamesh import Connector, Datasource
from oceanum.datamesh.query import GeoFilter
from oceanum import cli


@pytest.fixture
def conn():
    """Connection fixture"""
    return Connector(os.environ["DATAMESH_TOKEN"])


def test_catalog_search(conn):
    cat = conn.get_catalog(search="wave")
    ds0 = cat.ids[0]
    assert ds0 in str(cat)
    assert isinstance(cat[ds0], Datasource)
    assert len(cat)


def test_catalog_timefilter(conn):
    cat = conn.get_catalog(timefilter=["2010-01-01", "2020-01-01"])
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
