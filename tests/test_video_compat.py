"""Tests for `oceanum` package."""
import os
import pytest
import pandas
import geopandas
import xarray

from click.testing import CliRunner

from oceanum.datamesh import Connector, Datasource


HERE = os.path.dirname(__file__)


@pytest.fixture
def conn():
    """Connection fixture"""
    return Connector()


@pytest.fixture
def video():
    xv = pytest.importorskip("xarray_video")
    vid = xv.open_video(os.path.join(HERE, "data", "ocean_test_1.mp4"))
    return vid


def test_write_video(conn, video):
    datasource_id = "test-write-video"
    conn.write_datasource(
        datasource_id,
        video,
        {"type": "Point", "coordinates": [174, -39]},
        overwrite=True,
    )
    vid = conn.load_datasource(datasource_id)
    assert (vid == video).all().all()
    conn.delete_datasource(datasource_id)
