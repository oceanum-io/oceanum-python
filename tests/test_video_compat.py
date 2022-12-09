"""Tests for `oceanum` package."""
import os
import pytest
import pandas
import geopandas
import xarray

from click.testing import CliRunner

from oceanum.datamesh import Connector, Datasource

import xarray_video as xv

HERE = os.path.dirname(__file__)


@pytest.fixture
def conn():
    """Connection fixture"""
    return Connector(
        "ccd8ac3daf2a68a10aacb909128f3f67bc3663a4", gateway="http://localhost:8000"
    )


@pytest.fixture
def video():
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
