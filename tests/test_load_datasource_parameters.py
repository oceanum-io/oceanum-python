#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""load_datasource passes its parameters on every container path.

The Dataset path sends them to /zarr as X-PARAMETERS (ZarrClient); the
DataFrame and GeoDataFrame paths fetch /data/{id} and used to drop them,
returning the parameterless data instead.
"""
import datetime
import io
import json
from unittest.mock import patch, MagicMock

import geopandas
import pandas
import pytest
import shapely

from oceanum.datamesh import Connector
from oceanum.datamesh.query import Container


def _parquet(container):
    df = pandas.DataFrame({"a": [1, 2]})
    if container == Container.GeoDataFrame:
        df = geopandas.GeoDataFrame(
            df, geometry=[shapely.Point(0, 0), shapely.Point(1, 1)], crs=4326
        )
    buf = io.BytesIO()
    df.to_parquet(buf)
    return buf.getvalue()


@pytest.fixture
def requests_mock():
    with patch("requests.Session.request") as mock_req:
        yield mock_req


def _load(requests_mock, container, parameters):
    response = MagicMock(status_code=200, content=_parquet(container))
    requests_mock.return_value = response
    conn = Connector(token="test-token")
    conn._validate_response = MagicMock()
    with patch("oceanum.datamesh.connection.Session.acquire"), patch.object(
        conn, "_stage_request", return_value=MagicMock(container=container)
    ):
        out = conn.load_datasource("some-datasource", parameters=parameters)
    data_calls = [
        c for c in requests_mock.call_args_list
        if "/data/some-datasource" in str(c.args) + str(c.kwargs)
    ]
    assert len(data_calls) == 1
    return out, data_calls[0].kwargs["headers"]


@pytest.mark.parametrize(
    "container", [Container.DataFrame, Container.GeoDataFrame], ids=["df", "gdf"]
)
def test_parameters_reach_the_data_request(requests_mock, container):
    parameters = {"cycle": datetime.datetime(2024, 1, 1), "member": 3}
    out, headers = _load(requests_mock, container, parameters)
    assert json.loads(headers["X-PARAMETERS"]) == {
        "cycle": "2024-01-01T00:00:00",
        "member": 3,
    }
    assert headers["Accept"] == "application/parquet"
    assert len(out) == 2


@pytest.mark.parametrize("parameters", [{}, None], ids=["empty", "none"])
def test_no_parameters_sends_no_header(requests_mock, parameters):
    """Unchanged request without parameters: the query engine keys its
    cache on the header, and a bare dump keeps its existing entry."""
    _, headers = _load(requests_mock, Container.DataFrame, parameters)
    assert "X-PARAMETERS" not in headers


def test_parameter_sets_do_not_share_a_download_file(requests_mock):
    """load_datasource_async can run two loads of one datasource at once."""
    requests_mock.return_value = MagicMock(
        status_code=200, content=_parquet(Container.DataFrame)
    )
    conn = Connector(token="test-token")
    conn._validate_response = MagicMock()
    paths = {
        conn._data_request("ds", parameters=p)
        for p in ({}, {"member": 1}, {"member": 2})
    }
    assert len(paths) == 3
    assert conn._data_request("ds", parameters={"a": 1, "b": 2}) == conn._data_request(
        "ds", parameters={"b": 2, "a": 1}
    )
