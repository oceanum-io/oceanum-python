import os
import pytest
import datetime
import shapely

from oceanum.datamesh import Query


def test_query_datasource():
    q = Query(datasource="test")


def test_query_timefilter():
    q = Query(
        datasource="test",
        timefilter={
            "times": [datetime.datetime(2000, 1, 1), datetime.datetime(2001, 1, 1)]
        },
    )
    q = Query(
        datasource="test",
        timefilter={"times": ["2000-01-01T00:00:00", "2001-01-01T00:00:00Z"]},
    )


def test_query_aggregate():
    q = Query(
        datasource="test",
        timefilter={"times": ["2000-01-01T00:00:00", "2001-01-01T00:00:00Z"]},
        aggregate={"operations": ["sum", "mean"]},
    )