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


def test_query_geofilter():
    q = Query(
        datasource="test",
        geofilter={
            "type": "feature",
            "geom": {
                "type": "Feature",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [114.59562876453432, -28.77320223799819],
                            [114.59885236328529, -28.77290277153547],
                            [114.59911343041955, -28.77161672273214],
                            [114.59586208356448, -28.771921278480875],
                            [114.59562876453432, -28.77320223799819],
                        ]
                    ],
                },
                "properties": {},
            },
        },
    )

def test_query_geofilter_geom():
    point=shapely.geometry.Point(0,0)
    q = Query(
        datasource="test",
        geofilter={'type':'feature','geom':point}
    )


def test_query_coord():
    q = Query(
        datasource="test",
        coordfilter=[{"coord": "ensemble", "values": [1,2,3]}]
    )
