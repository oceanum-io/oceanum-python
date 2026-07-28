import os
import json
import warnings
import pytest
import datetime
import shapely
import numpy
from pydantic import ValidationError
from geojson_pydantic import Feature

from oceanum.datamesh import Query
from oceanum.datamesh.query import Stage, LevelFilter


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
    q = Query(
        datasource="test",
        timefilter={
            "times": [
                numpy.datetime64("2000-01-01T00:00:00"),
                numpy.datetime64("2001-01-01T00:00:00"),
            ]
        },
    )
    q = Query(
        datasource="test",
        timefilter={"times": ["P5D", "P2D"]},
    )
    q = Query(
        datasource="test",
        timefilter={"times": [-numpy.timedelta64(5, "D"), numpy.timedelta64(2, "D")]},
    )
    q = Query(
        datasource="test",
        timefilter={"times": [-datetime.timedelta(5), -datetime.timedelta(2)]},
    )


def _times(timefilter):
    """Serialize a query and return the resolved timefilter times."""
    q = Query(datasource="test", timefilter=timefilter)
    return json.loads(q.model_dump_json())["timefilter"]["times"]


def test_query_timefilter_negative_periods():
    # ISO8601-2 convention: negative period => before now, positive => after now,
    # for both timestart and tend. The sign must survive serialization.
    assert _times({"times": ["P7D", "P1D"]}) == ["P7D", "P1D"]
    assert _times({"times": ["-P7D", "P1D"]}) == ["-P7D", "P1D"]
    assert _times({"times": ["-P7D", "-P1D"]}) == ["-P7D", "-P1D"]
    # period with a time component keeps its sign
    assert _times({"times": ["-P1DT12H", "PT6H"]}) == ["-P1DT12H", "PT6H"]
    # negative python timedelta
    assert _times(
        {"times": [-datetime.timedelta(days=7), datetime.timedelta(days=2)]}
    ) == ["-P7D", "P2D"]
    # negative numpy timedelta64
    assert _times(
        {"times": [-numpy.timedelta64(5, "D"), numpy.timedelta64(2, "D")]}
    ) == ["-P5D", "P2D"]


def test_query_levelfilter():
    q = Query(
        datasource="test",
        levelfilter={"type": "range", "levels": [0.0, 10.0]},
    )
    q = Query(
        datasource="test",
        levelfilter={"type": "series", "levels": [0.0]},
    )
    q = Query(
        datasource="test",
        levelfilter={"type": "series", "levels": [0.0, 10.0, 20.0]},
    )
    q = Query(
        datasource="test",
        levelfilter={"type": "trajectory", "levels": [0.0, 10.0, 20.0]},
    )


def test_query_levelfilter_range_requires_two_values():
    with pytest.raises(ValidationError) as excinfo:
        LevelFilter(type="range", levels=[0.0])
    message = str(excinfo.value)
    assert "type='series'" in message
    assert "exactly 2 values" in message

    with pytest.raises(ValidationError):
        LevelFilter(type="range", levels=[0.0, 5.0, 10.0])

    # type defaults to 'range', so an omitted type must be caught too - this is
    # the shape that reached production and crashed the query engine.
    with pytest.raises(ValidationError) as excinfo:
        LevelFilter(levels=[0.0])
    assert "type='series'" in str(excinfo.value)


def test_query_levelfilter_series_requires_at_least_one_value():
    with pytest.raises(ValidationError):
        LevelFilter(type="series", levels=[])


def test_query_levelfilter_trajectory_requires_at_least_one_value():
    with pytest.raises(ValidationError):
        LevelFilter(type="trajectory", levels=[])


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
    point = shapely.geometry.Point(0, 0)
    q = Query(datasource="test", geofilter={"type": "feature", "geom": point})


def test_query_geofilter_type_must_match_geom():
    # A bbox list with type='feature' used to reach the query engine and die
    # there with "'list' object has no attribute 'model_dump'".
    with pytest.raises(ValidationError) as excinfo:
        Query(datasource="test", geofilter={"type": "feature", "geom": [0, 0, 1, 1]})
    assert "type='bbox'" in str(excinfo.value)

    # ... and a feature with type='bbox' with "'Feature' object is not subscriptable".
    with pytest.raises(ValidationError) as excinfo:
        Query(
            datasource="test",
            geofilter={
                "type": "bbox",
                "geom": {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [0, 0]},
                    "properties": {},
                },
            },
        )
    assert "type='feature'" in str(excinfo.value)


def test_query_geofilter_accepts_feature_instance():
    # geom is typed Union[List[float], Feature] but a constructed Feature was
    # rejected outright - only the dict form was accepted.
    feature = Feature(
        type="Feature",
        geometry={"type": "Point", "coordinates": [0.0, 0.0]},
        properties={},
    )
    q = Query(datasource="test", geofilter={"type": "feature", "geom": feature})
    assert q.geofilter.geom.geometry.type == "Point"


def test_query_geofilter_bbox_must_be_ordered():
    Query(datasource="test", geofilter={"type": "bbox", "geom": [0, 0, 1, 1]})
    for reversed_bbox in ([1, 0, 0, 1], [0, 1, 1, 0]):
        with pytest.raises(ValidationError) as excinfo:
            Query(datasource="test", geofilter={"type": "bbox", "geom": reversed_bbox})
        assert "x_min <= x_max" in str(excinfo.value)


def test_query_geofilter_resolution_not_negative():
    with pytest.raises(ValidationError):
        Query(
            datasource="test",
            geofilter={"type": "bbox", "geom": [0, 0, 1, 1], "resolution": -5},
        )


def test_query_timefilter_series_requires_a_value():
    Query(datasource="test", timefilter={"type": "series", "times": ["2000-01-01"]})
    for type_ in ("series", "trajectory"):
        with pytest.raises(ValidationError) as excinfo:
            Query(datasource="test", timefilter={"type": type_, "times": []})
        assert "at least 1 value" in str(excinfo.value)


def test_query_timefilter_resolution_must_be_a_freqstr():
    Query(
        datasource="test",
        timefilter={"times": ["2000-01-01", "2001-01-01"], "resolution": "3h"},
    )
    Query(
        datasource="test",
        timefilter={"times": ["2000-01-01", "2001-01-01"], "resolution": "native"},
    )
    with pytest.raises(ValidationError) as excinfo:
        Query(
            datasource="test",
            timefilter={"times": ["2000-01-01", "2001-01-01"], "resolution": "banana"},
        )
    assert "not a valid pandas frequency string" in str(excinfo.value)


def test_query_limit_must_be_positive():
    Query(datasource="test", limit=10)
    for bad in (0, -5):
        with pytest.raises(ValidationError):
            Query(datasource="test", limit=bad)


def test_query_coord():
    q = Query(
        datasource="test", coordfilter=[{"coord": "ensemble", "values": [1, 2, 3]}]
    )


def test_query_coord_values_not_empty():
    with pytest.raises(ValidationError):
        Query(datasource="test", coordfilter=[{"coord": "ensemble", "values": []}])


def test_stage_resp():
    s = Stage(
        query={"datasource": "my-datasource"},
        qhash="abc",
        formats=["nc"],
        size=1000,
        dlen=100,
        coordmap={"var": "tyx"},
        coordkeys={"var": "tyx"},
        container="dataset",
        sig="efg",
    )


def test_query_unknown_param_close_match():
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        q = Query(datasource="test", tiemfilter=None)
    assert len(w) == 1
    assert "tiemfilter" in str(w[0].message)
    assert "timefilter" in str(w[0].message)
    assert "did you mean" in str(w[0].message).lower()
    # Ensure the misspelled param is not in the serialized output
    assert "tiemfilter" not in q.model_dump()


def test_query_unknown_param_no_match():
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        q = Query(datasource="test", zzzzz="bar")
    assert len(w) == 1
    assert "zzzzz" in str(w[0].message)
    assert "will be ignored" in str(w[0].message).lower()
    assert "zzzzz" not in q.model_dump()


def test_query_valid_params_no_warning():
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        q = Query(datasource="test", variables=["a", "b"])
    assert len(w) == 0
