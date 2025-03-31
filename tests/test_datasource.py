import os
import pytest
import datetime
import shapely
import pandas as pd
from pydantic import ValidationError

from oceanum.datamesh import Connector, Datasource
from oceanum import cli


@pytest.fixture
def conn():
    conn = Connector(os.environ["DATAMESH_TOKEN"])
    yield conn


@pytest.fixture
def dataframe():
    return pd.read_csv(
        os.path.join(os.path.dirname(__file__), "data", "point_data_1.csv")
    )


def test_get_catalog(conn):
    cat = conn.get_catalog()
    for datasrc in cat:
        datasrc.json()
        break


def test_datasource_properties(dataframe):
    schema = dataframe.to_xarray().to_dict(data=False)
    schema["attrs"]["name"] = "test"
    ds = Datasource(
        id="test",
        name="Test datasource",
        tstart="2000-01-01T00:00:00Z",
        geom={"type": "MultiPoint", "coordinates": [[173, -39], [174, -40]]},
        schema=schema,
        coordinates={"t": "time"},
        driver="dum",
    )
    ds._detail = True
    assert ds.bounds == (
        173.0,
        -40,
        174,
        -39,
    )
    assert ds.attributes["name"] == "test"
    assert "u10" in ds.variables


def test_all_properties(dataframe):
    ds = Datasource(
        id="test123",
        name="Test datasource",
        geom=shapely.geometry.shape({"type": "Point", "coordinates": [174, -40]}),
        schema=dataframe.to_xarray().to_dict(data=False),
        coordinates={"t": "time"},
        info={"some": "info"},
        labels=["test_label"],
        last_modified=datetime.datetime.utcnow(),
        tstart=datetime.datetime(2000, 1, 1),
        tend=datetime.datetime(2020, 1, 1),
        tags=["test1", "test2"],
        parchive="P7DT",
        driver="dum",
    )
    ds.json()


def test_fail_id(dataframe):
    with pytest.raises(ValidationError):
        ds = Datasource(
            id="test$123",  # This should fail
            name="Test datasource",
            geom={"type": "Point", "coordinates": [174, -40]},
            schema=dataframe.to_xarray().to_dict(data=False),
            coordinates={"t": "time"},
        )


def test_fail_details(dataframe):
    with pytest.raises(ValidationError):
        ds = Datasource(
            id="test123",  # This should fail
            name="Test datasource",
            geom={"type": "Point", "coordinates": [174, -40]},
            schema=dataframe.to_xarray().to_dict(data=False),
            coordinates={"t": "time"},
            details="this_is_not_a_url",
        )
