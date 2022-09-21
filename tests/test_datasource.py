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


def test_load_datasource(conn):
    cat = conn.get_catalog()
    for datasrc in cat:
        datasrc.json()
        break


def test_create_datasource(conn, dataframe):
    ds = Datasource(
        id="test-simple-datasource",
        name="Test datasource",
        tstart="2000-01-01T00:00:00Z",
        geom={"type": "Point", "coordinates": [174, -40]},
        schema=dataframe.to_xarray().to_dict(data=False),
        coordinates={"t": "time"},
        driver="dum",
    )
    conn._metadata_write(ds)
    ds_check = conn.get_datasource(ds.id)
    assert ds_check
    conn.delete_datasource("test-simple-datasource")


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
        geom={"type": "Point", "coordinates": [174, -40]},
        schema=dataframe.to_xarray().to_dict(data=False),
        coordinates={"t": "time"},
        info={"some": "info"},
        last_modified=datetime.datetime.utcnow(),
        tstart=datetime.datetime(2000, 1, 1),
        tend=datetime.datetime(2020, 1, 1),
        tags=["test1", "test2"],
        parchive="P7DT",
        driver="dum",
    )


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
