#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `oceanum` package."""
import os
import pytest
import pandas
import geopandas
import pyproj
import xarray
import rioxarray
import numpy
import dask.dataframe

from click.testing import CliRunner

from oceanum.datamesh import Connector, Datasource
from oceanum.datamesh.exceptions import DatameshWriteError
from oceanum import cli

HERE = os.path.dirname(__file__)


@pytest.fixture
def conn():
    """Connection fixture"""
    return Connector()


@pytest.fixture
def dataframe():
    df = pandas.read_csv(
        os.path.join(HERE, "data", "point_data_1.csv"),
        parse_dates=True,
        index_col=0,
    )
    return df


@pytest.fixture
def dataset():
    ds = xarray.open_dataset(os.path.join(HERE, "data", "grid_data_1.nc"))
    return ds


@pytest.fixture
def geotiff():
    ds = xarray.open_dataset(os.path.join(HERE, "data", "raster_data_1.tif"))
    return ds


@pytest.fixture
def mark_for_cleanup():
    """Fixture to track and cleanup datasources created during tests."""
    created_datasources = []
    
    def register_datasource(datasource_id, conn):
        created_datasources.append((datasource_id, conn))
        return datasource_id
    
    yield register_datasource
    
    # Cleanup all registered datasources
    for datasource_id, conn in created_datasources:
        try:
            conn.delete_datasource(datasource_id)
        except Exception as e:
            pass


def test_write_dataframe(conn, dataframe, mark_for_cleanup):
    datasource_id = mark_for_cleanup("test-write-dataframe", conn)
    conn.write_datasource(
        datasource_id,
        dataframe,
        {"type": "Point", "coordinates": [174, -39]},
        overwrite=True,
    )
    df = conn.load_datasource(datasource_id)
    assert (df == dataframe).all().all()
    conn.delete_datasource(datasource_id)


def test_write_dask_dataframe(conn, dataframe, mark_for_cleanup):
    datasource_id = mark_for_cleanup("test-write-dask-dataframe", conn)
    conn.write_datasource(
        datasource_id,
        dask.dataframe.from_pandas(dataframe, npartitions=1),
        {"type": "Point", "coordinates": [174, -39]},
        overwrite=True,
    )
    df = conn.load_datasource(datasource_id, use_dask=True)
    assert isinstance(df, xarray.Dataset)
    df = df.compute()
    assert (df["u10"] == dataframe["u10"]).all()
    conn.delete_datasource(datasource_id)


def test_write_dataset(conn, dataset, mark_for_cleanup):
    datasource_id = mark_for_cleanup("test-write-dataset", conn)
    conn.write_datasource(datasource_id, dataset, overwrite=True)
    ds = conn.load_datasource(datasource_id)
    assert (ds == dataset).all()["u10"]
    conn.delete_datasource(datasource_id)


def test_write_dataset_guess(conn, dataset, mark_for_cleanup):
    datasource_id = mark_for_cleanup("test-write-dataset-guess", conn)
    conn.write_datasource(
        datasource_id,
        dataset,
        overwrite=True,
        coordinates={"t": "time", "x": "longitude", "y": "latitude"},
    )
    dsrc = conn.get_datasource(datasource_id)
    assert dsrc.geom.bounds == (173, -38, 174, -37)
    conn.delete_datasource(datasource_id)


def test_write_dataset_crs(conn, dataset, mark_for_cleanup):
    datasource_id = mark_for_cleanup("test-write-dataset-crs", conn)
    dataset_2193 = dataset.copy().rename(
        {"longitude": "easting", "latitude": "northing"}
    )
    x, y = pyproj.Transformer.from_crs(
        pyproj.CRS("EPSG:4326"), pyproj.CRS("EPSG:2193"), always_xy=True
    ).transform(dataset["longitude"], dataset["latitude"])
    dataset_2193["easting"] = x
    dataset_2193["northing"] = y
    conn.write_datasource(
        datasource_id,
        dataset_2193,
        overwrite=True,
        coordinates={"t": "time", "x": "easting", "y": "northing"},
        crs=2193,
    )
    dsrc = conn.get_datasource(datasource_id)
    assert dsrc.geom.bounds[0] == 173
    conn.delete_datasource(datasource_id)


def test_bad_coordinates_fail(conn, dataset, mark_for_cleanup):
    datasource_id = mark_for_cleanup("test-write-dataset-coord-fail", conn)
    with pytest.raises(DatameshWriteError):
        conn.write_datasource(
            datasource_id,
            dataset,
            overwrite=True,
            coordinates={
                "t": "time",
                "x": "longitude",
                "y": "latitude",
                "z": "i_do_not_exist",
            },
        )
    conn.delete_datasource(datasource_id)


def test_append_dataset(conn, dataset, mark_for_cleanup):
    datasource_id = mark_for_cleanup("test-write-dataset-append", conn)
    dataset2 = dataset.copy()
    dataset2["time"] = dataset["time"] + numpy.timedelta64(1, "D")
    conn.write_datasource(datasource_id, dataset, overwrite=True)
    conn.write_datasource(datasource_id, dataset2, append="time")
    ds = conn.load_datasource(datasource_id)
    assert len(ds["u10"]) == 73
    assert (ds["u10"][:10] == dataset["u10"][:10]).all()
    conn.delete_datasource(datasource_id)


def test_write_region_dataset(conn, dataset, mark_for_cleanup):
    datasource_id = mark_for_cleanup("test-write-dataset-region", conn)
    dataset2 = dataset.isel(time=slice(10,-10))+2
    conn.write_datasource(datasource_id, dataset, overwrite=True)
    conn.write_datasource(datasource_id, dataset2, append="time")
    ds = conn.load_datasource(datasource_id)
    assert len(ds["u10"]) == 49
    assert (ds["u10"][:10] == dataset["u10"][:10]).all()
    numpy.testing.assert_almost_equal(ds["u10"][10:-10].values,
                                      (dataset["u10"][10:-10]+2).values,
                                      decimal=3)
    assert (ds["u10"][-10:] == dataset["u10"][-10:]).all()
    conn.delete_datasource(datasource_id)


def test_write_region_chunked_dataset(conn, dataset, mark_for_cleanup):
    datasource_id = mark_for_cleanup("test-write-dataset-region-chunked", conn)
    dataset2 = dataset.isel(time=slice(10,-10))+2
    conn.write_datasource(datasource_id, dataset.chunk({"time": 7}), overwrite=True)
    conn.write_datasource(datasource_id, dataset2, append="time")
    ds = conn.load_datasource(datasource_id)
    assert len(ds["u10"]) == 49
    assert (ds["u10"][:10] == dataset["u10"][:10]).all()
    numpy.testing.assert_almost_equal(ds["u10"][10:-10].values,
                                      (dataset["u10"][10:-10]+2).values,
                                      decimal=3)
    assert (ds["u10"][-10:] == dataset["u10"][-10:]).all()
    conn.delete_datasource(datasource_id)


def test_append_dataset_fail(conn, dataset, mark_for_cleanup):
    datasource_id = mark_for_cleanup("test-write-dataset-fail", conn)
    dataset2 = dataset.copy()
    dataset2["time"] = pandas.date_range(
        dataset["time"][10].values, dataset["time"][20].values, 49
    ).values
    conn.write_datasource(datasource_id, dataset, overwrite=True)
    with pytest.raises(DatameshWriteError):
        conn.write_datasource(datasource_id, dataset2, append="time")
    conn.delete_datasource(datasource_id)


def test_write_metadata(conn, dataframe, mark_for_cleanup):
    datasource_id = mark_for_cleanup("test-write-metadata", conn)
    conn.write_datasource(
        datasource_id,
        None,
        name=datasource_id,
        coordinates={},
        driver="null",
        geometry={"type": "Point", "coordinates": [174, -39]},
        schema={"attrs": {}, "dims": {}, "coords": {}, "data_vars": {}},
        tstart="2020-01-01T00:00:00Z",
    )
    ds = conn.get_datasource(datasource_id)
    assert ds.name == datasource_id
    conn.delete_datasource(datasource_id)


def test_update_metadata(conn, dataframe, mark_for_cleanup):
    datasource_id = mark_for_cleanup("test-write-update-metadata", conn)
    conn.write_datasource(
        datasource_id, dataframe, {"type": "Point", "coordinates": [174, -39]}
    )
    conn.write_datasource(
        datasource_id,
        None,
        name="new name",
    )
    ds = conn.get_datasource(datasource_id)
    assert ds.name == "new name"
    conn.delete_datasource(datasource_id)


def test_write_metadata_with_crs(conn, dataframe, mark_for_cleanup):
    datasource_id = mark_for_cleanup("test-write-metadata-crs", conn)
    conn.write_datasource(
        datasource_id,
        None,
        name=datasource_id,
        coordinates={},
        driver="null",
        geometry={"type": "Point", "coordinates": [1686592, 5682747]},
        tstart="2020-01-01T00:00:00Z",
        crs=2193,
    )
    ds = conn.get_datasource(datasource_id)
    assert ds.dataschema.attrs["crs"] == 2193
    assert abs(ds.geom.x - 174) < 1e-4
    conn.delete_datasource(datasource_id)


def test_write_metadata_with_label(conn, dataframe, mark_for_cleanup):
    datasource_id = mark_for_cleanup("test-write-metadata-label", conn)
    conn.write_datasource(
        datasource_id,
        None,
        name=datasource_id,
        coordinates={},
        driver="null",
        geometry={"type": "Point", "coordinates": [10, -15]},
        tstart="2020-01-01T00:00:00Z",
        labels=["test_label"],
    )
    ds = conn.get_datasource(datasource_id)
    assert ds.labels[0] == "test_label"
    assert len(ds.labels) == 1
    conn.delete_datasource(datasource_id)


def test_write_metadata_with_bad_crs(conn, dataframe, mark_for_cleanup):
    datasource_id = mark_for_cleanup("test-write-metadata-bad-crs", conn)
    with pytest.raises(DatameshWriteError):
        conn.write_datasource(
            datasource_id,
            None,
            name=datasource_id,
            coordinates={},
            driver="null",
            geometry={"type": "Point", "coordinates": [1686592, 5682747]},
            tstart="2020-01-01T00:00:00Z",
        )


def test_write_raster(conn, geotiff, mark_for_cleanup):
    datasource_id = mark_for_cleanup("test-write-raster", conn)
    conn.write_datasource(datasource_id, geotiff, overwrite=True)
    ds = conn.load_datasource(datasource_id)
    assert ds["band"]
    conn.delete_datasource(datasource_id)
