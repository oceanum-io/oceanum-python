"""Overwriting a datasource keeps what it describes, not how it was stored.

Prod 2026-09-29 (board oceanum/datamesh-gateway-v1#152): an onsql datasource
(written from a DataFrame) was overwritten with an xarray Dataset. The client
re-registered the record with the old driver and args before writing, so the
zarr write went to an onsql record, the proxy refused it, and the datasource
was left as an onsql record with no table behind it.

These tests run write_datasource against stubbed network calls.
"""

import numpy
import pandas
import pytest
import xarray

from oceanum.datamesh import Connector, Datasource
from oceanum.datamesh import connection as connection_module
from oceanum.datamesh.exceptions import DatameshWriteError

DATASOURCE = "ross_ice_shelf_cavity_ocean_data_asp"


class Recorder:
    """A Connector whose network calls are recorded instead of sent."""

    def __init__(self, existing, monkeypatch, zarr_error=None):
        self.calls = []
        self.metadata_writes = []
        conn = Connector.__new__(Connector)
        conn.get_datasource = self._get(existing)
        conn._delete = lambda ds_id: self.calls.append(("delete", ds_id)) or True
        conn._metadata_write = self._metadata_write
        conn._data_write = self._data_write
        monkeypatch.setattr(connection_module, "zarr_write", self._zarr_write(zarr_error))
        self.conn = conn

    def _get(self, existing):
        def get(ds_id):
            self.calls.append(("get", ds_id))
            return existing
        return get

    def _metadata_write(self, ds):
        self.calls.append(("metadata_write", ds.driver))
        self.metadata_writes.append(ds)

    def _data_write(self, ds_id, data, data_format, append, overwrite):
        self.calls.append(("data_write", ds_id))
        ds = Datasource(id=ds_id, name=ds_id, driver="onsql",
                        args={"uri": "postgresql://…/org_x", "table": ds_id})
        ds._exists = True
        return ds

    def _zarr_write(self, error):
        def zarr_write(conn, ds_id, data, append, overwrite):
            self.calls.append(("zarr_write", ds_id))
            if error:
                raise error
            ds = Datasource(id=ds_id, name=ds_id, driver="vzarr",
                            args={"urlpath": f"s3://bucket/{ds_id}"})
            ds._exists = True
            return ds
        return zarr_write


def _existing_onsql():
    ds = Datasource(
        id=DATASOURCE,
        name="Ross Ice Shelf cavity",
        description="CTD, microstructure and mooring data",
        tags=["antarctica", "ctd"],
        driver="onsql",
        args={"uri": "postgresql://…/org_antarcticanz", "table": DATASOURCE},
        schema={"dims": {"index": 1},
                "coords": {"index": {"dims": ["index"], "attrs": {}, "dtype": "int64", "shape": [1]}},
                "data_vars": {"date_time": {"dims": ["index"], "attrs": {}, "dtype": "datetime64[ns]", "shape": [1]},
                              "salinity_psu": {"dims": ["index"], "attrs": {}, "dtype": "float64", "shape": [1]}}},
        coordinates={"t": "date_time"},
        geom={"type": "Point", "coordinates": [174.46, -80.66]},
    )
    ds._exists = True
    return ds


def _dataset():
    time = pandas.date_range("2018-01-01", periods=3, freq="h")
    return xarray.Dataset({"salinity": ("time", numpy.arange(3.0))}, coords={"time": time})


def _dataframe():
    return pandas.DataFrame({
        "date_time": pandas.date_range("2018-01-01", periods=3, freq="h"),
        "salinity_psu": [34.1, 34.2, 34.3],
    })


def test_overwriting_onsql_with_a_dataset_writes_it_as_a_new_zarr_datasource(monkeypatch):
    rec = Recorder(_existing_onsql(), monkeypatch)

    ds = rec.conn.write_datasource(DATASOURCE, _dataset(), overwrite=True)

    # No record is registered between the delete and the data write: that
    # re-registration is what carried the onsql driver into the zarr write.
    kinds = [c[0] for c in rec.calls]
    assert kinds.index("delete") < kinds.index("zarr_write")
    assert "metadata_write" not in kinds[: kinds.index("zarr_write")]
    assert len(rec.metadata_writes) == 1

    final = rec.metadata_writes[0]
    assert final.driver == "vzarr"
    assert final.driver_args == {"urlpath": f"s3://bucket/{DATASOURCE}"}
    # The description of the data carries over...
    assert final.name == "Ross Ice Shelf cavity"
    assert final.description == "CTD, microstructure and mooring data"
    assert final.tags == ["antarctica", "ctd"]
    # ...but not the old table layout: the schema is the new data's.
    assert "time" in final.dataschema.dims and "index" not in final.dataschema.dims
    # 'date_time' does not exist in the new data, so the old mapping is dropped
    # and the coordinates are worked out from the dataset.
    assert final.coordinates.get("t") == "time"
    assert ds is final


def test_overwriting_a_dataframe_with_a_dataframe_keeps_a_valid_coordinate_mapping(monkeypatch):
    rec = Recorder(_existing_onsql(), monkeypatch)

    rec.conn.write_datasource(DATASOURCE, _dataframe(), overwrite=True)

    final = rec.metadata_writes[-1]
    assert final.driver == "onsql"
    assert final.coordinates == {"t": "date_time"}
    assert final.description == "CTD, microstructure and mooring data"


def test_explicit_properties_win_over_carried_ones(monkeypatch):
    rec = Recorder(_existing_onsql(), monkeypatch)

    rec.conn.write_datasource(DATASOURCE, _dataset(), overwrite=True,
                              description="Reprocessed", name="Ross v2")

    final = rec.metadata_writes[-1]
    assert final.description == "Reprocessed"
    assert final.name == "Ross v2"
    assert final.tags == ["antarctica", "ctd"]


def test_a_failed_write_after_the_delete_leaves_no_record_and_says_so(monkeypatch):
    rec = Recorder(_existing_onsql(), monkeypatch, zarr_error=RuntimeError("409 Conflict"))

    with pytest.raises(DatameshWriteError) as raised:
        rec.conn.write_datasource(DATASOURCE, _dataset(), overwrite=True)

    assert "was deleted before this write" in str(raised.value)
    # No half-made record: before, the pre-write registration left an onsql
    # record pointing at a table that no longer existed.
    assert rec.metadata_writes == []
