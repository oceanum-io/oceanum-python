"""Overwriting a datasource with data of another kind (oceanum/datamesh-gateway-v1#152).

Prod 2026-09-29: an onsql datasource (written from a DataFrame) was
overwritten with an xarray Dataset. The client re-registered the record with
the old driver and args before writing, so the zarr write went to an onsql
record, the proxy refused it, and the datasource was left as an onsql record
with no table behind it.

When the old storage can take the new data (same kind), the overwrite keeps
the existing record as it always has. When it cannot, the data is written as a
new datasource and only the descriptive metadata is carried over.

These tests run write_datasource against a stubbed server that, like the real
one, keeps whatever record was last registered.
"""

import datetime

import numpy
import pandas
import pytest
import xarray
from pydantic import ValidationError

from oceanum.datamesh import Connector, Datasource
from oceanum.datamesh import connection as connection_module
from oceanum.datamesh.datasource import parse_period
from oceanum.datamesh.exceptions import DatameshWriteError

DATASOURCE = "ross_ice_shelf_cavity_ocean_data_asp"
EXPIRES = datetime.datetime(2030, 1, 1, tzinfo=datetime.timezone.utc)


class FakeServer:
    """A Connector whose network calls go to an in-memory record store."""

    def __init__(self, existing, monkeypatch, zarr_error=None):
        self.calls = []
        self.metadata_writes = []
        self.record = existing
        conn = Connector.__new__(Connector)
        conn.get_datasource = self._get
        conn._delete = self._delete
        conn._metadata_write = self._metadata_write
        conn._data_write = self._data_write
        monkeypatch.setattr(connection_module, "zarr_write", self._zarr_write(zarr_error))
        self.conn = conn

    def _get(self, ds_id):
        self.calls.append(("get", ds_id))
        return self.record

    def _delete(self, ds_id):
        self.calls.append(("delete", ds_id))
        self.record = None
        return True

    def _metadata_write(self, ds):
        self.calls.append(("metadata_write", ds.driver))
        self.metadata_writes.append(ds)
        self.record = ds

    def _stored(self, ds_id, driver, args):
        # An existing record keeps its driver and args (the proxy's and the
        # query-engine's existing-record path); otherwise a new one is made.
        if self.record is not None:
            ds = Datasource(**self.record.model_dump(by_alias=True))
        else:
            ds = Datasource(id=ds_id, name=ds_id, driver=driver, args=args)
        ds._exists = True
        return ds

    def _data_write(self, ds_id, data, data_format, append, overwrite):
        self.calls.append(("data_write", ds_id))
        return self._stored(ds_id, "onsql", {"uri": "postgresql://…/org_x", "table": ds_id})

    def _zarr_write(self, error):
        def zarr_write(conn, ds_id, data, append, overwrite):
            self.calls.append(("zarr_write", ds_id))
            if error:
                raise error
            return self._stored(ds_id, "vzarr", {"urlpath": f"s3://bucket/{ds_id}"})
        return zarr_write

    def kinds(self):
        return [c[0] for c in self.calls]


def _existing(driver="onsql", **extra):
    args = ({"uri": "postgresql://…/org_antarcticanz", "table": DATASOURCE}
            if driver == "onsql" else {"urlpath": f"gs://org-bucket/{DATASOURCE}"})
    ds = Datasource(
        id=DATASOURCE,
        name="Ross Ice Shelf cavity",
        description="CTD, microstructure and mooring data",
        tags=["antarctica", "ctd"],
        driver=driver,
        args=args,
        schema={"dims": {"index": 1},
                "coords": {"index": {"dims": ["index"], "attrs": {}, "dtype": "int64", "shape": [1]}},
                "data_vars": {"date_time": {"dims": ["index"], "attrs": {}, "dtype": "datetime64[ns]", "shape": [1]},
                              "salinity_psu": {"dims": ["index"], "attrs": {}, "dtype": "float64", "shape": [1]}}},
        coordinates={"t": "date_time"},
        geom={"type": "Point", "coordinates": [174.46, -80.66]},
        tstart=datetime.datetime(1990, 1, 1),
        tend=datetime.datetime(1991, 1, 1),
        **extra,
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


# --- another kind: written as a new datasource ---------------------------------


def test_a_dataset_over_an_onsql_datasource_is_written_as_a_new_zarr_datasource(monkeypatch):
    server = FakeServer(_existing(expires=EXPIRES, pforecast="P7D"), monkeypatch)

    ds = server.conn.write_datasource(DATASOURCE, _dataset(), overwrite=True)

    # Nothing is registered between the delete and the data write: that
    # re-registration is what carried the onsql driver into the zarr write.
    kinds = server.kinds()
    assert kinds.index("delete") < kinds.index("zarr_write")
    assert "metadata_write" not in kinds[: kinds.index("zarr_write")]

    final = server.metadata_writes[-1]
    assert final.driver == "vzarr"
    assert final.driver_args == {"urlpath": f"s3://bucket/{DATASOURCE}"}
    # What the data is carries over, including frozen and period fields...
    assert final.name == "Ross Ice Shelf cavity"
    assert final.description == "CTD, microstructure and mooring data"
    assert final.tags == ["antarctica", "ctd"]
    assert final.expires == EXPIRES
    assert final.pforecast == datetime.timedelta(days=7)
    # ...but not the old table layout or extent: those come from the new data.
    assert "time" in final.dataschema.dims and "index" not in final.dataschema.dims
    assert final.tstart == datetime.datetime(2018, 1, 1)
    # 'date_time' does not exist in the new data, so the mapping is re-derived.
    assert final.coordinates.get("t") == "time"
    assert ds is final


def test_explicit_properties_win_over_carried_ones(monkeypatch):
    server = FakeServer(_existing(), monkeypatch)

    server.conn.write_datasource(DATASOURCE, _dataset(), overwrite=True,
                                 description="Reprocessed", name="Ross v2")

    final = server.metadata_writes[-1]
    assert final.description == "Reprocessed"
    assert final.name == "Ross v2"
    assert final.tags == ["antarctica", "ctd"]


def test_a_failed_write_after_the_delete_leaves_no_record_and_says_so(monkeypatch):
    server = FakeServer(_existing(), monkeypatch, zarr_error=RuntimeError("409 Conflict"))

    with pytest.raises(DatameshWriteError) as raised:
        server.conn.write_datasource(DATASOURCE, _dataset(), overwrite=True)

    assert "was deleted before this write" in str(raised.value)
    # No record pointing at storage that holds nothing: the client registers
    # none before a write of another kind.
    assert server.metadata_writes == []


# --- same kind: the existing record is kept, as before ------------------------


def test_a_dataframe_over_an_onsql_datasource_keeps_the_existing_record(monkeypatch):
    server = FakeServer(_existing(), monkeypatch)

    server.conn.write_datasource(DATASOURCE, _dataframe(), overwrite=True)

    kinds = server.kinds()
    assert kinds.index("delete") < kinds.index("metadata_write") < kinds.index("data_write")
    final = server.metadata_writes[-1]
    assert final.driver == "onsql"
    assert final.driver_args["uri"].endswith("org_antarcticanz")
    assert final.coordinates == {"t": "date_time"}


def test_a_dataset_over_an_onzarr_datasource_stays_onzarr(monkeypatch):
    server = FakeServer(_existing(driver="onzarr"), monkeypatch)

    server.conn.write_datasource(DATASOURCE, _dataset(), overwrite=True)

    final = server.metadata_writes[-1]
    assert final.driver == "onzarr"
    assert final.driver_args == {"urlpath": f"gs://org-bucket/{DATASOURCE}"}


# --- periods --------------------------------------------------------------------


def test_parse_period_passes_a_timedelta_through():
    assert parse_period(datetime.timedelta(hours=6)) == datetime.timedelta(hours=6)


def test_an_invalid_period_is_a_validation_error_not_a_type_error():
    with pytest.raises(ValidationError):
        Datasource(id="x", name="x", driver="_null", pforecast="seven days")
