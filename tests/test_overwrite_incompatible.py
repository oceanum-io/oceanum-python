"""Overwriting a datasource with data its storage cannot take (oceanum/datamesh-gateway-v1#152).

Prod 2026-09-29: an onsql datasource (written from a DataFrame) was
overwritten with an xarray Dataset. The client deleted it, re-registered the
old record with its onsql driver and args, and wrote zarr to it; the proxy
refused, and the datasource was left as an onsql record with no table behind
it. Every retry deleted it and broke it again.

An overwrite keeps the existing record (driver and args are the server's
choice, and the client does not change them), so it is now refused, before
anything is deleted, when that record is not known to take the new data.

These tests run write_datasource against a stub server held in memory.
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

DS_ID = "ross_ice_shelf_cavity_ocean_data_asp"


class FakeServer:
    """A Connector whose network calls go to an in-memory record."""

    def __init__(self, existing, monkeypatch):
        self.calls = []
        self.record = existing
        if existing is not None:
            existing._exists = True
        conn = Connector.__new__(Connector)
        conn._get_datasource = self._get
        conn._delete = self._delete
        conn._metadata_write = self._metadata_write
        conn._data_write = self._data_write
        monkeypatch.setattr(connection_module, "zarr_write", self._zarr_write)
        self.conn = conn

    def _get(self, ds_id):
        self.calls.append("get")
        return self.record

    def _delete(self, ds_id):
        self.calls.append("delete")
        self.record = None
        return True

    def _metadata_write(self, ds):
        self.calls.append(("metadata_write", ds.driver))
        self.record = ds

    def _stored(self):
        # The proxy / query-engine write into the registered record as is.
        ds = Datasource(**self.record.model_dump(by_alias=True))
        ds._exists = True
        return ds

    def _data_write(self, ds_id, data, data_format, append, overwrite):
        self.calls.append("data_write")
        return self._stored()

    def _zarr_write(self, conn, ds_id, data, append, overwrite):
        self.calls.append("zarr_write")
        return self._stored()


def _record(driver, args, **extra):
    return Datasource(
        id=DS_ID,
        name="Ross ice shelf",
        description="kept across an overwrite",
        driver=driver,
        args=args,
        **extra,
    )


def _dataset():
    return xarray.Dataset(
        {"temp": (("time",), numpy.arange(3.0))},
        coords={"time": pandas.date_range("2026-01-01", periods=3)},
    )


def _dataframe():
    return pandas.DataFrame(
        {"temp": numpy.arange(3.0)},
        index=pandas.DatetimeIndex(pandas.date_range("2026-01-01", periods=3), name="time"),
    )


ONSQL_ARGS = {"uri": "postgresql://db/org_x", "table": DS_ID}
ZARR_ARGS = {"urlpath": f"s3://bucket/{DS_ID}"}


@pytest.mark.parametrize(
    "driver, args, data",
    [
        ("onsql", ONSQL_ARGS, _dataset),             # the #152 case
        ("vzarr", ZARR_ARGS, _dataframe),
        ("onzarr", ZARR_ARGS, _dataframe),
        ("_null", {}, _dataset),                     # metadata-only record
        ("zarr", {"urlpath": "gs://external"}, _dataset),  # driver not on the list
    ],
)
def test_incompatible_overwrite_is_refused_before_delete(monkeypatch, driver, args, data):
    existing = _record(driver, args)
    server = FakeServer(existing, monkeypatch)

    with pytest.raises(DatameshWriteError) as err:
        server.conn.write_datasource(DS_ID, data(), overwrite=True)

    message = str(err.value)
    assert "Nothing was changed" in message
    assert f"delete_datasource('{DS_ID}')" in message
    assert f"'{driver}'" in message
    # Nothing was deleted or written: the datasource is untouched.
    assert server.calls == ["get"]
    assert server.record is existing


@pytest.mark.parametrize(
    "driver, args, data, write_call",
    [
        ("vzarr", ZARR_ARGS, _dataset, "zarr_write"),
        ("onzarr", ZARR_ARGS, _dataset, "zarr_write"),
        ("onsql", ONSQL_ARGS, _dataframe, "data_write"),
    ],
)
def test_compatible_overwrite_keeps_driver_and_args(monkeypatch, driver, args, data, write_call):
    server = FakeServer(_record(driver, args), monkeypatch)

    ds = server.conn.write_datasource(DS_ID, data(), overwrite=True)

    assert server.calls[:4] == ["get", "delete", ("metadata_write", driver), write_call]
    assert ds.driver == driver
    assert ds.driver_args == args
    assert ds.description == "kept across an overwrite"


def test_compatible_overwrite_with_periods(monkeypatch):
    """Rebuilding the record re-validates pforecast/parchive timedeltas;
    that used to raise a TypeError and the overwrite failed after the delete."""
    existing = _record(
        "vzarr", ZARR_ARGS, pforecast="P7D", parchive="PT12H",
    )
    server = FakeServer(existing, monkeypatch)

    ds = server.conn.write_datasource(DS_ID, _dataset(), overwrite=True)

    assert ds.pforecast == datetime.timedelta(days=7)
    assert ds.parchive == datetime.timedelta(hours=12)


def test_new_datasource_is_not_checked(monkeypatch):
    """No existing record: nothing to keep, the server picks the storage."""
    server = FakeServer(None, monkeypatch)
    server.conn._get_datasource = lambda ds_id: (_ for _ in ()).throw(
        connection_module.DatameshConnectError("not found")
    )

    def zarr_write(conn, ds_id, data, append, overwrite):
        server.calls.append("zarr_write")
        ds = Datasource(id=ds_id, name=ds_id, driver="vzarr", args=ZARR_ARGS)
        ds._exists = True
        return ds

    monkeypatch.setattr(connection_module, "zarr_write", zarr_write)

    ds = server.conn.write_datasource(DS_ID, _dataset())

    assert "zarr_write" in server.calls
    assert ds.driver == "vzarr"


def test_parse_period():
    assert parse_period("P1DT6H") == datetime.timedelta(days=1, hours=6)
    td = datetime.timedelta(hours=3)
    assert parse_period(td) is td
    with pytest.raises(ValueError):
        parse_period("not a period")
    with pytest.raises(ValidationError):
        Datasource(id="x", name="x", driver="vzarr", pforecast="junk")
