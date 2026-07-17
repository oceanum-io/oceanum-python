"""Overwrite of an EXISTING izarr datasource must POST the metadata record
exactly once (offline, fully mocked — no live services).

``write_datasource(overwrite=True)`` handles an existing record as
delete-then-recreate: it deletes the datasource, re-POSTs the record with the
carried-over properties, and only then writes the data. The v3 write wire
(``_zarr_write_v3``) POSTs the record itself for datasources it believes are
NEW — so the recreated record must be marked as existing, or the second POST
400s with "data source with this id already exists" (found live by the
second run of integration test_oceanql_dataset.py)."""

from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import xarray as xr

import oceanum.datamesh.connection as conn_mod
from oceanum.datamesh.connection import Connector
from oceanum.datamesh.datasource import Datasource


def _data():
    return xr.Dataset(
        {"u10": (("time",), np.arange(3.0))},
        coords={"time": pd.date_range("2026-01-01", periods=3, freq="h")},
    )


def _bare_connector():
    c = object.__new__(Connector)
    c._auth_headers = {}
    c._gateway = "http://gw"
    c._proto, c._host = "http", "svc"
    c._verify = True
    c.http_session = None
    return c


def test_overwrite_existing_izarr_posts_record_once(monkeypatch):
    c = _bare_connector()

    existing = Datasource(
        id="myds", name="My ds", driver="izarr", coordinates={"t": "time"}
    )
    existing._exists = True

    posts, patches = [], []

    def fake_metadata_write(ds):
        (patches if ds._exists else posts).append(ds.id)

    c.get_datasource = MagicMock(return_value=existing)
    c._delete = MagicMock()
    c._metadata_write = fake_metadata_write

    store = MagicMock()
    monkeypatch.setattr(conn_mod, "make_v3_store", MagicMock(return_value=store))
    with patch.object(xr.Dataset, "to_zarr", MagicMock()):
        c.write_datasource(
            "myds", _data(), coordinates={"t": "time"}, overwrite=True
        )

    assert c._delete.called, "existing datasource was not deleted first"
    assert len(posts) == 1, f"metadata record POSTed {len(posts)} times"
    store._finalise.assert_called_once()
    store.close.assert_called_once()
