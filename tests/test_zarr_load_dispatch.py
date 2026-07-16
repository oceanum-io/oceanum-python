"""Tests for the per-datasource read-wire dispatch in ``load_datasource``.

Rule (offline, fully mocked — no live services):
  * a datasource whose metadata driver is ``izarr`` is opened over the v3
    (izarr/Icechunk) wire via ``make_v3_store`` + ``open_zarr(zarr_format=3)``;
  * every other driver keeps the v2 wire path (``make_v2wire_store`` +
    ``open_zarr(zarr_format=2)``) untouched;
  * on the v3 path the stage session is released (the v3 store owns its own),
    while the v2 path hands its stage session to the v2 store.
"""
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

import oceanum.datamesh.connection as conn_mod
from oceanum.datamesh.connection import Connector
from oceanum.datamesh.query import Container, Stage


def _bare_connector():
    c = object.__new__(Connector)
    c._auth_headers = {}
    c._gateway = "http://gw"
    c._verify = True
    c.http_session = None
    return c


def _dataset_stage():
    return Stage(
        query={"datasource": "myds"},
        qhash="h",
        formats=["nc"],
        size=1,
        dlen=1,
        coordmap={},
        coordkeys={},
        container=Container.Dataset,
        sig="s",
    )


def _drive_load(driver):
    """Run load_datasource for a Dataset-container datasource and report wiring.

    Returns a dict with the wire used ('v2'/'v3'), the zarr_format passed to
    open_zarr, and whether the stage session was closed.
    """
    c = _bare_connector()
    session = MagicMock()
    c.get_datasource = MagicMock(return_value=SimpleNamespace(driver=driver))
    c._stage_request = MagicMock(return_value=_dataset_stage())

    calls = {}

    def fake_v3(conn, dsid, *, read_only=True):
        calls["wire"] = "v3"
        return SimpleNamespace(kind="v3store")

    def fake_v2(conn, dsid, sess, **kw):
        calls["wire"] = "v2"
        calls["v2_session"] = sess
        return SimpleNamespace(kind="v2store")

    def fake_open_zarr(store, **kw):
        calls["zarr_format"] = kw.get("zarr_format")
        calls["store"] = store
        return SimpleNamespace(opened=store)

    with patch.object(conn_mod.Session, "acquire", return_value=session), \
         patch.object(conn_mod, "make_v3_store", side_effect=fake_v3), \
         patch.object(conn_mod, "make_v2wire_store", side_effect=fake_v2), \
         patch.object(conn_mod.xarray, "open_zarr", side_effect=fake_open_zarr):
        result = c.load_datasource("myds")

    calls["result"] = result
    calls["session_closed"] = session.close.called
    return calls


def test_izarr_uses_v3_store():
    calls = _drive_load("izarr")
    assert calls["wire"] == "v3"
    assert calls["zarr_format"] == 3
    assert calls["store"].kind == "v3store"


def test_izarr_releases_stage_session():
    # The v3 store acquires its own session, so the stage session is released.
    calls = _drive_load("izarr")
    assert calls["session_closed"] is True


def test_onzarr_uses_v2_wire():
    calls = _drive_load("onzarr")
    assert calls["wire"] == "v2"
    assert calls["zarr_format"] == 2
    assert calls["store"].kind == "v2store"


def test_vzarr_uses_v2_wire():
    calls = _drive_load("vzarr")
    assert calls["wire"] == "v2"
    assert calls["zarr_format"] == 2


def test_v2_path_hands_stage_session_to_store():
    # The v2 wire path keeps using the stage session (not released here).
    calls = _drive_load("onzarr")
    assert calls["v2_session"] is not None
    assert calls["session_closed"] is False
