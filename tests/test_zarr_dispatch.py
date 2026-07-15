"""Tests for the PINNED per-datasource write-wire dispatch in Connector.

Rule:
  * existing datasource -> keep its frozen driver (append/update AND overwrite);
  * new datasource      -> DATAMESH_DEFAULT_ZARR_DRIVER (default "onzarr");
  * izarr -> v3 wire, everything else (onzarr/vzarr) -> v2 wire;
  * never an implicit driver conversion.
"""
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
import xarray as xr

import oceanum.datamesh.connection as conn_mod
from oceanum.datamesh.connection import Connector
from oceanum.datamesh.exceptions import DatameshConnectError


# --------------------------------------------------------------------------- #
# Pure decision function
# --------------------------------------------------------------------------- #

def _connector():
    return object.__new__(Connector)


@pytest.mark.parametrize(
    "driver, expected",
    [("onzarr", "onzarr"), ("vzarr", "vzarr"), ("izarr", "izarr")],
)
def test_resolve_keeps_existing_driver(driver, expected):
    c = _connector()
    ds = SimpleNamespace(driver=driver)
    assert c._resolve_zarr_write_driver(ds) == expected


def test_resolve_new_defaults_to_onzarr(monkeypatch):
    monkeypatch.delenv("DATAMESH_DEFAULT_ZARR_DRIVER", raising=False)
    c = _connector()
    assert c._resolve_zarr_write_driver(SimpleNamespace(driver="_null")) == "onzarr"


def test_resolve_new_honours_env_flag(monkeypatch):
    monkeypatch.setenv("DATAMESH_DEFAULT_ZARR_DRIVER", "izarr")
    c = _connector()
    assert c._resolve_zarr_write_driver(SimpleNamespace(driver="_null")) == "izarr"
    assert c._resolve_zarr_write_driver(SimpleNamespace(driver=None)) == "izarr"


# --------------------------------------------------------------------------- #
# End-to-end routing through write_datasource
# --------------------------------------------------------------------------- #

def _bare_connector():
    c = object.__new__(Connector)
    c._auth_headers = {}
    c._gateway = "http://gw"
    c._proto = "http"
    c._host = "gw"
    c._verify = True
    c.http_session = None
    return c


def _tail_ds():
    """A datasource mock that satisfies write_datasource's post-write tail."""
    ds = MagicMock()
    ds.geom = True
    ds._check_coordinates.return_value = []
    return ds


def _drive_write(existing_driver=None, overwrite=False, append=None,
                 explicit_driver=None, env_default=None, monkeypatch=None):
    """Run write_datasource for a Dataset and return which wire was used.

    Returns "v2" if zarr_write was called, "v3" if _zarr_write_v3 was called.
    """
    if env_default is not None:
        monkeypatch.setenv("DATAMESH_DEFAULT_ZARR_DRIVER", env_default)
    else:
        monkeypatch.delenv("DATAMESH_DEFAULT_ZARR_DRIVER", raising=False)

    c = _bare_connector()
    data = xr.Dataset({"v": (("time",), [1, 2, 3])}, coords={"time": [1, 2, 3]})

    # Existing datasource lookup.
    if existing_driver is not None:
        existing = MagicMock(driver=existing_driver, _exists=True)
        existing.model_dump.return_value = {"driver": existing_driver}
        c.get_datasource = MagicMock(return_value=existing)
    else:
        c.get_datasource = MagicMock(side_effect=DatameshConnectError("nope"))

    c._delete = MagicMock()
    c._metadata_write = MagicMock()

    calls = {}

    def fake_zarr_write(conn, dsid, d, ap, ov):
        calls["wire"] = "v2"
        return _tail_ds()

    def fake_v3(dsid, d, ap, ov, ds):
        calls["wire"] = "v3"
        return _tail_ds()

    # Datasource(...) for the freshly-built _ds and any overwrite reset.
    def fake_datasource(**kw):
        return MagicMock(driver=kw.get("driver", "_null"), _exists=False)

    props = {}
    if explicit_driver is not None:
        props["driver"] = explicit_driver

    with patch.object(conn_mod, "zarr_write", side_effect=fake_zarr_write), \
         patch.object(Connector, "_zarr_write_v3", side_effect=fake_v3), \
         patch.object(conn_mod, "Datasource", side_effect=fake_datasource):
        c.write_datasource(
            "myds", data, overwrite=overwrite, append=append, **props,
        )
    return calls["wire"]


def test_existing_onzarr_append_uses_v2(monkeypatch):
    assert _drive_write(existing_driver="onzarr", append="time",
                        monkeypatch=monkeypatch) == "v2"


def test_existing_vzarr_append_uses_v2(monkeypatch):
    assert _drive_write(existing_driver="vzarr", append="time",
                        monkeypatch=monkeypatch) == "v2"


def test_existing_izarr_append_uses_v3(monkeypatch):
    assert _drive_write(existing_driver="izarr", append="time",
                        monkeypatch=monkeypatch) == "v3"


def test_existing_izarr_overwrite_keeps_v3(monkeypatch):
    assert _drive_write(existing_driver="izarr", overwrite=True,
                        monkeypatch=monkeypatch) == "v3"


def test_existing_onzarr_overwrite_keeps_v2(monkeypatch):
    assert _drive_write(existing_driver="onzarr", overwrite=True,
                        monkeypatch=monkeypatch) == "v2"


def test_new_datasource_default_uses_v2(monkeypatch):
    assert _drive_write(existing_driver=None, monkeypatch=monkeypatch) == "v2"


def test_new_datasource_env_izarr_uses_v3(monkeypatch):
    assert _drive_write(existing_driver=None, env_default="izarr",
                        monkeypatch=monkeypatch) == "v3"


def test_new_datasource_explicit_izarr_uses_v3(monkeypatch):
    assert _drive_write(existing_driver=None, explicit_driver="izarr",
                        monkeypatch=monkeypatch) == "v3"
