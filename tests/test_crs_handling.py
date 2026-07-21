"""Tests for CRS handling in Datasource._set_crs method."""

import pytest
import pyproj
from oceanum.datamesh import Datasource


@pytest.fixture
def base_datasource():
    """Create a minimal datasource for testing _set_crs."""
    return Datasource(
        id="test-crs",
        name="Test CRS Handling",
        geom={"type": "Point", "coordinates": [0, 0]},
        driver="test",
    )


def test_set_crs_4326_not_stamped(base_datasource):
    """EPSG:4326 should not be stamped to attrs (it is the platform default)."""
    crs_4326 = pyproj.CRS.from_epsg(4326)
    result = base_datasource._set_crs(crs_4326)

    # Verify return value is the CRS unchanged
    assert result == crs_4326

    # Verify attrs does NOT contain 'crs' key (not stamped)
    assert "crs" not in base_datasource.dataschema.attrs


def test_set_crs_non_4326_stamped(base_datasource):
    """Non-4326 EPSG codes should be stamped to attrs."""
    crs_2193 = pyproj.CRS.from_epsg(2193)  # NZGD2000 / New Zealand Transverse Mercator
    result = base_datasource._set_crs(crs_2193)

    # Verify return value is the CRS unchanged
    assert result == crs_2193

    # Verify attrs contains the EPSG code as an integer
    assert "crs" in base_datasource.dataschema.attrs
    assert base_datasource.dataschema.attrs["crs"] == 2193
    assert isinstance(base_datasource.dataschema.attrs["crs"], int)


def test_set_crs_non_epsg_stamped_as_string(base_datasource):
    """CRS without EPSG code should be stamped as string representation."""
    # Create a CRS with a proj string that has no EPSG code
    crs_proj = pyproj.CRS("+proj=laea +lat_0=0 +lon_0=0")
    result = base_datasource._set_crs(crs_proj)

    # Verify return value is the CRS unchanged
    assert result == crs_proj

    # Verify attrs contains the CRS as a string
    assert "crs" in base_datasource.dataschema.attrs
    crs_str = base_datasource.dataschema.attrs["crs"]
    assert isinstance(crs_str, str)

    # Verify the string representation can be parsed back by pyproj
    crs_reparsed = pyproj.CRS.from_user_input(crs_str)
    assert crs_reparsed.to_proj4() == crs_proj.to_proj4()


def test_set_crs_returns_input_unchanged(base_datasource):
    """_set_crs should always return the input CRS unchanged."""
    test_cases = [
        pyproj.CRS.from_epsg(4326),  # Default
        pyproj.CRS.from_epsg(3857),  # Web Mercator
        pyproj.CRS("+proj=merc +lon_0=0 +k=1 +x_0=0 +y_0=0"),  # Proj string without EPSG
    ]

    for crs in test_cases:
        result = base_datasource._set_crs(crs)
        assert result is crs  # Same object reference


def test_bugfix_old_code_would_stamp_4326():
    """
    Verify the bug is fixed: old code compared to_epsg() (int) to "4326" (str).

    Since int != str is always True, the old code would stamp dataschema.attrs
    even for EPSG:4326 (the platform default), defeating the intent.

    This test demonstrates the exact bug by simulating the old logic.
    """
    # Simulate the old buggy comparison
    crs = pyproj.CRS.from_epsg(4326)
    epsg_value = crs.to_epsg()  # Returns int 4326

    # Old code: if crs.to_epsg() != "4326":  # int != str is always True!
    old_code_condition = epsg_value != "4326"  # This is ALWAYS True
    assert old_code_condition is True, "Bug simulation: int != str is always True"

    # Now verify the fixed code behaves correctly
    ds = Datasource(
        id="test-4326",
        name="Test EPSG:4326 Fix",
        geom={"type": "Point", "coordinates": [0, 0]},
        driver="test",
    )
    ds._set_crs(crs)

    # The fix should NOT stamp 4326 to attrs
    assert "crs" not in ds.dataschema.attrs, (
        "Bug fix verification: EPSG:4326 should not be stamped "
        "(old code would have stamped it due to int != str always being True)"
    )


def test_bugfix_none_epsg_handling():
    """
    Verify the bug is fixed for None EPSG values.

    Old code: if crs.to_epsg() != "4326":
        self.dataschema.attrs["crs"] = crs.to_epsg()  # Could store None!

    The fix stores to_string() for None EPSG values.
    """
    ds = Datasource(
        id="test-proj-only",
        name="Test Proj String CRS",
        geom={"type": "Point", "coordinates": [0, 0]},
        driver="test",
    )

    crs_proj = pyproj.CRS("+proj=laea +lat_0=0 +lon_0=0")
    assert crs_proj.to_epsg() is None, "Verify test setup: this CRS has no EPSG code"

    ds._set_crs(crs_proj)

    # The fix should store the to_string() representation, not None
    assert "crs" in ds.dataschema.attrs
    assert ds.dataschema.attrs["crs"] is not None, (
        "Bug fix verification: None EPSG should be converted to string, "
        "not stored as None"
    )
    assert isinstance(ds.dataschema.attrs["crs"], str)
