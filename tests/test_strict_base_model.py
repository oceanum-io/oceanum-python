# -*- coding: utf-8 -*-
"""Tests for StrictBaseModel extra='forbid' behavior."""

import pytest
from pydantic import ValidationError

from oceanum._base import StrictBaseModel
from oceanum.datamesh.query import Query, GeoFilter, TimeFilter, Aggregate, Function
from oceanum.datamesh.datasource import Datasource, Schema
from oceanum.datamesh.session import Session
from oceanum.cli.models import TokenResponse, Auth0Config


class TestStrictBaseModel:
    """Test that StrictBaseModel forbids extra fields."""

    def test_strict_base_model_forbids_extra_fields(self):
        """Test that StrictBaseModel raises ValidationError for extra fields."""

        class TestModel(StrictBaseModel):
            name: str
            value: int

        # Valid instantiation should work
        model = TestModel(name="test", value=42)
        assert model.name == "test"
        assert model.value == 42

        # Extra field should raise ValidationError
        with pytest.raises(ValidationError) as exc_info:
            TestModel(name="test", value=42, extra_field="should fail")

        # Verify the error mentions the extra field
        assert "extra_field" in str(exc_info.value)

    def test_strict_base_model_catches_typos(self):
        """Test that typos in field names are caught."""

        class TestModel(StrictBaseModel):
            datasource: str
            description: str = ""

        # Correct field names work
        model = TestModel(datasource="test-source", description="A test")
        assert model.datasource == "test-source"

        # Typo in field name raises ValidationError
        with pytest.raises(ValidationError) as exc_info:
            TestModel(datasource="test-source", descrption="typo in description")

        assert "descrption" in str(exc_info.value)


class TestQueryExtraForbid:
    """Test that Query class forbids extra fields."""

    def test_query_forbids_extra_fields(self):
        """Test that Query raises ValidationError for extra fields."""
        # Valid Query
        query = Query(datasource="test-datasource")
        assert query.datasource == "test-datasource"

        # Extra field should raise ValidationError
        with pytest.raises(ValidationError) as exc_info:
            Query(datasource="test-datasource", unknown_param="value")

        assert "unknown_param" in str(exc_info.value)

    def test_query_catches_common_typos(self):
        """Test that common typos in Query fields are caught."""
        # Typo: 'varaibles' instead of 'variables'
        with pytest.raises(ValidationError) as exc_info:
            Query(datasource="test-datasource", varaibles=["temp"])

        assert "varaibles" in str(exc_info.value)

        # Typo: 'timeFilter' instead of 'timefilter'
        with pytest.raises(ValidationError) as exc_info:
            Query(datasource="test-datasource", timeFilter={})

        assert "timeFilter" in str(exc_info.value)


class TestGeoFilterExtraForbid:
    """Test that GeoFilter class forbids extra fields."""

    def test_geofilter_forbids_extra_fields(self):
        """Test that GeoFilter raises ValidationError for extra fields."""
        # Valid GeoFilter
        geofilter = GeoFilter(geom=[0, 0, 10, 10])
        assert geofilter.geom == [0, 0, 10, 10]

        # Extra field should raise ValidationError
        with pytest.raises(ValidationError) as exc_info:
            GeoFilter(geom=[0, 0, 10, 10], unknown_field="value")

        assert "unknown_field" in str(exc_info.value)


class TestAggregateExtraForbid:
    """Test that Aggregate class forbids extra fields."""

    def test_aggregate_forbids_extra_fields(self):
        """Test that Aggregate raises ValidationError for extra fields."""
        # Valid Aggregate
        agg = Aggregate()
        assert agg.spatial is True

        # Extra field should raise ValidationError
        with pytest.raises(ValidationError) as exc_info:
            Aggregate(spatail=True)  # typo: 'spatail' instead of 'spatial'

        assert "spatail" in str(exc_info.value)


class TestFunctionExtraForbid:
    """Test that Function class forbids extra fields."""

    def test_function_forbids_extra_fields(self):
        """Test that Function raises ValidationError for extra fields."""
        # Valid Function
        func = Function(id="test-func", args={"param": 1})
        assert func.id == "test-func"

        # Extra field should raise ValidationError
        with pytest.raises(ValidationError) as exc_info:
            Function(id="test-func", args={}, unknown="value")

        assert "unknown" in str(exc_info.value)


class TestDatasourceExtraForbid:
    """Test that Datasource class forbids extra fields."""

    def test_datasource_forbids_extra_fields(self):
        """Test that Datasource raises ValidationError for extra fields."""
        # Extra field should raise ValidationError
        with pytest.raises(ValidationError) as exc_info:
            Datasource(
                id="test-ds",
                name="Test Datasource",
                driver="onzarr",
                unknown_field="value"
            )

        assert "unknown_field" in str(exc_info.value)

    def test_datasource_catches_typos(self):
        """Test that typos in Datasource fields are caught."""
        # Typo: 'discription' instead of 'description'
        with pytest.raises(ValidationError) as exc_info:
            Datasource(
                id="test-ds",
                name="Test Datasource",
                driver="onzarr",
                discription="typo"
            )

        assert "discription" in str(exc_info.value)


class TestSchemaExtraForbid:
    """Test that Schema class forbids extra fields."""

    def test_schema_forbids_extra_fields(self):
        """Test that Schema raises ValidationError for extra fields."""
        # Valid Schema
        schema = Schema(attrs={"title": "Test"})
        assert schema.attrs == {"title": "Test"}

        # Extra field should raise ValidationError
        with pytest.raises(ValidationError) as exc_info:
            Schema(attrs={}, unknown_field="value")

        assert "unknown_field" in str(exc_info.value)


class TestSessionExtraForbid:
    """Test that Session class forbids extra fields."""

    def test_session_forbids_extra_fields(self):
        """Test that Session raises ValidationError for extra fields."""
        from datetime import datetime

        # Extra field should raise ValidationError
        with pytest.raises(ValidationError) as exc_info:
            Session(
                id="test-session",
                user="test-user",
                creation_time=datetime.now(),
                end_time=datetime.now(),
                write=False,
                unknown_field="value"
            )

        assert "unknown_field" in str(exc_info.value)


class TestCLIModelsExtraForbid:
    """Test that CLI models forbid extra fields."""

    def test_auth0_config_forbids_extra_fields(self):
        """Test that Auth0Config raises ValidationError for extra fields."""
        # Valid Auth0Config
        config = Auth0Config(domain="test.auth0.com", client_id="abc123")
        assert config.domain == "test.auth0.com"

        # Extra field should raise ValidationError
        with pytest.raises(ValidationError) as exc_info:
            Auth0Config(domain="test.auth0.com", client_id="abc123", extra="value")

        assert "extra" in str(exc_info.value)

    def test_token_response_forbids_extra_fields(self):
        """Test that TokenResponse raises ValidationError for extra fields."""
        # Extra field should raise ValidationError
        with pytest.raises(ValidationError) as exc_info:
            TokenResponse(
                access_token="token123",
                expires_in=3600,
                token_type="Bearer",
                unknown_field="value"
            )

        assert "unknown_field" in str(exc_info.value)
