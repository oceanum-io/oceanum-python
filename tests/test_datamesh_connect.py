#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `oceanum` package."""
import os
import pytest

from click.testing import CliRunner

from oceanum.datamesh import Connector, Datasource
from oceanum import cli


pytestmark = pytest.mark.requires_datamesh_token


@pytest.fixture
def conn():
    """Connection fixture"""
    return Connector(os.environ["DATAMESH_TOKEN"])


# These tests run against prod. Without a limit they list everything the
# token can see: 100-170 s per query with a broad token, enough to take the
# metadata server down (2026-10-01). A few rows exercise the same code path.
CATALOG_LIMIT = 5


def test_catalog(conn):
    cat = conn.get_catalog(limit=CATALOG_LIMIT)
    ds0 = cat.ids[0]
    assert ds0 in str(cat)
    assert isinstance(cat[ds0], Datasource)
    assert len(cat)


@pytest.mark.asyncio
async def test_catalog_async(conn):
    cat = await conn.get_catalog_async(limit=CATALOG_LIMIT)
    ds0 = cat.ids[0]
    assert ds0 in str(cat)
    assert isinstance(cat[ds0], Datasource)
    assert len(cat)


def _test_command_line_interface():
    """Test the CLI."""
    runner = CliRunner()
    result = runner.invoke(cli.main)
    assert result.exit_code == 0
    assert "Oceanum.io CLI" in result.output
    help_result = runner.invoke(cli.main, ["--help"])
    assert help_result.exit_code == 0
    assert "--help  Show this message and exit." in help_result.output
