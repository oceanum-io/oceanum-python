#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `oceanum` package."""
import os
import pytest

from click.testing import CliRunner

from oceanum.datamesh import Connector, Datasource
from oceanum import cli


@pytest.fixture
def conn():
    """Connection fixture"""
    return Connector(os.environ["DATAMESH_TOKEN"])


def test_catalog(conn):
    cat = conn.get_catalog()
    ds0 = cat.ids[0]
    assert ds0 in str(cat)
    assert isinstance(cat[ds0], Datasource)
    assert len(cat)


@pytest.mark.asyncio
async def test_catalog(conn):
    cat = await conn.get_catalog_async()
    ds0 = cat.ids[0]
    assert ds0 in str(cat)
    assert isinstance(cat[ds0], Datasource)
    assert len(cat)


def test_command_line_interface():
    """Test the CLI."""
    runner = CliRunner()
    result = runner.invoke(cli.main)
    assert result.exit_code == 0
    assert "Oceanum.io CLI" in result.output
    help_result = runner.invoke(cli.main, ["--help"])
    assert help_result.exit_code == 0
    assert "--help  Show this message and exit." in help_result.output
