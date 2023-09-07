import pytest
import pandas as pd
from click.testing import CliRunner

from oceanum import cli


@pytest.fixture(scope="module")
def runner():
    instance = CliRunner()
    yield instance


def test_main(runner):
    result = runner.invoke(cli.main)
    assert result.exit_code == 0
    assert "main" in result.output


def test_storage(runner):
    result = runner.invoke(cli.storage)
    assert result.exit_code == 0
    assert "Oceanum storage commands" in result.output


def test_storage_ls(runner):
    result = runner.invoke(cli.storage, ["ls", "file_test_dir"])
    assert result.exit_code == 0
    assert "file_test_dir/test_file" in result.output


def test_storage_ls_long(runner):
    result = runner.invoke(cli.storage, ["ls", "-l", "file_test_dir"])
    assert result.exit_code == 0
    assert pd.to_datetime(result.output.split()[1])
    assert "TOTAL" in result.output


def test_storage_ls_human_readable(runner):
    result = runner.invoke(cli.storage, ["ls", "-l", "file_test_dir"])
    size_bytes = result.output.split()[0]
    result = runner.invoke(cli.storage, ["ls", "-lh", "file_test_dir"])
    size_human = " ".join(result.output.split()[0:2])
    size_human == cli.bytes_to_human(float(size_bytes))


def test_storage_ls_recursive(runner):
    # TODO: Create netsted test files so this option can be tested
    result = runner.invoke(cli.storage, ["ls", "-r"])
    assert result.exit_code == 0


def test_datamesh(runner):
    result = runner.invoke(cli.datamesh)
    assert result.exit_code == 0
    assert "Oceanum datamesh commands" in result.output
