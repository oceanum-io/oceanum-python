import pytest
from uuid import uuid4
import pandas as pd
from pathlib import Path
from click.testing import CliRunner

from oceanum import cli


@pytest.fixture(scope="module")
def runner():
    instance = CliRunner()
    yield instance


# =====================================================================================
# oceanum storage ls
# =====================================================================================
def test_main(runner):
    result = runner.invoke(cli.main)
    assert result.exit_code == 0
    assert "main" in result.output


def test_storage(runner):
    result = runner.invoke(cli.storage, ["-s", "https://storage.oceanum.io", "--help"])
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


# =====================================================================================
# oceanum storage get
# =====================================================================================
def test_storage_get_not_found_raises(runner, tmp_path):
    source = str(uuid4())
    dest = str(tmp_path / source)
    result = runner.invoke(cli.storage, ["get", source, dest])
    assert isinstance(result.exception, FileNotFoundError)


def test_storage_get_file_into_file_dest(runner, tmp_path):
    source = "test_object"
    dest = tmp_path / source
    result = runner.invoke(cli.storage, ["get", source, str(dest)])
    assert result.exit_code == 0
    assert dest.is_file()


def test_storage_get_file_into_existing_folder(runner, tmp_path):
    source = "test_object"
    result = runner.invoke(cli.storage, ["get", source, str(tmp_path)])
    assert result.exit_code == 0
    assert (tmp_path / source).is_file()


def test_storage_get_file_into_nonexisting_folder_fails(runner, tmp_path):
    source = "test_object"
    dest = str(tmp_path / "nonexisting_folder") + "/"
    result = runner.invoke(cli.storage, ["get", source, dest])
    assert isinstance(result.exception, FileNotFoundError)


def test_storage_get_file_into_file_within_nonexisting_folder_works(runner, tmp_path):
    source = "test_object"
    dest = tmp_path / "nonexisting_folder" / source
    result = runner.invoke(cli.storage, ["get", source, str(dest)])
    assert result.exit_code == 0
    assert dest.is_file()


def test_storage_get_file_with_recursive_fails(runner, tmp_path):
    source = "test_object"
    dest = tmp_path / source
    result = runner.invoke(cli.storage, ["get", "-r", source, str(dest)])
    assert isinstance(result.exception, NotADirectoryError)


def test_storage_get_folder_without_recursive_fails(runner, tmp_path):
    source = "test_folder"
    dest = tmp_path / source
    result = runner.invoke(cli.storage, ["get", source, str(dest)])
    assert isinstance(result.exception, IsADirectoryError)


def test_storage_get_folder_into_full_folder_dest(runner, tmp_path):
    source = "test_folder"
    dest = tmp_path / source
    result = runner.invoke(cli.storage, ["get", "-r", source, str(dest)])
    assert result.exit_code == 0
    assert dest.is_dir()


def test_storage_get_folder_existing_root_folder_dest(runner, tmp_path):
    source = "test_folder"
    dest = tmp_path
    result = runner.invoke(cli.storage, ["get", "-r", source, str(dest)])
    assert result.exit_code == 0
    assert (dest / source).is_dir()


# =====================================================================================
# oceanum storage put
# =====================================================================================
# TODO: Ensure objects created here are deleted

def test_storage_put_not_found_raises(runner):
    source = str(uuid4())
    result = runner.invoke(cli.storage, ["put", source, "/test_upload"])
    assert isinstance(result.exception, FileNotFoundError)


def test_storage_put_file(runner, tmp_path):
    source = tmp_path / "test_file_put"
    source.touch()
    dest = "/test_upload/"
    result = runner.invoke(cli.storage, ["put", str(source), dest])
    assert result.exit_code == 0


def test_storage_get_folder_without_recursive_fails(runner, tmp_path):
    source = tmp_path / "test_dir_put"
    source.mkdir()
    dest = Path("test_upload") / source.name
    result = runner.invoke(cli.storage, ["put", str(source), str(dest)])
    assert isinstance(result.exception, IsADirectoryError)


def test_storage_put_folder_into_existing_file_fails(runner, tmp_path):
    dest = "/test_upload/test_file_put"
    source = tmp_path / "test_file_put"
    source.touch()
    result = runner.invoke(cli.storage, ["put", str(source), dest])
    source = tmp_path / "test_dir_put"
    source.mkdir()
    result = runner.invoke(cli.storage, ["put", "-r", str(source), dest])
    assert isinstance(result.exception, FileExistsError)


# def test_datamesh(runner):
#     result = runner.invoke(cli.datamesh)
#     assert result.exit_code == 0
#     assert "Oceanum datamesh commands" in result.output
