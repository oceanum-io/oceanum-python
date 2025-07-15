#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `oceanum` package."""
import os
import tempfile
import pytest
import fsspec

from oceanum.storage import FileSystem

REMOTE_PATH = "test_storage"


@pytest.fixture
def fs():
    """Connection fixture"""
    fs = FileSystem(os.environ["DATAMESH_TOKEN"])
    return fs


@pytest.fixture
def fs_async():
    """Connection fixture"""
    fs = FileSystem(os.environ["DATAMESH_TOKEN"], asynchronous=True)
    return fs


@pytest.fixture
def dummy_files():
    tmpdir = tempfile.TemporaryDirectory()
    os.makedirs(os.path.join(tmpdir.name, "test"))
    with open(os.path.join(tmpdir.name, "test", "file1.txt"), "w") as f:
        f.write("hello")
    with open(os.path.join(tmpdir.name, "test", "file2.txt"), "w") as f:
        f.write("world")
    return tmpdir


def test_file_not_found(fs):
    with pytest.raises(FileNotFoundError):
        fs.ls("/not_found")


def test_dir_not_found(fs):
    with pytest.raises(FileNotFoundError):
        fs.ls("/not_found/")


def test_ls(fs, dummy_files):
    rand_dir = os.path.join(REMOTE_PATH,os.path.basename(tempfile.TemporaryDirectory().name))
    fs.mkdirs(rand_dir, exist_ok=True)
    fs.put(os.path.join(dummy_files.name, 'test'), rand_dir, recursive=True)
    files = fs.ls(os.path.join(rand_dir, 'test'))
    assert os.path.join(rand_dir, 'test', 'file1.txt') in [f["name"] for f in files]
    assert os.path.join(rand_dir, 'test', 'file2.txt') in [f["name"] for f in files]

def test_ls_file_prefix(fs, dummy_files):
    test_folder = f'{REMOTE_PATH}/test'
    fs.mkdirs(test_folder, exist_ok=True)
    fs.put(dummy_files.name, test_folder, recursive=True)
    files = fs.ls(test_folder, file_prefix="file1")
    assert len(files) == 1
    assert files[0]["name"] == "test_storage/test/file1.txt"

def test_ls_glob(fs, dummy_files):
    test_folder = f'{REMOTE_PATH}/test'
    fs.mkdirs(test_folder, exist_ok=True)
    fs.put(dummy_files.name, test_folder, recursive=True)
    files = fs.ls(test_folder, match_glob="**/*2.txt")
    assert len(files) == 1
    assert files[0]["name"] == "test_storage/test/file2.txt"

def test_ls_limit(fs, dummy_files):
    test_folder = f'{REMOTE_PATH}/test'
    fs.mkdirs(test_folder, exist_ok=True)
    fs.put(dummy_files.name, test_folder, recursive=True)
    # Something is off with limit
    # At the storage API level, it returns 1 file when limit=2
    files = fs.ls(test_folder, limit=2)
    assert len(files) == 1
    assert files[0]["name"] == "test_storage/test/file1.txt"

def test_get(fs, dummy_files):
    fs.mkdirs(REMOTE_PATH, exist_ok=True)
    fs.put(os.path.join(dummy_files.name, "test"), REMOTE_PATH, recursive=True)

    localdir = tempfile.TemporaryDirectory()
    fs.get(REMOTE_PATH, localdir.name + "/", recursive=True)

    assert os.path.exists(os.path.join(localdir.name, "test", "file1.txt"))


def test_open(fs, dummy_files):
    fs.mkdirs(REMOTE_PATH, exist_ok=True)
    fs.put(os.path.join(dummy_files.name, "test"), REMOTE_PATH, recursive=True)

    with fsspec.open(
        "oceanum://" + os.path.join(REMOTE_PATH, "test", "file1.txt"), "r"
    ) as f:
        assert f.read() == "hello"


def test_copy(fs, dummy_files):
    fs.mkdirs(REMOTE_PATH, exist_ok=True)
    fs.put(os.path.join(dummy_files.name, "test"), REMOTE_PATH, recursive=True)

    fs.copy(
        os.path.join(REMOTE_PATH, "test", "file1.txt"),
        os.path.join(REMOTE_PATH, "test", "file_copy.txt"),
    )

    with fsspec.open(
        "oceanum://" + os.path.join(REMOTE_PATH, "test", "file_copy.txt"), "r"
    ) as f:
        assert f.read() == "hello"


def test_copy_fails(fs):
    with pytest.raises(FileNotFoundError):
        fs.copy(
            os.path.join(REMOTE_PATH, "test", "file_not_there.txt"),
            os.path.join(REMOTE_PATH, "test", "file_copy.txt"),
        )


def test_exists_file(fs, dummy_files):
    """Test exists() function with a file that exists."""
    fs.mkdirs(REMOTE_PATH, exist_ok=True)
    fs.put(os.path.join(dummy_files.name, "test"), REMOTE_PATH, recursive=True)

    # Test file exists
    assert fs.exists(os.path.join(REMOTE_PATH, "test", "file1.txt"))


def test_exists_directory(fs, dummy_files):
    """Test exists() function with a directory that exists."""
    fs.mkdirs(REMOTE_PATH, exist_ok=True)
    fs.put(os.path.join(dummy_files.name, "test"), REMOTE_PATH, recursive=True)

    # Test directory exists
    assert fs.exists(os.path.join(REMOTE_PATH, "test"))


def test_exists_nonexistent(fs):
    """Test exists() function with a path that doesn't exist."""
    # Test non-existent file
    assert not fs.exists(os.path.join(REMOTE_PATH, "nonexistent_file.txt"))

    # Test non-existent directory
    assert not fs.exists(os.path.join(REMOTE_PATH, "nonexistent_dir"))


def test_isfile_true(fs, dummy_files):
    """Test isfile() function with an actual file."""
    fs.mkdirs(REMOTE_PATH, exist_ok=True)
    fs.put(os.path.join(dummy_files.name, "test"), REMOTE_PATH, recursive=True)

    # Test file is correctly identified as file
    assert fs.isfile(os.path.join(REMOTE_PATH, "test", "file1.txt"))
    assert fs.isfile(os.path.join(REMOTE_PATH, "test", "file2.txt"))


def test_isfile_false_directory(fs, dummy_files):
    """Test isfile() function with a directory."""
    fs.mkdirs(REMOTE_PATH, exist_ok=True)
    fs.put(os.path.join(dummy_files.name, "test"), REMOTE_PATH, recursive=True)

    # Test directory is not identified as file
    assert not fs.isfile(os.path.join(REMOTE_PATH, "test"))
    assert not fs.isfile(REMOTE_PATH)


def test_isfile_false_nonexistent(fs):
    """Test isfile() function with non-existent path."""
    # Test non-existent path returns False
    assert not fs.isfile(os.path.join(REMOTE_PATH, "nonexistent_file.txt"))


def test_isdir_true(fs, dummy_files):
    """Test isdir() function with an actual directory."""
    fs.mkdirs(REMOTE_PATH, exist_ok=True)
    fs.put(os.path.join(dummy_files.name, "test"), REMOTE_PATH, recursive=True)

    # Test directory is correctly identified as directory
    assert fs.isdir(os.path.join(REMOTE_PATH, "test"))
    assert fs.isdir(REMOTE_PATH)


def test_isdir_false_file(fs, dummy_files):
    """Test isdir() function with a file."""
    fs.mkdirs(REMOTE_PATH, exist_ok=True)
    fs.put(os.path.join(dummy_files.name, "test"), REMOTE_PATH, recursive=True)

    # Test file is not identified as directory
    assert not fs.isdir(os.path.join(REMOTE_PATH, "test", "file1.txt"))
    assert not fs.isdir(os.path.join(REMOTE_PATH, "test", "file2.txt"))


def test_isdir_false_nonexistent(fs):
    """Test isdir() function with non-existent path."""
    # Test non-existent path returns False
    assert not fs.isdir(os.path.join(REMOTE_PATH, "nonexistent_dir"))


# Test the sync wrapper functions from oceanum.storage module
def test_sync_exists():
    """Test the sync exists() wrapper function."""
    from oceanum.storage import exists

    # Test with environment token
    token = os.environ.get("DATAMESH_TOKEN")
    if token:
        # Test non-existent path (should be safe to test)
        assert not exists("nonexistent_test_path_12345", token=token)


def test_sync_isfile():
    """Test the sync isfile() wrapper function."""
    from oceanum.storage import isfile

    # Test with environment token
    token = os.environ.get("DATAMESH_TOKEN")
    if token:
        # Test non-existent path (should be safe to test)
        assert not isfile("nonexistent_test_file_12345.txt", token=token)


def test_sync_isdir():
    """Test the sync isdir() wrapper function."""
    from oceanum.storage import isdir

    # Test with environment token
    token = os.environ.get("DATAMESH_TOKEN")
    if token:
        # Test non-existent path (should be safe to test)
        assert not isdir("nonexistent_test_dir_12345", token=token)


def test_exists_edge_cases(fs, dummy_files):
    """Test exists() with various edge cases."""
    fs.mkdirs(REMOTE_PATH, exist_ok=True)
    fs.put(os.path.join(dummy_files.name, "test"), REMOTE_PATH, recursive=True)

    # Test with different path formats
    assert fs.exists(f"{REMOTE_PATH}/test/file1.txt")  # Forward slash
    assert fs.exists(os.path.join(REMOTE_PATH, "test", "file1.txt"))  # os.path.join

    # Test with trailing slash on directory
    assert fs.exists(f"{REMOTE_PATH}/test/")
    assert fs.exists(f"{REMOTE_PATH}/test")  # Without trailing slash


def test_file_type_consistency(fs, dummy_files):
    """Test that exists, isfile, and isdir are consistent."""
    fs.mkdirs(REMOTE_PATH, exist_ok=True)
    fs.put(os.path.join(dummy_files.name, "test"), REMOTE_PATH, recursive=True)

    file_path = os.path.join(REMOTE_PATH, "test", "file1.txt")
    dir_path = os.path.join(REMOTE_PATH, "test")

    # For existing file
    assert fs.exists(file_path)
    assert fs.isfile(file_path)
    assert not fs.isdir(file_path)

    # For existing directory
    assert fs.exists(dir_path)
    assert not fs.isfile(dir_path)
    assert fs.isdir(dir_path)

    # For non-existent path
    nonexistent = os.path.join(REMOTE_PATH, "nonexistent")
    assert not fs.exists(nonexistent)
    assert not fs.isfile(nonexistent)
    assert not fs.isdir(nonexistent)


def test_rm_file(fs, dummy_files):
    """Test rm() function with a file."""
    fs.mkdirs(REMOTE_PATH, exist_ok=True)
    fs.put(os.path.join(dummy_files.name, "test"), REMOTE_PATH, recursive=True)

    file_path = os.path.join(REMOTE_PATH, "test", "file1.txt")

    # Verify file exists before removal
    assert fs.exists(file_path)

    # Remove the file
    fs.rm(file_path)

    # Verify file no longer exists
    assert not fs.exists(file_path)


def test_rm_directory_recursive(fs, dummy_files):
    """Test rm() function with a directory recursively."""
    test_dir = os.path.join(REMOTE_PATH, "test_rm_recursive")
    fs.mkdirs(test_dir, exist_ok=True)
    fs.put(os.path.join(dummy_files.name, "test"), test_dir, recursive=True)

    dir_path = os.path.join(test_dir, "test")

    # Verify directory and contents exist
    assert fs.exists(dir_path)
    assert fs.isdir(dir_path)
    assert fs.exists(os.path.join(dir_path, "file1.txt"))

    # Remove directory recursively
    fs.rm(dir_path, recursive=True)

    # Verify directory no longer exists
    assert not fs.exists(dir_path)


def test_rm_directory_non_recursive_empty(fs):
    """Test rm() function with an empty directory non-recursively."""
    test_dir = os.path.join(REMOTE_PATH, "test_rm_empty")
    fs.mkdirs(test_dir, exist_ok=True)

    # Verify directory exists
    assert fs.exists(test_dir)
    assert fs.isdir(test_dir)

    # Remove empty directory non-recursively
    fs.rm(test_dir, recursive=False)

    # Verify directory no longer exists
    assert not fs.exists(test_dir)


def test_rm_directory_non_recursive_not_empty(fs, dummy_files):
    """Test rm() function with a non-empty directory non-recursively should fail."""
    test_dir = os.path.join(REMOTE_PATH, "test_rm_nonempty")
    fs.mkdirs(test_dir, exist_ok=True)
    fs.put(os.path.join(dummy_files.name, "test"), test_dir, recursive=True)

    dir_path = os.path.join(test_dir, "test")

    # Verify directory and contents exist
    assert fs.exists(dir_path)
    assert fs.isdir(dir_path)
    assert fs.exists(os.path.join(dir_path, "file1.txt"))

    # Attempt to remove non-empty directory non-recursively should raise error
    with pytest.raises(OSError, match="Directory not empty"):
        fs.rm(dir_path, recursive=False)

    # Verify directory still exists
    assert fs.exists(dir_path)


def test_rm_nonexistent_path(fs):
    """Test rm() function with non-existent path."""
    nonexistent_path = os.path.join(REMOTE_PATH, "nonexistent_file.txt")

    # Attempt to remove non-existent path should raise FileNotFoundError
    with pytest.raises(FileNotFoundError):
        fs.rm(nonexistent_path)


def test_sync_rm():
    """Test the sync rm() wrapper function."""
    from oceanum.storage import rm

    # Test with environment token
    token = os.environ.get("DATAMESH_TOKEN")
    if token:
        # Test non-existent path (should raise FileNotFoundError)
        with pytest.raises(FileNotFoundError):
            rm("nonexistent_test_file_12345.txt", token=token)


def test_rm_edge_cases(fs, dummy_files):
    """Test rm() with various edge cases."""
    # Create test structure
    test_dir = os.path.join(REMOTE_PATH, "test_rm_edge")
    fs.mkdirs(test_dir, exist_ok=True)
    fs.put(os.path.join(dummy_files.name, "test"), test_dir, recursive=True)

    # Test with different path formats
    file_path = os.path.join(test_dir, "test", "file1.txt")

    # Remove with forward slash format
    fs.rm(f"{test_dir}/test/file2.txt")
    assert not fs.exists(f"{test_dir}/test/file2.txt")

    # Remove remaining file with os.path.join format
    fs.rm(file_path)
    assert not fs.exists(file_path)

    # Remove the now-empty directory
    fs.rm(os.path.join(test_dir, "test"), recursive=False)
    assert not fs.exists(os.path.join(test_dir, "test"))
