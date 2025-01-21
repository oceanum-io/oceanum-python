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
    fs.mkdirs(REMOTE_PATH, exist_ok=True)
    fs.put(os.path.join(dummy_files.name, "test"), REMOTE_PATH, recursive=True)

    files = fs.ls(REMOTE_PATH)
    assert len(files) == 1
    assert files[0]["type"] == "directory"

    # As the bucket tends to contain many things
    # just make sure that the expected folder contains
    # the expected file. Shows ls gives us those files
    found = False
    for p in _fs.walk(REMOTE_PATH):
        if p[0] == 'test_storage/test':
            found = ("file1.txt" in p[-1]) and ("file2.txt" in p[-1])
            break
    assert found

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
