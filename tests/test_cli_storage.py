#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `oceanum storage` CLI commands.

These tests require authentication with the Oceanum service. They will be automatically
skipped in CI/CD environments or when the user is not logged in locally.

To run these tests locally:
1. Ensure you have the DATAMESH_TOKEN environment variable set
2. Run `oceanum auth login` to authenticate
3. Run the tests with `pytest tests/test_cli_storage.py`

In CI/CD environments, these tests will be skipped and only basic help command tests
will run to ensure the CLI is properly installed and accessible.
"""
import os
import tempfile
import pytest
import subprocess

from oceanum.storage import FileSystem

REMOTE_PATH = "test_storage_cli"


def is_ci_environment():
    """Check if we're running in a CI/CD environment."""
    ci_indicators = [
        'CI', 'CONTINUOUS_INTEGRATION', 'GITHUB_ACTIONS', 'GITLAB_CI',
        'JENKINS_URL', 'TRAVIS', 'CIRCLECI', 'BUILDKITE', 'DRONE'
    ]
    return any(os.environ.get(indicator) for indicator in ci_indicators)


def is_logged_in():
    """Check if user is logged in by attempting a simple CLI command."""
    # First check if DATAMESH_TOKEN is available (required for FileSystem)
    if not os.environ.get("DATAMESH_TOKEN"):
        return False

    try:
        result = subprocess.run(
            ["oceanum", "auth", "--help"],
            capture_output=True,
            text=True,
            timeout=10
        )
        # If auth help works, try a simple storage command that requires login
        if result.returncode == 0:
            test_result = subprocess.run(
                ["oceanum", "storage", "exists", "/"],
                capture_output=True,
                text=True,
                timeout=10
            )
            return "You need to login first!" not in test_result.stderr
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pass
    return False


# Skip condition checks
skip_if_no_login = pytest.mark.skipif(
    is_ci_environment() or not is_logged_in(),
    reason="Skipping CLI tests - no login available or running in CI/CD environment"
)


@pytest.fixture
def fs():
    """FileSystem fixture for setup/teardown."""
    if is_ci_environment() or not is_logged_in():
        pytest.skip("Skipping fixture setup - no login available")
    return FileSystem(os.environ["DATAMESH_TOKEN"])


@pytest.fixture
def test_files(fs):
    """Create test files and directories for CLI testing."""
    if is_ci_environment() or not is_logged_in():
        pytest.skip("Skipping fixture setup - no login available")

    tmpdir = tempfile.TemporaryDirectory()

    # Create test file structure
    os.makedirs(os.path.join(tmpdir.name, "test_dir"))
    with open(os.path.join(tmpdir.name, "test_file.txt"), "w") as f:
        f.write("test content")
    with open(os.path.join(tmpdir.name, "test_dir", "nested_file.txt"), "w") as f:
        f.write("nested content")

    # Upload to remote storage
    fs.mkdirs(REMOTE_PATH, exist_ok=True)
    fs.put(os.path.join(tmpdir.name, "test_file.txt"), f"{REMOTE_PATH}/test_file.txt")
    fs.put(os.path.join(tmpdir.name, "test_dir"), REMOTE_PATH, recursive=True)

    yield {
        "file": f"{REMOTE_PATH}/test_file.txt",
        "dir": f"{REMOTE_PATH}/test_dir",
        "nested_file": f"{REMOTE_PATH}/test_dir/nested_file.txt",
        "nonexistent": f"{REMOTE_PATH}/nonexistent_path",
    }

    tmpdir.cleanup()


def run_cli_command(args):
    """Helper function to run CLI commands."""
    cmd = ["oceanum"] + args
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result


@skip_if_no_login
def test_cli_exists_file(test_files):
    """Test CLI exists command with existing file."""
    result = run_cli_command(["storage", "exists", test_files["file"]])
    assert result.returncode == 0
    assert "EXISTS:" in result.stdout
    assert test_files["file"] in result.stdout


@skip_if_no_login
def test_cli_exists_directory(test_files):
    """Test CLI exists command with existing directory."""
    result = run_cli_command(["storage", "exists", test_files["dir"]])
    assert result.returncode == 0
    assert "EXISTS:" in result.stdout
    assert test_files["dir"] in result.stdout


@skip_if_no_login
def test_cli_exists_nonexistent(test_files):
    """Test CLI exists command with non-existent path."""
    result = run_cli_command(["storage", "exists", test_files["nonexistent"]])
    assert result.returncode == 1
    assert "NOT FOUND:" in result.stdout
    assert test_files["nonexistent"] in result.stdout


@skip_if_no_login
def test_cli_isfile_true(test_files):
    """Test CLI isfile command with actual file."""
    result = run_cli_command(["storage", "isfile", test_files["file"]])
    assert result.returncode == 0
    assert "FILE:" in result.stdout
    assert test_files["file"] in result.stdout


@skip_if_no_login
def test_cli_isfile_false_directory(test_files):
    """Test CLI isfile command with directory."""
    result = run_cli_command(["storage", "isfile", test_files["dir"]])
    assert result.returncode == 1
    assert "NOT A FILE:" in result.stdout
    assert test_files["dir"] in result.stdout


@skip_if_no_login
def test_cli_isdir_true(test_files):
    """Test CLI isdir command with actual directory."""
    result = run_cli_command(["storage", "isdir", test_files["dir"]])
    assert result.returncode == 0
    assert "DIRECTORY:" in result.stdout
    assert test_files["dir"] in result.stdout


@skip_if_no_login
def test_cli_isdir_false_file(test_files):
    """Test CLI isdir command with file."""
    result = run_cli_command(["storage", "isdir", test_files["file"]])
    assert result.returncode == 1
    assert "NOT A DIRECTORY:" in result.stdout
    assert test_files["file"] in result.stdout


@skip_if_no_login
def test_cli_rm_file_force(fs):
    """Test CLI rm command with force flag (no confirmation)."""
    # Create a test file to remove
    test_path = f"{REMOTE_PATH}/rm_test_file.txt"
    tmpdir = tempfile.TemporaryDirectory()
    with open(os.path.join(tmpdir.name, "rm_test.txt"), "w") as f:
        f.write("test")
    fs.put(os.path.join(tmpdir.name, "rm_test.txt"), test_path)

    # Verify file exists
    assert fs.exists(test_path)

    # Remove with force flag
    result = run_cli_command(["storage", "rm", "-f", test_path])
    assert result.returncode == 0
    assert "Successfully removed:" in result.stdout
    assert test_path in result.stdout

    # Verify file is gone
    assert not fs.exists(test_path)

    tmpdir.cleanup()


@skip_if_no_login
def test_cli_help_commands():
    """Test that help text is available for all commands."""
    # Test main storage help
    result = run_cli_command(["storage", "--help"])
    assert result.returncode == 0
    assert "exists" in result.stdout
    assert "isfile" in result.stdout
    assert "isdir" in result.stdout
    assert "rm" in result.stdout

    # Test individual command help
    for cmd in ["exists", "isfile", "isdir", "rm"]:
        result = run_cli_command(["storage", cmd, "--help"])
        assert result.returncode == 0
        assert "Usage:" in result.stdout


@skip_if_no_login
def test_cli_rm_help_shows_new_options():
    """Test that rm help shows the new force option."""
    result = run_cli_command(["storage", "rm", "--help"])
    assert result.returncode == 0
    assert "-f, --force" in result.stdout
    assert "Force removal without confirmation" in result.stdout
    assert "-r, --recursive" in result.stdout


def test_cli_main_help():
    """Test that main CLI help works without login."""
    result = run_cli_command(["--help"])
    assert result.returncode == 0
    assert "Usage:" in result.stdout
    assert "oceanum" in result.stdout


def test_cli_auth_help():
    """Test that auth help works without login."""
    result = run_cli_command(["auth", "--help"])
    assert result.returncode == 0
    assert "Usage:" in result.stdout
    assert "login" in result.stdout


def test_cli_storage_help():
    """Test that storage help works without login."""
    result = run_cli_command(["storage", "--help"])
    assert result.returncode == 0
    assert "Usage:" in result.stdout
    # Only test for commands that actually exist
    assert "ls" in result.stdout
    assert "get" in result.stdout
    assert "put" in result.stdout
    assert "rm" in result.stdout
