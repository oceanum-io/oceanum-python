import os
import pytest
import dotenv


dotenv.load_dotenv()

# Fallback gate for tests that use a token fixture but are not marked
TOKEN_FIXTURE_NAMES = {"conn", "fs", "fs_async"}


def pytest_configure(config):
    token = os.getenv("OCEANUM_TEST_DATAMESH_TOKEN")
    if token:
        os.environ["DATAMESH_TOKEN"] = token
    elif os.getenv("CI"):
        # Skipping is for local runs; in CI a missing secret must not pass as green
        raise pytest.UsageError(
            "OCEANUM_TEST_DATAMESH_TOKEN is not set. It is required when CI is set."
        )


def pytest_collection_modifyitems(config, items):
    if os.getenv("OCEANUM_TEST_DATAMESH_TOKEN"):
        return
    skip_marker = pytest.mark.skip(reason="OCEANUM_TEST_DATAMESH_TOKEN not set; skipping token-gated tests")
    for item in items:
        if item.get_closest_marker("requires_datamesh_token") or TOKEN_FIXTURE_NAMES.intersection(item.fixturenames):
            item.add_marker(skip_marker)
