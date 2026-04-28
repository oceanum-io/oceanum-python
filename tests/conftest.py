import os
import pytest
import dotenv


dotenv.load_dotenv()

def pytest_configure(config):
    config.addinivalue_line(
        "markers", "requires_datamesh_token: marks tests that need OCEANUM_TEST_DATAMESH_TOKEN"
    )

def pytest_collection_modifyitems(config, items):
    token = os.getenv("OCEANUM_TEST_DATAMESH_TOKEN")
    if token:
        os.environ["DATAMESH_TOKEN"] = token
        return
    skip_marker = pytest.mark.skip(reason="OCEANUM_TEST_DATAMESH_TOKEN not set; skipping token-gated tests")
    for item in items:
        if item.get_closest_marker("requires_datamesh_token"):
            item.add_marker(skip_marker)
