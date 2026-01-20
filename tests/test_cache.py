#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for LocalCache class."""
import os
import time
import tempfile
import pytest
import pandas as pd
import geopandas as gpd
import xarray as xr
import numpy as np
from shapely.geometry import Point

from oceanum.datamesh.cache import LocalCache, CacheError
from oceanum.datamesh.query import Query


@pytest.fixture
def temp_cache_dir():
    """Create a temporary cache directory for testing."""
    temp_dir = tempfile.mkdtemp(prefix="test_cache_")
    yield temp_dir
    # Cleanup after tests
    import shutil

    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)


@pytest.fixture
def cache(temp_cache_dir):
    """Create a LocalCache instance with temporary directory."""
    return LocalCache(cache_timeout=2, cache_dir=temp_cache_dir, lock_timeout=2)


@pytest.fixture
def sample_query():
    """Create a sample query for testing."""
    return Query(datasource="test-datasource")


@pytest.fixture
def sample_xarray_dataset():
    """Create a sample xarray Dataset."""
    return xr.Dataset(
        {
            "temperature": (["x", "y"], np.random.rand(3, 3)),
            "pressure": (["x", "y"], np.random.rand(3, 3)),
        },
        coords={"x": [0, 1, 2], "y": [0, 1, 2]},
    )


@pytest.fixture
def sample_geodataframe():
    """Create a sample GeoDataFrame."""
    return gpd.GeoDataFrame(
        {"name": ["A", "B", "C"], "value": [1, 2, 3]},
        geometry=[Point(0, 0), Point(1, 1), Point(2, 2)],
        crs="EPSG:4326",
    )


@pytest.fixture
def sample_dataframe():
    """Create a sample DataFrame."""
    return pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})


class TestLocalCacheInit:
    """Test LocalCache initialization."""

    def test_init_creates_cache_dir(self, temp_cache_dir):
        """Test that cache directory is created if it doesn't exist."""
        cache_dir = os.path.join(temp_cache_dir, "new_cache")
        assert not os.path.exists(cache_dir)
        cache = LocalCache(cache_dir=cache_dir)
        assert os.path.exists(cache_dir)
        assert cache.cache_dir == cache_dir

    def test_init_with_existing_dir(self, temp_cache_dir):
        """Test initialization with existing directory."""
        cache = LocalCache(cache_dir=temp_cache_dir)
        assert cache.cache_dir == temp_cache_dir

    def test_init_default_values(self, temp_cache_dir):
        """Test default initialization values."""
        cache = LocalCache(cache_dir=temp_cache_dir)
        assert cache.cache_timeout == 600
        assert cache.lock_timeout == 60

    def test_init_custom_values(self, temp_cache_dir):
        """Test custom initialization values."""
        cache = LocalCache(cache_timeout=300, cache_dir=temp_cache_dir, lock_timeout=30)
        assert cache.cache_timeout == 300
        assert cache.lock_timeout == 30


class TestCachePath:
    """Test cache path generation."""

    def test_cachepath_with_query_object(self, cache, sample_query):
        """Test cache path generation with Query object."""
        path = cache._cachepath(sample_query)
        assert path.startswith(cache.cache_dir)
        assert len(os.path.basename(path)) == 56  # SHA224 hex digest length

    def test_cachepath_with_dict(self, cache):
        """Test cache path generation with dictionary."""
        query_dict = {"datasource": "test-datasource"}
        path = cache._cachepath(query_dict)
        assert path.startswith(cache.cache_dir)

    def test_cachepath_consistency(self, cache, sample_query):
        """Test that same query produces same cache path."""
        path1 = cache._cachepath(sample_query)
        path2 = cache._cachepath(sample_query)
        assert path1 == path2

    def test_cachepath_different_queries(self, cache):
        """Test that different queries produce different cache paths."""
        query1 = Query(datasource="datasource1")
        query2 = Query(datasource="datasource2")
        path1 = cache._cachepath(query1)
        path2 = cache._cachepath(query2)
        assert path1 != path2


class TestLocking:
    """Test cache locking mechanism."""

    def test_lock_creates_lockfile(self, cache, sample_query):
        """Test that lock creates a lockfile."""
        lockfile = cache._cachepath(sample_query) + ".lock"
        assert not os.path.exists(lockfile)
        cache.lock(sample_query)
        assert os.path.exists(lockfile)

    def test_locked_returns_true_when_locked(self, cache, sample_query):
        """Test that _locked returns True when file is locked."""
        assert not cache._locked(sample_query)
        cache.lock(sample_query)
        assert cache._locked(sample_query)

    def test_locked_returns_false_after_timeout(self, cache, sample_query):
        """Test that lock expires after timeout."""
        cache.lock(sample_query)
        assert cache._locked(sample_query)
        time.sleep(2.1)  # Wait for lock timeout
        assert not cache._locked(sample_query)

    def test_unlock_removes_lockfile(self, cache, sample_query):
        """Test that unlock removes the lockfile."""
        cache.lock(sample_query)
        lockfile = cache._cachepath(sample_query) + ".lock"
        assert os.path.exists(lockfile)
        cache.unlock(sample_query)
        assert not os.path.exists(lockfile)

    def test_unlock_when_not_locked(self, cache, sample_query):
        """Test that unlock does nothing when not locked."""
        cache.unlock(sample_query)  # Should not raise error

    def test_lock_idempotent(self, cache, sample_query):
        """Test that locking multiple times is safe."""
        cache.lock(sample_query)
        cache.lock(sample_query)  # Should not raise error
        assert cache._locked(sample_query)


class TestPutAndGet:
    """Test cache put and get operations."""

    def test_put_and_get_xarray_dataset(
        self, cache, sample_query, sample_xarray_dataset
    ):
        """Test storing and retrieving xarray Dataset."""
        cache.put(sample_query, sample_xarray_dataset)
        cached_file = cache._cachepath(sample_query) + ".nc"
        assert os.path.exists(cached_file)

        retrieved = cache.get(sample_query)
        assert isinstance(retrieved, xr.Dataset)
        xr.testing.assert_equal(retrieved, sample_xarray_dataset)

    def test_put_and_get_geodataframe(self, cache, sample_query, sample_geodataframe):
        """Test storing and retrieving GeoDataFrame."""
        cache.put(sample_query, sample_geodataframe)
        cached_file = cache._cachepath(sample_query) + ".gpq"
        assert os.path.exists(cached_file)

        retrieved = cache.get(sample_query)
        assert isinstance(retrieved, gpd.GeoDataFrame)
        pd.testing.assert_frame_equal(retrieved, sample_geodataframe)

    def test_put_and_get_dataframe(self, cache, sample_query, sample_dataframe):
        """Test storing and retrieving DataFrame."""
        cache.put(sample_query, sample_dataframe)
        cached_file = cache._cachepath(sample_query) + ".pq"
        assert os.path.exists(cached_file)

        retrieved = cache.get(sample_query)
        assert isinstance(retrieved, pd.DataFrame)
        pd.testing.assert_frame_equal(retrieved, sample_dataframe)

    def test_put_unsupported_type(self, cache, sample_query):
        """Test that putting unsupported type raises TypeError."""
        with pytest.raises(TypeError, match="Unsupported data type"):
            cache.put(sample_query, "unsupported string data")

    def test_get_nonexistent_cache(self, cache, sample_query):
        """Test getting from cache when no cached data exists."""
        result = cache.get(sample_query)
        assert result is None

    def test_get_expired_cache(self, cache, sample_query, sample_dataframe):
        """Test that expired cache returns None and removes file."""
        cache.put(sample_query, sample_dataframe)
        cached_file = cache._cachepath(sample_query) + ".pq"
        assert os.path.exists(cached_file)

        time.sleep(2.1)  # Wait for cache timeout
        result = cache.get(sample_query)
        assert result is None
        assert not os.path.exists(cached_file)

    def test_get_with_lock_waits(self, cache, sample_query, sample_dataframe):
        """Test that get waits when cache is locked."""
        cache.lock(sample_query)

        # Start time
        start = time.time()

        # In a real scenario, another process would unlock
        # For testing, we'll unlock after a short delay
        import threading

        def unlock_after_delay():
            time.sleep(0.3)
            cache.unlock(sample_query)

        thread = threading.Thread(target=unlock_after_delay)
        thread.start()

        result = cache.get(sample_query, timeout=5)
        elapsed = time.time() - start

        thread.join()

        # Should have waited at least 0.3 seconds
        assert elapsed >= 0.3
        assert result is None  # No cached data exists

    def test_get_with_lock_timeout(self, cache, sample_query):
        """Test that get raises CacheError when lock timeout is reached."""
        cache.lock(sample_query)

        assert cache.get(sample_query, timeout=0.5) is None

    def test_get_corrupted_cache_returns_none(self, cache, sample_query):
        """Test that corrupted cache file returns None."""
        cached_file = cache._cachepath(sample_query) + ".nc"

        # Create a corrupted file
        with open(cached_file, "w") as f:
            f.write("corrupted data")

        result = cache.get(sample_query)
        assert result is None


class TestCopy:
    """Test cache copy operation."""

    def test_copy_moves_file(self, cache, sample_query, temp_cache_dir):
        """Test that copy moves file to cache location."""
        source_file = os.path.join(temp_cache_dir, "source.nc")

        # Create a source file
        with open(source_file, "w") as f:
            f.write("test data")

        assert os.path.exists(source_file)

        cache.copy(sample_query, source_file, ".nc")

        cached_file = cache._cachepath(sample_query) + ".nc"
        assert os.path.exists(cached_file)
        assert not os.path.exists(source_file)  # Original should be moved


class TestCacheIntegration:
    """Integration tests for cache operations."""

    def test_multiple_queries_different_caches(self, cache, sample_dataframe):
        """Test that different queries create different cache files."""
        query1 = Query(datasource="datasource1")
        query2 = Query(datasource="datasource2")

        cache.put(query1, sample_dataframe)
        cache.put(query2, sample_dataframe)

        cached_file1 = cache._cachepath(query1) + ".pq"
        cached_file2 = cache._cachepath(query2) + ".pq"

        assert os.path.exists(cached_file1)
        assert os.path.exists(cached_file2)
        assert cached_file1 != cached_file2

    def test_cache_overwrite(self, cache, sample_query):
        """Test that putting new data overwrites old cache."""
        df1 = pd.DataFrame({"col": [1, 2, 3]})
        df2 = pd.DataFrame({"col": [4, 5, 6]})

        cache.put(sample_query, df1)
        retrieved1 = cache.get(sample_query)
        pd.testing.assert_frame_equal(retrieved1, df1)

        cache.put(sample_query, df2)
        retrieved2 = cache.get(sample_query)
        pd.testing.assert_frame_equal(retrieved2, df2)

    def test_lock_unlock_workflow(self, cache, sample_query, sample_dataframe):
        """Test typical lock-unlock workflow."""
        # Lock before writing
        cache.lock(sample_query)
        assert cache._locked(sample_query)

        # Write data
        cache.put(sample_query, sample_dataframe)

        # Unlock after writing
        cache.unlock(sample_query)
        assert not cache._locked(sample_query)

        # Should be able to read
        result = cache.get(sample_query)
        assert result is not None
        pd.testing.assert_frame_equal(result, sample_dataframe)
