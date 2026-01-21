import os
import time
import tempfile
import hashlib
import threading
import xarray as xr
import pandas as pd
import geopandas as gpd

from .query import Query

CACHE_DIR = os.path.join(tempfile.gettempdir(), "oceanum-io-cache")
LOCK_TIMEOUT = 60


class CacheError(Exception):
    pass


class LocalCache:
    _thread_locks = {}
    _thread_locks_lock = threading.Lock()

    def __init__(self, cache_timeout=600, cache_dir=CACHE_DIR, lock_timeout=60):
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir, exist_ok=True)
        self.cache_dir = cache_dir
        self.cache_timeout = cache_timeout
        self.lock_timeout = lock_timeout

    def _get_thread_lock(self, query):
        """Get or create a thread lock for a specific query."""
        cache_key = self._cachepath(query)
        with LocalCache._thread_locks_lock:
            if cache_key not in LocalCache._thread_locks:
                LocalCache._thread_locks[cache_key] = threading.RLock()
            return LocalCache._thread_locks[cache_key]

    def _cachepath(self, query):
        if not isinstance(query, Query):
            query = Query(**query)
        return os.path.join(
            self.cache_dir,
            hashlib.sha224(query.model_dump_json(warnings=False).encode()).hexdigest(),
        )

    def _locked(self, query):
        lockfile = self._cachepath(query) + ".lock"
        return os.path.exists(lockfile) and (
            os.path.getmtime(lockfile) + self.lock_timeout > time.time()
        )

    def lock(self, query):
        lockfile = self._cachepath(query) + ".lock"
        if not os.path.exists(lockfile):
            with open(lockfile, "w") as f:
                f.write("")

    def unlock(self, query):
        if self._locked(query):
            os.remove(self._cachepath(query) + ".lock")

    def _get(self, query):
        cache_file = self._cachepath(query)
        try:
            if os.path.exists(cache_file + ".nc"):
                if (
                    os.path.getmtime(cache_file + ".nc") + self.cache_timeout
                    < time.time()
                ):
                    os.remove(cache_file + ".nc")
                    return None
                return xr.open_dataset(cache_file + ".nc")
            elif os.path.exists(cache_file + ".gpq"):
                if (
                    os.path.getmtime(cache_file + ".gpq") + self.cache_timeout
                    < time.time()
                ):
                    os.remove(cache_file + ".gpq")
                    return None
                return gpd.read_parquet(cache_file + ".gpq")
            elif os.path.exists(cache_file + ".pq"):
                if (
                    os.path.getmtime(cache_file + ".pq") + self.cache_timeout
                    < time.time()
                ):
                    os.remove(cache_file + ".pq")
                    return None
                return pd.read_parquet(cache_file + ".pq")
        except:
            return None

    def get(self, query, timeout=None):
        if timeout is None:
            timeout = self.lock_timeout
        thread_lock = self._get_thread_lock(query)
        acquired = thread_lock.acquire(timeout=timeout)
        if not acquired:
            return None
        try:
            if self._locked(query):
                start_time = time.time()
                while self._locked(query):
                    elapsed = time.time() - start_time
                    if elapsed >= timeout:
                        return None
                    time.sleep(0.1)
            return self._get(query)
        finally:
            thread_lock.release()

    def copy(self, query, fname, ext):
        os.rename(fname, self._cachepath(query) + ext)

    def put(self, query, data):
        thread_lock = self._get_thread_lock(query)
        with thread_lock:
            cache_file = self._cachepath(query)
            if isinstance(data, xr.Dataset):
                data.to_netcdf(cache_file + ".nc")
            elif isinstance(data, gpd.GeoDataFrame):
                data.to_parquet(cache_file + ".gpq")
            elif isinstance(data, pd.DataFrame):
                data.to_parquet(cache_file + ".pq")
            else:
                raise TypeError("Unsupported data type")
