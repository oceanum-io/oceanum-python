import os
import time
import tempfile
import hashlib
import xarray as xr
import pandas as pd
import geopandas as gpd

from .query import Query

CACHE_DIR = os.path.join(tempfile.gettempdir(), "oceanum-io-cache")


class CacheError(Exception):
    pass


class LocalCache:
    def __init__(self, cache_timeout=600, cache_dir=CACHE_DIR, lock_timeout=60):
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir, exist_ok=True)
        self.cache_dir = cache_dir
        self.cache_timeout = cache_timeout
        self.lock_timeout = lock_timeout

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

    def get(self, query, timeout=120):
        if self._locked(query):
            if timeout <= 0:
                raise CacheError("Cache lock timeout")
            time.sleep(0.1)
            return self.get(query, timeout=timeout - 0.1)
        item = self._get(query)
        return item

    def copy(self, query, fname, ext):
        os.rename(fname, self._cachepath(query) + ext)

    def put(self, query, data):
        cache_file = self._cachepath(query)
        if isinstance(data, xr.Dataset):
            data.to_netcdf(cache_file + ".nc")
        elif isinstance(data, gpd.GeoDataFrame):
            data.to_parquet(cache_file + ".gpq")
        elif isinstance(data, pd.DataFrame):
            data.to_parquet(cache_file + ".pq")
        else:
            raise TypeError("Unsupported data type")
