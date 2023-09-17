import os
import time
import tempfile
import hashlib
import xarray as xr
import pandas as pd
import geopandas as gpd

from .query import Query

CACHE_DIR = os.path.join(tempfile.gettempdir(), "oceanum-io-cache")


class LocalCache:
    def __init__(self, cache_timeout=600, cache_dir=CACHE_DIR):
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir, exist_ok=True)
        self.cache_dir = cache_dir
        self.cache_timeout = cache_timeout

    def _cachepath(self, query):
        if not isinstance(query, Query):
            query = Query(**query)
        return os.path.join(
            self.cache_dir,
            hashlib.sha224(query.model_dump_json(warnings=False).encode()).hexdigest(),
        )

    def get(self, query):
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
