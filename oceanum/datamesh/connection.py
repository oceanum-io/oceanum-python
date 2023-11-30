import os
import io
import re
import shutil
import json
import datetime
import tempfile
import hashlib
import requests
import fsspec
import xarray
import geopandas
import pandas
import shapely
import warnings
import tempfile
from urllib.parse import urlparse
import asyncio
from functools import wraps, partial
from contextlib import contextmanager

from .datasource import Datasource, _datasource_props, _datasource_driver
from .catalog import Catalog
from .query import Query, Stage, Container, TimeFilter, GeoFilter
from .zarr import zarr_write, ZarrClient
from .cache import LocalCache
from .exceptions import DatameshConnectError, DatameshQueryError, DatameshWriteError

DEFAULT_CONFIG = {"DATAMESH_SERVICE": "https://datamesh.oceanum.io"}

DASK_QUERY_SIZE = 1000000000  # 1GB


def asyncwrapper(func):
    @wraps(func)
    async def run(*args, loop=None, executor=None, **kwargs):
        if loop is None:
            loop = asyncio.get_event_loop()
        pfunc = partial(func, *args, **kwargs)
        return await loop.run_in_executor(executor, pfunc)

    return run


# Windows compatibility tempfile
@contextmanager
def tempFile(mode="wb"):
    file = tempfile.NamedTemporaryFile(mode, delete=False)
    try:
        yield file
    finally:
        file.close()
        if os.path.exists(file.name):
            os.unlink(file.name)


class Connector(object):
    """Datamesh connector class.

    All datamesh operations are methods of this class
    """

    def __init__(
        self,
        token=None,
        service=os.environ.get("DATAMESH_SERVICE", DEFAULT_CONFIG["DATAMESH_SERVICE"]),
        gateway=os.environ.get("DATAMESH_GATEWAY", None),
    ):
        """Datamesh connector constructor

        Args:
            token (string): Your datamesh access token. Defaults to os.environ.get("DATAMESH_TOKEN", None).
            service (string, optional): URL of datamesh service. Defaults to os.environ.get("DATAMESH_SERVICE", "https://datamesh.oceanum.io").
            gateway (string, optional): URL of gateway service. Defaults to os.environ.get("DATAMESH_GATEWAY", "https://gateway.<datamesh_service_domain>").

        Raises:
            ValueError: Missing or invalid arguments
        """
        if token is None:
            token = os.environ.get("DATAMESH_TOKEN", None)
            if token is None:
                raise ValueError(
                    "A valid key must be supplied as a connection constructor argument or defined in environment variables as DATAMESH_TOKEN"
                )
        self._token = token
        url = urlparse(service)
        self._proto = url.scheme
        self._host = url.netloc
        self._auth_headers = {
            "Authorization": "Token " + self._token,
            "X-DATAMESH-TOKEN": self._token,
        }
        self._gateway = gateway or f"{self._proto}://gateway.{self._host}"
        self._cachedir = tempfile.TemporaryDirectory(prefix="datamesh_")

    @property
    def host(self):
        """Datamesh host

        Returns:
            string: Datamesh server host
        """
        return self._host

    # Check the status of the metadata server
    def _status(self):
        resp = requests.get(f"{self._proto}://{self._host}", headers=self._auth_headers)
        return resp.status_code == 200

    def _validate_response(self, resp):
        if resp.status_code >= 400:
            try:
                msg = resp.json()["detail"]
            except:
                msg = resp.text
            raise DatameshConnectError(msg)


    def _metadata_request(self, datasource_id="", params={}):
        resp = requests.get(
            f"{self._proto}://{self._host}/datasource/{datasource_id}",
            headers=self._auth_headers,
            params=params,
        )
        if resp.status_code == 404:
            raise DatameshConnectError(f"Datasource {datasource_id} not found")
        elif resp.status_code == 401:
            raise DatameshConnectError(f"Datasource {datasource_id} not Authorized")
        self._validate_response(resp)
        return resp

    def _metadata_write(self, datasource):
        if datasource._exists:
            resp = requests.patch(
                f"{self._proto}://{self._host}/datasource/{datasource.id}/",
                data=datasource.model_dump_json(by_alias=True, warnings=False),
                headers={**self._auth_headers, "Content-Type": "application/json"},
            )

        else:
            resp = requests.post(
                f"{self._proto}://{self._host}/datasource/",
                data=datasource.model_dump_json(by_alias=True, warnings=False),
                headers={**self._auth_headers, "Content-Type": "application/json"},
            )
        self._validate_response(resp)
        return resp

    def _delete(self, datasource_id):
        resp = requests.delete(
            f"{self._gateway}/data/{datasource_id}",
            headers=self._auth_headers,
        )
        self._validate_response(resp)
        return True

    def _data_request(self, datasource_id, data_format="application/json", cache=False):
        tmpfile = os.path.join(self._cachedir.name, datasource_id)
        resp = requests.get(
            f"{self._gateway}/data/{datasource_id}",
            headers={"Accept": data_format, **self._auth_headers},
        )
        self._validate_response(resp)
        with open(tmpfile, "wb") as f:
            f.write(resp.content)
        return tmpfile

    def _data_write(
        self,
        datasource_id,
        data,
        data_format="application/json",
        append=None,
        overwrite=False,
    ):
        if overwrite:
            resp = requests.put(
                f"{self._gateway}/data/{datasource_id}",
                data=data,
                headers={"Content-Type": data_format, **self._auth_headers},
            )
        else:
            headers = {"Content-Type": data_format, **self._auth_headers}
            if append:
                headers["X-Append"] = str(append)
            resp = requests.patch(
                f"{self._gateway}/data/{datasource_id}",
                data=data,
                headers=headers,
            )
        self._validate_response(resp)
        return Datasource(**resp.json())

    def _stage_request(self, query, cache=False):
        qhash = hashlib.sha224(
            query.model_dump_json(warnings=False).encode()
        ).hexdigest()

        resp = requests.post(
            f"{self._gateway}/oceanql/stage/",
            headers=self._auth_headers,
            data=query.model_dump_json(warnings=False),
        )
        if resp.status_code >= 400:
            msg = resp.json()["detail"]
            raise DatameshQueryError(msg)
        elif resp.status_code == 204:
            return None
        else:
            return Stage(**resp.json())

    def _query(self, query, use_dask=False, cache_timeout=0):
        if not isinstance(query, Query):
            query = Query(**query)
        if cache_timeout and not use_dask:
            localcache = LocalCache(cache_timeout)
            cached = localcache.get(query)
            if cached is not None:
                return cached
        stage = self._stage_request(query)
        if stage is None:
            warnings.warn("No data found for query")
            return None
        elif stage.size > DASK_QUERY_SIZE:
            warnings.warn(
                "Query is too large for direct access, using lazy access with dask"
            )
            use_dask = True
        if use_dask and (stage.container == Container.Dataset):
            mapper = ZarrClient(self,stage.qhash)
            return xarray.open_zarr(
                mapper, consolidated=True, decode_coords="all", mask_and_scale=True
            )
        else:
            if cache_timeout:
                localcache.lock(query)
            transfer_format = (
                "application/x-netcdf4"
                if stage.container == Container.Dataset
                else "application/parquet"
            )
            headers = {"Accept": transfer_format, **self._auth_headers}
            resp = requests.post(
                f"{self._gateway}/oceanql/",
                headers=headers,
                data=query.model_dump_json(warnings=False),
            )
            if resp.status_code >= 400:
                msg = resp.json()["detail"]
                if cache_timeout:
                    localcache.unlock(query)
                raise DatameshQueryError(msg)
            else:
                with tempFile("wb") as f:
                    f.write(resp.content)
                    f.seek(0)
                    if stage.container == Container.Dataset:
                        ds = xarray.load_dataset(
                            f.name, decode_coords="all", mask_and_scale=True
                        )
                        ext = ".nc"
                    elif stage.container == Container.GeoDataFrame:
                        ds = geopandas.read_parquet(f.name)
                        ext = ".gpq"
                    else:
                        ds = pandas.read_parquet(f.name)
                        ext = ".pq"
                    if cache_timeout:
                        localcache.copy(query, f.name, ext)
                        localcache.unlock(query)
                return ds

    def get_catalog(self, search=None, timefilter=None, geofilter=None):
        """Get datamesh catalog

        Args:
            search (string, optional): Search string for filtering datasources
            timefilter (Union[:obj:`oceanum.datamesh.query.TimeFilter`, list], Optional): Time filter as valid Query TimeFilter or list of [start,end]
            geofilter (Union[:obj:`oceanum.datamesh.query.GeoFilter`, dict, shapely.geometry], Optional): Spatial filter as valid Query Geofilter or geojson geometry as dict or shapely Geometry

        Returns:
            :obj:`oceanum.datamesh.Catalog`: A datamesh catalog instance
        """
        query = {}
        if search:
            query["search"] = search
        if timefilter:
            times = TimeFilter(times=timefilter).times
            query["in_trange"] = f"{times[0]}Z,{times[1]}Z"
        if geofilter:
            if isinstance(geofilter, GeoFilter):
                geos = geofilter.geom
            else:
                geos = shapely.geometry.shape(geofilter)
            query["geom_intersects"] = str(geofilter)
        meta = self._metadata_request(params=query)
        cat = Catalog(meta.json())
        cat._connector = self
        return cat

    @asyncwrapper
    def get_catalog_async(self, filter={}):
        """Get datamesh catalog asynchronously

        Args:
            filter (dict, optional): Set of filters to apply. Defaults to {}.
            loop: event loop. default=None will use :obj:`asyncio.get_running_loop()`
            executor: :obj:`concurrent.futures.Executor` instance. default=None will use the default executor

        Returns:
            Coroutine<:obj:`oceanum.datamesh.Catalog`>: A datamesh catalog instance
        """
        return self.get_catalog(filter)

    def get_datasource(self, datasource_id):
        """Get a Datasource instance from the datamesh. This does not load the actual data.

        Args:
            datasource_id (string): Unique datasource id

        Returns:
            :obj:`oceanum.datamesh.Datasource`: A datasource instance

        Raises:
            DatameshConnectError: Datasource cannot be found or is not authorized for the datamesh key
        """
        meta = self._metadata_request(datasource_id)
        meta_dict = meta.json()
        props = {
            "id": datasource_id,
            "geom": meta_dict["geometry"],
            **meta_dict["properties"],
        }
        ds = Datasource(**props)
        ds._exists = True
        ds._detail = True
        return ds

    @asyncwrapper
    def get_datasource_async(self, datasource_id):
        """Get a Datasource instance from the datamesh asynchronously. This does not load the actual data.

        Args:
            datasource_id (string): Unique datasource id
            loop: event loop. default=None will use :obj:`asyncio.get_running_loop()`
            executor: :obj:`concurrent.futures.Executor` instance. default=None will use the default executor

        Returns:
            Coroutine<:obj:`oceanum.datamesh.Datasource`>: A datasource instance

        Raises:
            DatameshConnectError: Datasource cannot be found or is not authorized for the datamesh key
        """
        return self.get_datasource(datasource_id)

    def load_datasource(self, datasource_id, parameters={}, use_dask=True):
        """Load a datasource into the work environment.
        For datasources which load into DataFrames or GeoDataFrames, this returns an in memory instance of the DataFrame.
        For datasources which load into an xarray Dataset, an open zarr backed dataset is returned.

        Args:
            datasource_id (string): Unique datasource id
            parameters (dict): Additional datasource parameters
            use_dask (bool, optional): Load datasource as a dask enabled datasource if possible. Defaults to True.

        Returns:
            Union[:obj:`pandas.DataFrame`, :obj:`geopandas.GeoDataFrame`, :obj:`xarray.Dataset`]: The datasource container
        """
        stage = self._stage_request(
            Query(datasource=datasource_id, parameters=parameters)
        )
        if stage is None:
            warnings.warn("No data found for query")
            return None
        if stage.container == Container.Dataset:
            mapper = ZarrClient(self,datasource_id, parameters=parameters)
            return xarray.open_zarr(
                mapper, consolidated=True, decode_coords="all", mask_and_scale=True
            )
        elif stage.container == Container.GeoDataFrame:
            tmpfile = self._data_request(datasource_id, "application/parquet")
            return geopandas.read_parquet(tmpfile)
        elif stage.container == Container.DataFrame:
            tmpfile = self._data_request(datasource_id, "application/parquet")
            return pandas.read_parquet(tmpfile)

    @asyncwrapper
    def load_datasource_async(self, datasource_id, parameters={}, use_dask=True):
        """Load a datasource asynchronously into the work environment

        Args:
            datasource_id (string): Unique datasource id
            use_dask (bool, optional): Load datasource as a dask enabled datasource if possible. Defaults to True.
            loop: event loop. default=None will use :obj:`asyncio.get_running_loop()`
            executor: :obj:`concurrent.futures.Executor` instance. default=None will use the default executor


        Returns:
            coroutine<Union[:obj:`pandas.DataFrame`, :obj:`geopandas.GeoDataFrame`, :obj:`xarray.Dataset`]>: The datasource container
        """
        return self.load_datasource(datasource_id, parameters, use_dask)

    def query(self, query=None, *, use_dask=False, cache_timeout=0, **query_keys):
        """Make a datamesh query

        Args:
            query (Union[:obj:`oceanum.datamesh.Query`, dict]): Datamesh query as a query object or a valid query dictionary

        Kwargs:
            use_dask (bool, optional): Load datasource as a dask enabled datasource if possible. Defaults to False.
            cache_timeout (int, optional): Local cache timeout in seconds. Defaults to 0 (no local cache). Only applies if use_dask=False. Will return an identical query from a local cache if available with an age of less than cache_timeout seconds. Does not check for more recent data on the server.
            **query_keys: Keywords form of query, for example datamesh.query(datasource="my_datasource")

        Returns:
            Union[:obj:`pandas.DataFrame`, :obj:`geopandas.GeoDataFrame`, :obj:`xarray.Dataset`]: The datasource container
        """
        if query is None:
            query = Query(**query_keys)
        return self._query(query, use_dask, cache_timeout)

    @asyncwrapper
    async def query_async(
        self, query, *, use_dask=False, cache_timeout=0, **query_keys
    ):
        """Make a datamesh query asynchronously

        Args:
            query (Union[:obj:`oceanum.datamesh.Query`, dict]): Datamesh query as a query object or a valid query dictionary

        Kwargs:
            use_dask (bool, optional): Load datasource as a dask enabled datasource if possible. Defaults to False.
            cache_timeout (int, optional): Local cache timeout in seconds. Defaults to 0 (no local cache). Only applies if use_dask=False. Will return an identical query from a local cache if available with an age of less than cache_timeout seconds. Does not check for more recent data on the server.
            loop: event loop. default=None will use :obj:`asyncio.get_running_loop()`
            executor: :obj:`concurrent.futures.Executor` instance. default=None will use the default executor
            **query_keys: Keywords form of query, for example datamesh.query(datasource="my_datasource")


        Returns:
            Coroutine<Union[:obj:`pandas.DataFrame`, :obj:`geopandas.GeoDataFrame`, :obj:`xarray.Dataset`]>: The datasource container
        """
        if query is None:
            query = Query(**query_keys)
        return self._query(query, use_dask, cache_timeout)

    def write_datasource(
        self,
        datasource_id,
        data,
        geometry=None,
        append=None,
        overwrite=False,
        **properties,
    ):
        """Write a datasource to datamesh from the work environment

        Args:
            datasource_id (string): Unique datasource id
            data (Union[:obj:`pandas.DataFrame`, :obj:`geopandas.GeoDataFrame`, :obj:`xarray.Dataset`, None]):  The data to be written to datamesh. If data is None, just update metadata properties.
            geometry (:obj:`oceanum.datasource.Geometry`, optional): GeoJSON geometry of the datasource
            append (string, optional): Coordinate to append on. default=None
            overwrite (bool, optional): Overwrite existing datasource. default=False
            **properties: Additional properties for the datasource - see :obj:`oceanum.datamesh.Datasource`

        Returns:
            :obj:`oceanum.datamesh.Datasource`: The datasource instance that was written to
        """
        if not re.match("^[a-z0-9_-]*$", datasource_id):
            raise DatameshWriteError(
                "Datasource ID must only contain lowercase letters, numbers, dashes and underscores"
            )
        try:
            ds = self.get_datasource(datasource_id)
        except DatameshConnectError as e:
            overwrite = True
        if data is not None:
            try:
                if isinstance(data, xarray.Dataset):
                    ds = zarr_write(
                        self,
                        datasource_id,
                        data,
                        append,
                        overwrite,
                    )
                else:
                    with tempFile("w+b") as f:
                        data.to_parquet(f, index=True)
                        f.seek(0)
                        ds = self._data_write(
                            datasource_id,
                            f.read(),
                            "application/parquet",
                            append,
                            overwrite,
                        )
                ds._exists = True
            except Exception as e:
                raise DatameshWriteError(e)
        elif overwrite:
            ds = Datasource(id=datasource_id, geom=geometry, **properties)
        for key in properties:
            if key not in ["driver", "schema"]:
                setattr(ds, key, properties[key])
        if geometry:
            ds.geom = geometry
        try:
            self._metadata_write(ds)
        except Exception as e:
            raise DatameshWriteError(f"Cannot register datasource {datasource_id}: {e}")
        return ds

    @asyncwrapper
    def write_datasource_async(
        self, datasource_id, data, append=None, overwrite=False, **properties
    ):
        """Write a datasource to datamesh from the work environment asynchronously

        Args:
            datasource_id (string): Unique datasource id
            data (Union[:obj:`pandas.DataFrame`, :obj:`geopandas.GeoDataFrame`, :obj:`xarray.Dataset`, None]): The data to be written to datamesh. If data is None, just update metadata properties.
            geometry (:obj:`oceanum.datasource.Geometry`): GeoJSON geometry of the datasource
            append (string, optional): Coordinate to append on. default=None
            overwrite (bool, optional): Overwrite existing datasource. default=False
            **properties: Additional properties for the datasource - see :obj:`oceanum.datamesh.Datasource` constructor

        Returns:
            Coroutine<:obj:`oceanum.datamesh.Datasource`>: The datasource instance that was written to
        """
        return self.write_datasource(
            datasource_id, data, append, overwrite, **properties
        )

    def delete_datasource(self, datasource_id):
        """Delete a datasource from datamesh. This will delete the datamesh registration and any stored data.

        Args:
            datasource_id (string): Unique datasource id

        Returns:
            boolean: Return True for successfully deleted datasource
        """
        return self._delete(datasource_id)

    @asyncwrapper
    def delete_datasource_async(self, datasource_id):
        """Asynchronously delete a datasource from datamesh. This will delete the datamesh registration and any stored data.

        Args:
            datasource_id (string): Unique datasource id

        Returns:
            boolean: Return True for successfully deleted datasource
        """
        return self._delete(datasource_id)
