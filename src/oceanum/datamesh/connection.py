import os
import io
import re
import shutil
import json
import time
import datetime
import tempfile
import hashlib
import fsspec
import xarray
import geopandas
import pandas
import shapely
import pyproj
import dask
import dask.dataframe
import warnings
import tempfile
from urllib.parse import urlparse
import asyncio
from functools import wraps, partial
from contextlib import contextmanager
import pyproj
import numbers

from .datasource import Datasource
from .catalog import Catalog
from .query import Query, Stage, Container, TimeFilter, GeoFilter, GeoFilterType
from .zarr import zarr_write, ZarrClient
from .cache import LocalCache
from .exceptions import DatameshConnectError, DatameshQueryError, DatameshWriteError
from .session import Session
from .utils import retried_request, DATAMESH_READ_TIMEOUT
from ..__init__ import __version__

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
        user=None,
        session_duration=None
    ):
        """Datamesh connector constructor

        Args:
            token (string): Your datamesh access token. Defaults to os.environ.get("DATAMESH_TOKEN", None).
            service (string, optional): URL of datamesh service. Defaults to os.environ.get("DATAMESH_SERVICE", "https://datamesh.oceanum.io").
            gateway (string, optional): URL of gateway service. Defaults to os.environ.get("DATAMESH_GATEWAY", "https://gateway.<datamesh_service_domain>").
            user (string, optional): Organisation user name for the datamesh connection. Defaults to None.
            session_duration (float, optional): The desired length of time for acquired datamesh sessions in hours. Will be 1 hour by default.

        Raises:
            ValueError: Missing or invalid arguments
        """
        self._token = token or os.environ.get("DATAMESH_TOKEN")
        url = urlparse(service)
        self._proto = url.scheme
        self._host = url.netloc
        self._init_auth_headers(self._token, user)
        if session_duration and not isinstance(session_duration, numbers.Number):
            raise ValueError(f"Session duration must be a valid numbers: {session_duration}")
        self._session_params =\
            {"duration": float(session_duration)} if session_duration else {}
        self._gateway = gateway
        self._cachedir = tempfile.TemporaryDirectory(prefix="datamesh_")

        self._check_info()
        if self._host.split(".")[-1] != self._gateway.split(".")[-1]:
            warnings.warn("Gateway and service domain do not match")

    def _init_auth_headers(self, token: str| None, user: str| None = None):
        if token is not None:
            if token.startswith("Bearer "):
                self._auth_headers = {"Authorization": token}
            else:
                self._auth_headers = {
                    "Authorization": "Token " + token,
                    "X-DATAMESH-TOKEN": token,
                }
                if user:
                    self._auth_headers["X-DATAMESH-USER"] = user
        else:
            raise ValueError(
                "A valid key must be supplied as a connection constructor argument or defined in environment variables as DATAMESH_TOKEN"
            )

    @property
    def host(self):
        """Datamesh host

        Returns:
            string: Datamesh server host
        """
        return self._host

    # Check the status of the metadata server
    def _status(self):
        resp = retried_request(f"{self._proto}://{self._host}",
                               headers=self._auth_headers)
        return resp.status_code == 200

    def _check_info(self):
        """
        Check if there are any infos available that need to be displayed.
        Typically will ask to update the client if the version is outdated.
        Also will try to guess gateway address if not provided.
        """

        _gateway = self._gateway or f"{self._proto}://{self._host}"
        try:
            resp = retried_request(f"{_gateway}/info/oceanum_python/{__version__}",
                                   headers=self._auth_headers)
            if resp.status_code == 200:
                r = resp.json()
                if "message" in r:
                    print(r["message"])
                print("Using datamesh API version 1")
                self._gateway = _gateway
                self._is_v1 = True
                return
            raise DatameshConnectError(f"Failed to reach datamesh: {resp.status_code}-{resp.text}")
        except:
            _gateway = self._gateway or f"{self._proto}://gateway.{self._host}"
            self._gateway = _gateway
            self._is_v1 = False
            print("Using datamesh API version beta")
        return

    def _validate_response(self, resp):
        if resp.status_code >= 400:
            try:
                msg = resp.json()["detail"]
            except:
                raise DatameshConnectError("Datamesh server error: " + resp.text)
            raise DatameshConnectError(msg)

    def _metadata_request(self, datasource_id="", params={}):
        resp = retried_request(
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
        data = datasource.model_dump_json(by_alias=True, warnings=False).encode(
            "utf-8", "ignore"
        )
        headers = {**self._auth_headers, "Content-Type": "application/json"}
        if datasource._exists:
            resp = retried_request(
                f"{self._proto}://{self._host}/datasource/{datasource.id}/",
                method="PATCH",
                data=data,
                headers=headers,
            )

        else:
            resp = retried_request(
                f"{self._proto}://{self._host}/datasource/",
                method="POST",
                data=data,
                headers=headers,
            )
        self._validate_response(resp)
        return resp

    def _delete(self, datasource_id):
        resp = retried_request(
            f"{self._gateway}/data/{datasource_id}",
            method="DELETE",
            headers=self._auth_headers,
        )
        self._validate_response(resp)
        return True

    def _data_request(self, datasource_id, data_format="application/json", cache=False):
        tmpfile = os.path.join(self._cachedir.name, datasource_id)
        resp = retried_request(
            f"{self._gateway}/data/{datasource_id}",
            headers={"Accept": data_format, **self._auth_headers},
            timeout=(DATAMESH_READ_TIMEOUT, 1800),
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
            resp = retried_request(
                f"{self._gateway}/data/{datasource_id}",
                method="PUT",
                data=data,
                headers={"Content-Type": data_format, **self._auth_headers},
                timeout=(DATAMESH_READ_TIMEOUT, None),
            )
        else:
            headers = {"Content-Type": data_format, **self._auth_headers}
            if append:
                headers["X-Append"] = str(append)
            resp = retried_request(
                f"{self._gateway}/data/{datasource_id}",
                method="PATCH",
                data=data,
                headers=headers,
                timeout=(DATAMESH_READ_TIMEOUT, None),
            )
        self._validate_response(resp)
        return Datasource(**resp.json())

    def _stage_request(self, query, session, cache=False):
        qhash = hashlib.sha224(
            query.model_dump_json(warnings=False).encode()
        ).hexdigest()

        resp = retried_request(
            f"{self._gateway}/oceanql/stage/",
            method="POST",
            headers=session.add_header(self._auth_headers),
            data=query.model_dump_json(warnings=False),
        )
        if resp.status_code >= 400:
            try:
                msg = resp.json()["detail"]
                raise DatameshQueryError(msg)
            except:
                raise DatameshConnectError("Datamesh server error: " + resp.text)
        elif resp.status_code == 204:
            return None
        else:
            return Stage(**resp.json())

    def _query(self, query, use_dask=False, cache_timeout=0, retry=0):
        if not isinstance(query, Query):
            query = Query(**query)
        if cache_timeout and not use_dask:
            localcache = LocalCache(cache_timeout)
            cached = localcache.get(query)
            if cached is not None:
                return cached
        session = Session.acquire(self)
        stage = self._stage_request(query, session)
        if stage is None:
            warnings.warn("No data found for query")
            return None
        elif stage.dlen >= 2000000 and stage.container in [
            Container.GeoDataFrame,
            Container.DataFrame,
        ]:
            warnings.warn(
                "Query limited to 2000000 rows, not all data may be returned. Use a more specific query."
            )
        elif stage.size > DASK_QUERY_SIZE:
            warnings.warn(
                "Query is too large for direct access, using lazy access with dask"
            )
            use_dask = True
        if use_dask and (stage.container == Container.Dataset):
            mapper = ZarrClient(self, stage.qhash, session=session, api="query")
            return xarray.open_zarr(
                mapper, consolidated=True, decode_coords="all", mask_and_scale=True
            )
        else:
            # Try finally takes care of closing the session
            # in the previous use_dask case the session needs to carry on
            # in order to the zarr client to keep working
            try:
                if cache_timeout:
                    localcache.lock(query)
                transfer_format = (
                    "application/x-netcdf4"
                    if stage.container == Container.Dataset
                    else "application/parquet"
                )
                headers = {"Accept": transfer_format, **self._auth_headers}
                resp = retried_request(
                    f"{self._gateway}/oceanql/",
                    method="POST",
                    headers=headers,
                    data=query.model_dump_json(warnings=False),
                    timeout=(DATAMESH_READ_TIMEOUT, 600)
                )
                if resp.status_code >= 500:
                    if cache_timeout:
                        localcache.unlock(query)
                    if retry < 5:
                        time.sleep(retry)
                        return self._query(query, use_dask, cache_timeout, retry + 1)
                    else:
                        raise DatameshConnectError("Datamesh server error: " + resp.text)
                if resp.status_code >= 400:
                    try:
                        msg = resp.json()["detail"]
                    except:
                        raise DatameshConnectError("Datamesh server error: " + resp.text)
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
            finally:
                session.close()

    def get_catalog(self, search=None, timefilter=None, geofilter=None, limit=None):
        """Get datamesh catalog

        Args:
            search (string, optional): Search string for filtering datasources
            timefilter (Union[:obj:`oceanum.datamesh.query.TimeFilter`, list], Optional): Time filter as valid Query TimeFilter or list of [start,end]
            geofilter (Union[:obj:`oceanum.datamesh.query.GeoFilter`, dict, shapely.geometry], Optional): Spatial filter as valid Query Geofilter or geojson geometry as dict or shapely Geometry
            limit (int, optional): Limit the number of datasources returned. Defaults to None.

        Returns:
            :obj:`oceanum.datamesh.Catalog`: A datamesh catalog instance
        """
        query = {}
        if limit:
            query["limit"] = limit
        if search:
            query["search"] = search
        if isinstance(timefilter, list):
            timefilter = TimeFilter(times=timefilter)
        if timefilter:
            times = timefilter.times
            query["in_trange"] = (
                f"{times[0] or datetime.datetime(1,1,1)}Z,{times[1] or datetime.datetime(2500,1,1)}Z"
            )
        if geofilter:
            if isinstance(geofilter, GeoFilter):
                if geofilter.type == GeoFilterType.feature:
                    geos = geofilter.geom.geometry
                elif geofilter.type == GeoFilterType.bbox:
                    geos = shapely.geometry.box(*geofilter.geom)
            else:
                geos = shapely.geometry.shape(geofilter)
            query["geom_intersects"] = geos.wkt
        meta = self._metadata_request(params=query)
        cat = Catalog(meta.json())
        cat._connector = self
        return cat

    @asyncwrapper
    def get_catalog_async(self, search=None, timefilter=None, geofilter=None):
        """Get datamesh catalog asynchronously

        Args:
            search (string, optional): Search string for filtering datasources
            timefilter (Union[:obj:`oceanum.datamesh.query.TimeFilter`, list], Optional): Time filter as valid Query TimeFilter or list of [start,end]
            geofilter (Union[:obj:`oceanum.datamesh.query.GeoFilter`, dict, shapely.geometry], Optional): Spatial filter as valid Query Geofilter or geojson geometry as dict or shapely Geometry

        Returns:
            Coroutine<:obj:`oceanum.datamesh.Catalog`>: A datamesh catalog instance
        """
        return self.get_catalog(search, timefilter, geofilter)

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

    def load_datasource(self, datasource_id, parameters={}, use_dask=False):
        """Load a datasource into the work environment.
        For datasources which load into DataFrames or GeoDataFrames, this returns an in memory instance of the DataFrame.
        For datasources which load into an xarray Dataset, an open zarr backed dataset is returned.

        Args:
            datasource_id (string): Unique datasource id
            parameters (dict): Additional datasource parameters
            use_dask (bool, optional): Load datasource as a dask enabled datasource if possible. Defaults to False.

        Returns:
            Union[:obj:`pandas.DataFrame`, :obj:`geopandas.GeoDataFrame`, :obj:`xarray.Dataset`]: The datasource container
        """
        session = Session.acquire(self)
        stage = self._stage_request(
            Query(datasource=datasource_id, parameters=parameters),
            session=session
        )
        if stage is None:
            warnings.warn("No data found for query")
            return None
        if stage.container == Container.Dataset or use_dask:
            mapper = ZarrClient(self, datasource_id, session, parameters=parameters, api="zarr")
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
    def load_datasource_async(self, datasource_id, parameters={}, use_dask=False):
        """Load a datasource asynchronously into the work environment

        Args:
            datasource_id (string): Unique datasource id
            use_dask (bool, optional): Load datasource as a dask enabled datasource if possible. Defaults to False.
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
    def query_async(self, query, *, use_dask=False, cache_timeout=0, **query_keys):
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
        geometry=None,  # Deprecating this option so property is consistent with the rest of the code
        geom=None,
        append=None,
        overwrite=False,
        index=None,
        crs=None,
        **properties,
    ):
        """Write a datasource to datamesh from the work environment

        Args:
            datasource_id (string): Unique datasource id
            data (Union[:obj:`pandas.DataFrame`, :obj:`geopandas.GeoDataFrame`, :obj:`xarray.Dataset`, None]):  The data to be written to datamesh. If data is None, just update metadata properties.
            geom (:obj:`oceanum.datasource.Geometry`, optional): GeoJSON geometry of the datasource in WGS84 if crs=None else in the specified crs. If not provided the geometry will be infered from the data if possible. default=None
            coordinates (Dict[:obj:`oceanum.datasource.Coordinates`,str], optional): Coordinate mapping for xarray datasets. default=None
            append (string, optional): Coordinate to append on. default=None
            overwrite (bool, optional): Overwrite existing datasource. default=False
            crs (Union[string,int], optional): Coordinate reference system for the datasource if not WGS84. The geom argument is also assumed to be in this CRS. default=None
            **properties: Additional properties for the datasource - see :obj:`oceanum.datamesh.Datasource`

        Returns:
            :obj:`oceanum.datamesh.Datasource`: The datasource instance that was written to
        """
        if not re.match("^[a-z0-9_-]*$", datasource_id):
            raise DatameshWriteError(
                "Datasource ID must only contain lowercase letters, numbers, dashes and underscores"
            )

        # Create the initial datasource object and check properties
        try:
            geom = geom or geometry or None
            if crs:
                crs = pyproj.CRS(crs)
                if geom:
                    geom = shapely.ops.transform(
                        pyproj.Transformer.from_crs(
                            crs, 4326, always_xy=True
                        ).transform,
                        shapely.geometry.shape(geom),
                    )
            name = properties.pop("name", None)
            driver = properties.pop("driver", "_null")
            _ds = Datasource(
                id=datasource_id,
                name=name or re.sub("[_-]", " ", datasource_id.capitalize()),
                geom=geom,
                driver=driver,
                **properties,
            )
        except Exception as e:
            raise DatameshWriteError(
                f"Cannot create datasource: {str(e)}. Check that the properties are valid"
            )

        # Try to get an existing datasoure with the same id
        try:
            ds = self.get_datasource(datasource_id)
        except DatameshConnectError as e:
            overwrite = True
            ds = _ds

        if ds._exists and overwrite:
            try:
                self._delete(datasource_id)
            except Exception as e:
                raise DatameshWriteError(f"Cannot delete existing datasource")

        # Write data to datasource
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
                elif isinstance(data, dask.dataframe.DataFrame):
                    for part in data.partitions:
                        with tempFile("w+b") as f:
                            part.compute().to_parquet(
                                f, compression="gzip", index="True"
                            )
                            f.seek(0)
                            ds = self._data_write(
                                datasource_id,
                                f.read(),
                                "application/parquet",
                                append,
                                overwrite,
                            )
                        append = True
                        overwrite = False
                    ds.driver_args["index"] = data.index.name
                elif isinstance(data, pandas.DataFrame):
                    with tempFile("w+b") as f:
                        data.to_parquet(f, compression="gzip", index="True")
                        f.seek(0)
                        ds = self._data_write(
                            datasource_id,
                            f.read(),
                            "application/parquet",
                            append,
                            overwrite,
                        )
                else:
                    raise DatameshWriteError(
                        "Data must be a pandas.DataFrame, geopandas.GeoDataFrame or xarray.Dataset"
                    )
                ds._exists = True
            except Exception as e:
                raise DatameshWriteError(e)
        elif overwrite:
            ds = _ds

        # Update the datasource properties
        for key in properties:
            if key not in ["driver", "schema", "crs"]:
                setattr(ds, key, properties[key])
        if name:
            ds.name = name
        if geom:
            ds.geom = geom

        # Do some property sniffing for missing properties
        if not append and data is not None:
            ds._guess_props(data, crs, append)

        # Do some final checks and conversions
        if crs:
            ds._set_crs(crs)
        badcoords = ds._check_coordinates()
        if badcoords:
            raise DatameshWriteError(f"Coordinates {badcoords} not found in data")
        if not ds.geom:
            warnings.warn(
                "Geometry not set for datasource, will have a default geometry of Point(0,0)"
            )

        # Write the metadata
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
            geom (:obj:`oceanum.datasource.Geometry`): GeoJSON geometry of the datasource
            append (string, optional): Coordinate to append on. default=None
            overwrite (bool, optional): Overwrite existing datasource. default=False
            **properties: Additional properties for the datasource - see :obj:`oceanum.datamesh.Datasource` constructor

        Returns:
            Coroutine<:obj:`oceanum.datamesh.Datasource`>: The datasource instance that was written to
        """
        return self.write_datasource(
            datasource_id, data, append, overwrite, **properties
        )

    def update_metadata(self, datasource_id, **properties):
        """Update the metadata of a datasource in datamesh

        Args:
            datasource_id (string): Unique datasource id
            **properties: Additional properties for the datasource - see :obj:`oceanum.datamesh.Datasource` constructor

        Returns:
            :obj:`oceanum.datamesh.Datasource`: The datasource instance that was updated
        """
        ds = self.get_datasource(datasource_id)
        for key in properties:
            if key not in ["driver", "schema", "driver_args"]:
                setattr(ds, key, properties[key])
            elif key in ["driver", "driver_args"]:
                warnings.warn(f"{key} is not and updatable property of a datasource")
        self._metadata_write(ds)
        return ds

    @asyncwrapper
    def update_metadata_async(self, datasource_id, **properties):
        """Update the metadata of a datasource in datamesh asynchronously

        Args:
            datasource_id (string): Unique datasource id
            **properties: Additional properties for the datasource - see :obj:`oceanum.datamesh.Datasource` constructor

        Returns:
            Coroutine<:obj:`oceanum.datamesh.Datasource`>: The datasource instance that was updated
        """
        return self.update_metadata(datasource_id, **properties)

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
