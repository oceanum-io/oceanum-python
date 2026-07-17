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
import numpy
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
import urllib3
from pydantic import ValidationError

from .datasource import Datasource
from .catalog import Catalog
from .query import Query, Stage, Container, TimeFilter, GeoFilter, GeoFilterType
from .zarr import zarr_write, ZarrClient
from .zarr_v2wire import make_v2wire_store
from .zarr_v3 import make_v3_store
from .cache import LocalCache
from .exceptions import DatameshConnectError, DatameshQueryError, DatameshWriteError
from .session import Session
from .utils import (
    retried_request,
    HTTPSession,
    DATAMESH_WRITE_TIMEOUT,
    DATAMESH_CONNECT_TIMEOUT,
    DATAMESH_DOWNLOAD_TIMEOUT,
    DATAMESH_STAGE_READ_TIMEOUT,
)
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
        _gateway=os.environ.get("DATAMESH_GATEWAY", None),
        user=None,
        session_duration=None,
        verify=True,
    ):
        """Datamesh connector constructor

        Args:
            token (string): Your datamesh access token. Defaults to os.environ.get("DATAMESH_TOKEN", None).
            service (string): The datamesh service url. Defaults to os.environ.get("DATAMESH_SERVICE", "https://datamesh.oceanum.io").
            user (string, optional): Optional user identifier to be sent in the header for datamesh authentication. Defaults to None.
            session_duration (float, optional): The desired length of time for acquired datamesh sessions in seconds. Will be 3600 seconds by default.
            verify (bool, optional): Whether to verify the datamesh server certificate. Defaults to True.
        Raises:
            ValueError: Missing or invalid arguments
        """
        self._token = token or os.environ.get("DATAMESH_TOKEN")
        url = urlparse(service)
        self._proto = url.scheme
        self._host = url.netloc
        self._init_auth_headers(self._token, user)
        if session_duration and not isinstance(session_duration, numbers.Number):
            raise ValueError(
                f"Session duration must be a valid numbers: {session_duration}"
            )
        self._session_params = (
            {"duration": float(session_duration)} if session_duration else {}
        )
        if _gateway and re.match(r"^https?://gateway\.datamesh(-v0)?\.oceanum\.(io|tech)", _gateway):
            warnings.warn(
                f"The gateway url {_gateway} is deprecated. Please use https://datamesh.oceanum.io or https://datamesh.oceanum.tech instead.",
                DeprecationWarning,
            )
        self._gateway = _gateway or f"{self._proto}://{self._host}"
        self._cachedir = tempfile.TemporaryDirectory(prefix="datamesh_")
        self._verify = verify

        # Suppress InsecureRequestWarning when verify=False is used
        if not verify:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        self.http_session = HTTPSession(headers=self._auth_headers)

        self._check_info()
        if self._host.split(".")[-1] != self._gateway.split(".")[-1]:
            warnings.warn("Gateway and service domain do not match")


    def _init_auth_headers(self, token: str | None, user: str | None = None):
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

    def _retried_request(self, *args, **kwargs):
        """Wrapper around retried_request to use connection settings"""
        return retried_request(
            *args,
            verify=self._verify,
            http_session=self.http_session,
            **kwargs,
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
        resp = self._retried_request(
            f"{self._proto}://{self._host}",
        )
        return resp.status_code == 200

    def _check_info(self):
        """
        Check if there are any infos available that need to be displayed.
        Typically will ask to update the client if the version is outdated.
        Also will set gateway address to service address if not provided.
        """
        try:
            resp = self._retried_request(
                f"{self._gateway}/info/oceanum_python/{__version__}",
                retries=5,
            )
            if resp.status_code == 200:
                r = resp.json()
                if "message" in r:
                    print(r["message"])
                return
            raise DatameshConnectError(
                f"Failed to reach datamesh: {resp.status_code}-{resp.text}"
            )
        except Exception as e:
            warnings.warn(f"Failed to check status of datamesh gateway at {self._gateway}: {e}")

    def _validate_response(self, resp):
        if resp.status_code >= 400:
            try:
                msg = resp.json()["detail"]
            except:
                raise DatameshConnectError("Datamesh server error: " + resp.text)
            raise DatameshConnectError(msg)

    def _metadata_request(self, datasource_id="", params={}):
        resp = self._retried_request(
            f"{self._proto}://{self._host}/datasource/{datasource_id}",
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
        headers = {"Content-Type": "application/json"}
        if datasource._exists:
            resp = self._retried_request(
                f"{self._proto}://{self._host}/datasource/{datasource.id}/",
                method="PATCH",
                data=data,
                headers=headers,
            )

        else:
            resp = self._retried_request(
                f"{self._proto}://{self._host}/datasource/",
                method="POST",
                data=data,
                headers=headers,
            )
        self._validate_response(resp)
        return resp

    def _delete(self, datasource_id):
        resp = self._retried_request(
            f"{self._gateway}/data/{datasource_id}",
            method="DELETE",
            timeout=(DATAMESH_CONNECT_TIMEOUT, 600),
        )
        self._validate_response(resp)
        return True

    def _data_request(self, datasource_id, data_format="application/json", cache=False):
        tmpfile = os.path.join(self._cachedir.name, datasource_id)
        resp = self._retried_request(
            f"{self._gateway}/data/{datasource_id}",
            headers={"Accept": data_format},
            timeout=(DATAMESH_CONNECT_TIMEOUT, DATAMESH_DOWNLOAD_TIMEOUT),
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
        # Connection timeout does not act in the same way in write and read contexts
        # and using a short connection timeout in write contexts leads to closed connections
        headers = {"Content-Type": data_format}
        if overwrite:
            resp = self._retried_request(
                f"{self._gateway}/data/{datasource_id}",
                method="PUT",
                data=data,
                headers=headers,
                timeout=(DATAMESH_WRITE_TIMEOUT, DATAMESH_WRITE_TIMEOUT),
            )
        else:
            if append:
                headers["X-Append"] = str(append)
            resp = self._retried_request(
                f"{self._gateway}/data/{datasource_id}",
                method="PATCH",
                data=data,
                headers=headers,
                timeout=(DATAMESH_WRITE_TIMEOUT, DATAMESH_WRITE_TIMEOUT),
            )
        self._validate_response(resp)
        return Datasource(**resp.json())

    def _stage_request(self, query, session, cache=False):
        qhash = hashlib.sha224(
            query.model_dump_json(warnings=False).encode()
        ).hexdigest()

        resp = self._retried_request(
            f"{self._gateway}/oceanql/stage/",
            method="POST",
            headers=session.header,
            data=query.model_dump_json(warnings=False),
            timeout=(DATAMESH_CONNECT_TIMEOUT, DATAMESH_STAGE_READ_TIMEOUT),
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
            store = make_v2wire_store(
                self, stage.qhash, session, api="query", verify=self._verify,
                read_only=True,
            )
            return xarray.open_zarr(
                store, zarr_format=2, consolidated=True,
                decode_coords="all", mask_and_scale=True,
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
                headers = {"Accept": transfer_format,
                           **session.header}
                resp = self._retried_request(
                    f"{self._gateway}/oceanql/",
                    method="POST",
                    headers=headers,
                    data=query.model_dump_json(warnings=False),
                    timeout=(DATAMESH_CONNECT_TIMEOUT, DATAMESH_DOWNLOAD_TIMEOUT),
                )
                if resp.status_code > 500:
                    if cache_timeout:
                        localcache.unlock(query)
                    if retry < 5:
                        time.sleep(retry)
                        return self._query(query, use_dask, cache_timeout, retry + 1)
                    else:
                        raise DatameshConnectError(
                            "Datamesh server error: " + resp.text
                        )
                if resp.status_code >= 400:
                    try:
                        msg = resp.json()["detail"]
                    except:
                        raise DatameshConnectError(
                            "Datamesh server error: " + resp.text
                        )
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

    def _get_datasource_metadata(self, datasource_id):
        """Get the metadata dictionary for a given datasource id from the datamesh.

        Args:
            datasource_id (string): Unique datasource id

        Returns:
            dict: Metadata dictionary for the given datasource id

        """
        meta = self._metadata_request(datasource_id)
        meta_dict = meta.json()
        props = {
            "id": datasource_id,
            "geom": meta_dict["geometry"],
            **meta_dict["properties"],
        }
        return props

    def get_datasource(self, datasource_id):
        """Get a Datasource instance from the datamesh. This does not load the actual data.

        Args:
            datasource_id (string): Unique datasource id

        Returns:
            :obj:`oceanum.datamesh.Datasource`: A datasource instance

        Raises:
            DatameshConnectError: Datasource cannot be found or is not authorized for the datamesh key
        """
        props = self._get_datasource_metadata(datasource_id)
        try:
            ds = Datasource(**props)
        except ValidationError as e:
            raise DatameshConnectError(
                "\n"
                "\nPydantic ValidationError raised in function get_datasource.\n"
                "The metadata held in the database for the Datasource object are (old?) not consistent with the present Datasource pydantic model. Please fix\n"
                "The present metadata can be retrieved using the _get_datasource_metadata method.\n\n"
                f"{e}\n\n"
            ) from None
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
            Query(datasource=datasource_id, parameters=parameters), session=session
        )
        if stage is None:
            warnings.warn("No data found for query")
            return None
        if stage.container == Container.Dataset or use_dask:
            # Per-datasource read-wire dispatch: izarr datasources are served
            # over the v3 (izarr/Icechunk) wire, everything else over the v2
            # wire. The driver is resolved from the metadata record (never sent
            # on the zarr wire). The id may carry a chunk-pattern suffix
            # (e.g. "myds:timeseries") which is not part of the record id.
            driver = getattr(
                self.get_datasource(datasource_id.split(":")[0]), "driver", None
            )
            if driver == "izarr":
                # The v3 store acquires and owns its OWN session in the factory,
                # so release the stage session here (only one session lives for
                # the returned dataset's lifetime — the v3 store's). The store
                # (and its session) stays open for the lazily-read dataset; the
                # server embeds consolidated_metadata in the root zarr.json, so
                # consolidated open works.
                session.close()
                store = make_v3_store(self, datasource_id, read_only=True)
                return xarray.open_zarr(
                    store, zarr_format=3, consolidated=True,
                    decode_coords="all", mask_and_scale=True,
                )
            store = make_v2wire_store(
                self,
                datasource_id,
                session,
                parameters=parameters,
                api="zarr",
                verify=self._verify,
                read_only=True,
            )
            return xarray.open_zarr(
                store, zarr_format=2, consolidated=True,
                decode_coords="all", mask_and_scale=True,
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

    def _resolve_zarr_write_driver(self, ds):
        """Pick the driver to write a Dataset with (PINNED dispatch rule).

        Existing datasources keep their own frozen ``driver`` (append/update
        and overwrite alike — never an implicit driver conversion). A brand-new
        datasource with no explicit driver falls back to
        ``DATAMESH_DEFAULT_ZARR_DRIVER`` (default ``"onzarr"``; set to
        ``"izarr"`` to opt new datasources into the v3 wire).
        """
        driver = getattr(ds, "driver", None)
        if driver in (None, "", "_null"):
            driver = os.environ.get("DATAMESH_DEFAULT_ZARR_DRIVER", "onzarr")
        return driver

    def _zarr_write_v3(self, datasource_id, data, append, overwrite, ds):
        """Write an xarray Dataset over the v3 (izarr/Icechunk) wire.

        Overwrite of an existing izarr datasource is handled upstream in
        ``write_datasource`` as delete-then-recreate (Icechunk has no
        overwrite-in-place), so by the time we get here the store is fresh and
        we always write ``mode="w"`` unless appending.

        The wire is data-plane only: the client never sends driver/driver_args.
        The proxy auto-registers the write on the first ``PUT`` and resolves the
        driver args server-side from the metadata record — which is why, for a
        NEW datasource, the record is POSTed FIRST (before opening the store),
        with the izarr driver + repository args. For an EXISTING datasource the
        record already exists, so no metadata is written here and the
        ``write_datasource`` tail PATCHes it afterwards, unchanged.
        """
        mode = "a" if (append and not overwrite) else "w"
        existed = ds._exists

        if not existed:
            # NEW datasource: there is no metadata record yet for the proxy to
            # resolve driver_args from, so POST it FIRST with the izarr driver
            # and the canonical bare-repository convention (repository ==
            # datasource id, resolved server-side against
            # ICECHUNK_BUCKET/ICECHUNK_PREFIX). Sniff data-derived properties
            # first (geom is mandatory server-side); the write_datasource tail
            # then PATCHes with the full set.
            driver_args = ds.driver_args or None
            if not driver_args:
                driver_args = {"repository": datasource_id}
            ds._guess_props(data, None, append)
            if ds.geom is None:
                # geom is mandatory on POST; the v2 proxy registered new
                # records with this same documented default.
                ds.geom = shapely.geometry.Point(0, 0)
            dump = ds.model_dump(by_alias=True)
            dump["driver"] = "izarr"
            dump["args"] = driver_args
            ds = Datasource(**dump)
            ds._exists = False
            self._metadata_write(ds)
            ds._exists = True

        # Open the per-datasource v3 store (owns one session) and stream the
        # dataset through it, then finalise on success / abort on failure. A
        # single write session covers both the region write and the extend so
        # the whole append lands as one Icechunk commit.
        store = make_v3_store(self, datasource_id, read_only=False)
        try:
            try:
                if mode == "a":
                    # Append with v2-parity overlap-replace semantics.
                    self._zarr_v3_overlap_append(store, data, append)
                else:
                    data.to_zarr(
                        store,
                        mode="w",
                        consolidated=False,
                        zarr_format=3,
                    )
            except Exception:
                store._abort()
                raise
            store._finalise()
        finally:
            store.close()
        ds._exists = True
        return ds

    def _zarr_v3_overlap_append(self, store, data, append):
        """Overlap-replace append on the v3 wire (parity with ``zarr_write``).

        Ports the v2 overlap-replace algorithm from
        :func:`oceanum.datamesh.zarr.zarr_write` to the v3 store, preserving its
        exact semantics and error cases:

        * the append coordinate must exist in the existing store and be 1-D, and
          the incoming append coordinate must be monotonic non-decreasing;
        * the overlap (existing indices within ``[new[0], new[-1]]``) must be
          contiguous and no longer than the incoming data, and the overlapping
          timestamps must exactly match the existing values (no inserting new
          timestamps inside an existing range);
        * the overlapping section is region-written (dropping coords/vars that
          don't carry the append dim, exactly like v2) and the remainder is
          appended along the append dim.

        The existing dataset is opened through a read-only, non-owning clone of
        the *write* store so the read rides the same session. All validation
        reads happen before any write, and the write phase is ordered so no
        chunk is ever read after it has been written this session — see the
        write-phase comment below. This matters because the live proxy does
        NOT give read-your-writes for chunk data within a write session (chunk
        PUTs land in per-worker write forks invisible to reads until finalise;
        only ``zarr.json`` metadata commits eagerly). Both writes go through the
        caller's single write session (finalised once by ``_zarr_write_v3``).
        """

        def _is_monotonic_non_decreasing(values) -> bool:
            if len(values) < 2:
                return True
            return bool(numpy.all(values[:-1] <= values[1:]))

        # Read the existing store through a read-only clone (zarr-python opens
        # stores read-only for reads); the clone shares — and does not own —
        # the write session.
        read_store = store.with_read_only(True)
        with xarray.open_zarr(
            read_store, zarr_format=3, consolidated=True,
            decode_coords="all", mask_and_scale=True,
        ) as dexist:
            if append not in dexist.coords:
                raise DatameshWriteError(
                    f"Append coordinate {append} not in existing zarr"
                )
            cexist = dexist[append]
            if len(cexist.dims) > 1:
                raise DatameshWriteError(
                    f"Append coordinate {append} has more than one dimension"
                )
            # Materialise the existing coordinate so every subsequent check and
            # both writes operate on an in-memory copy (all reads happen before
            # any write on the shared session).
            cexist = cexist.load()
            cnew = data[append]
            if not _is_monotonic_non_decreasing(cnew.values):
                raise DatameshWriteError(
                    f"Append coordinate {append} in incoming data must be monotonic non-decreasing"
                )
            append_dim = cexist.dims[0]
            (replace_range,) = numpy.nonzero(
                ((cexist >= data[append][0]) & (cexist <= data[append][-1])).values
            )  # Get range in new data which overlaps - this just replaces everything >= first value in the new data
            if len(replace_range):
                if not numpy.all(numpy.diff(replace_range) == 1):
                    raise DatameshWriteError(
                        f"Cannot append on coordinate {append}: overlapping indices in existing zarr are non-contiguous (existing coordinate likely non-monotonic)"
                    )
                # Fail if the replacement range is larger than incomign data
                if len(replace_range) > len(data[append]):
                    raise DatameshWriteError(
                        f"Cannot append to zarr with a region that would be smaller than the original"
                    )

                drop_coords = [c for c in data.coords if c != append]
                drop_vars = [
                    v for v in data.data_vars if not append in data[v].dims
                ]
                replace_section = data.isel(
                    **{append_dim: slice(0, len(replace_range))}
                ).drop_vars(drop_coords + drop_vars, errors="ignore")
                replace_slice = slice(
                    int(replace_range[0]), int(replace_range[-1]) + 1
                )
                replace_coord = replace_section[append]
                existing_coord = cexist[replace_slice]
                if not numpy.array_equal(
                    replace_coord.values, existing_coord.values
                ):
                    raise DatameshWriteError(
                        f"Cannot append on coordinate {append}: overlap timestamps do not match existing archive values. Inserting new timestamps into an existing coordinate range is not supported"
                    )
                # Fail if we are replacing an internal section and ends of coordinates do not match
                if replace_range[-1] + 1 < len(cexist) and not numpy.array_equal(
                    replace_section[append], cexist[replace_slice]
                ):
                    raise DatameshWriteError(
                        f"Data inconsistency on coordinate {append} replacing a inner section of an existing zarr array"
                    )
            # -- write phase (session read-visibility constraint) -------------
            # The live proxy does NOT provide read-your-writes for chunk data
            # within a write session: metadata (``zarr.json``) PUTs commit
            # eagerly to the session branch and are visible to subsequent reads,
            # but chunk PUTs go into per-worker write forks that reads cannot see
            # until ``_finalise``. So a read-modify-write of a chunk written
            # earlier in this same session would read the STALE branch copy and
            # clobber the earlier write. The invariant we must preserve is:
            # within the session, no chunk read may occur for an index range
            # already written this session. We therefore order the two writes so
            # every RMW reads only pre-session state:
            #   1. Append the extension FIRST. Its boundary-chunk RMW reads the
            #      pre-session archive (nothing has been written yet), and the
            #      metadata resize commits eagerly so step 2 sees the new shape.
            #   2. Region-write the FULL incoming data over the whole
            #      overlap+extension span (deliberately re-writing the extension
            #      indices with identical values). Chunks fully covered by the
            #      region are written with no read. A partially-covered chunk's
            #      RMW read can only contribute indices OUTSIDE the region — i.e.
            #      below ``replace_start`` — which were never written this
            #      session, so the stale branch copy is valid there; every index
            #      >= ``replace_start`` comes from the region buffer. This holds
            #      for any per-variable chunk layout.
            if len(data[append]) > len(replace_range):
                extension = data.isel(
                    **{append_dim: slice(len(replace_range), None)}
                )
                extension.to_zarr(
                    store,
                    mode="a",
                    append_dim=append_dim,
                    zarr_format=3,
                    consolidated=False,
                )
            if len(replace_range):
                replace_start = int(replace_range[0])
                full_section = data.drop_vars(
                    drop_coords + drop_vars, errors="ignore"
                )
                full_slice = slice(
                    replace_start, replace_start + len(data[append])
                )
                full_section.to_zarr(
                    store,
                    mode="a",
                    region={append_dim: full_slice},
                    zarr_format=3,
                    consolidated=False,
                )

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
            geom (:obj:`oceanum.datasource.Geometry`, optional): GeoJSON geometry of the datasource in WGS84 if crs=None else in the specified crs. If not provided the geometry will be inferred from the data if possible. default=None
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
            if append:
                raise DatameshWriteError(f"Cannot append to non-existent datasource")
            else:
                overwrite = True
                ds = _ds

        if ds._exists and overwrite:
            try:
                self._delete(datasource_id)
                # This allows to carry over all metadata properties
                # while wipping the existing stored data cleanly
                ds._exists = False
                ds = Datasource(**ds.model_dump(by_alias=True))
                self._metadata_write(ds)
                # The record was just re-POSTed: mark it as existing so the
                # write wire doesn't POST it a second time (the izarr path
                # POSTs metadata FIRST for datasources it believes are new).
                ds._exists = True
            except Exception as e:
                raise DatameshWriteError(f"Cannot delete existing datasource")

        # Write data to datasource
        if data is not None:
            try:
                if isinstance(data, xarray.Dataset):
                    # Per-datasource wire dispatch (PINNED):
                    #  - existing datasource: keep its driver (never implicit
                    #    conversion), overwrite via its own wire;
                    #  - new datasource: driver from DATAMESH_DEFAULT_ZARR_DRIVER
                    #    (default "onzarr" -> v2 wire; "izarr" -> v3 wire).
                    target_driver = self._resolve_zarr_write_driver(ds)
                    if target_driver == "izarr":
                        ds = self._zarr_write_v3(
                            datasource_id, data, append, overwrite, ds
                        )
                    else:
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
