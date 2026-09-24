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
import urllib3
from pydantic import ValidationError

from .datasource import Datasource
from .catalog import Catalog
from .query import Query, Stage, Container, TimeFilter, GeoFilter, GeoFilterType
from .zarr import zarr_write, ZarrClient
from .cache import LocalCache
from .exceptions import (
    DatameshConnectError,
    DatameshQueryError,
    DatameshUnavailableError,
    DatameshWriteError,
)
from .session import Session
from .utils import (
    retried_request,
    gateway_retry_delay,
    unavailable_error,
    HTTPSession,
    DATAMESH_WRITE_TIMEOUT,
    DATAMESH_CONNECT_TIMEOUT,
    DATAMESH_DOWNLOAD_TIMEOUT,
    DATAMESH_STAGE_READ_TIMEOUT,
    DATAMESH_METADATA_READ_TIMEOUT,
    DATAMESH_QUERY_RETRIES,
    DATAMESH_UNAVAILABLE_RETRY_AFTER,
)
from ..__init__ import __version__


DEFAULT_CONFIG = {"DATAMESH_SERVICE": "https://datamesh.oceanum.io"}

class _GatewayOutcome(Exception):
    """Internal: a gateway failure whose wait must happen with no session held.

    Every gateway wait on the query path is taken by _query, after the session
    is closed -- a 15-30s sleep holding a session pins server-side state for
    no reason, and the wait exists to pace the *caller*, not to keep us busy.

    `error is None` means wait then re-attempt; otherwise wait then raise it.
    Never escapes this module.
    """

    def __init__(self, delay, error=None):
        super().__init__(f"gateway outcome after {delay:.1f}s")
        self.delay = delay
        self.error = error


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
        # A catalog search is not the "small json payload" that
        # DATAMESH_READ_TIMEOUT assumes, so it gets its own budget -- and a read
        # timeout here is a latency spike on the metadata server, not a failed
        # chain with expensive work still running, so it is worth retrying.
        resp = self._retried_request(
            f"{self._proto}://{self._host}/datasource/{datasource_id}",
            params=params,
            timeout=(DATAMESH_CONNECT_TIMEOUT, DATAMESH_METADATA_READ_TIMEOUT),
            retry_read_timeout=True,
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
        # Same server as _metadata_request, so the same budget. No
        # retry_read_timeout here -- these are POST/PATCH and must not repeat.
        timeout = (DATAMESH_CONNECT_TIMEOUT, DATAMESH_METADATA_READ_TIMEOUT)
        if datasource._exists:
            resp = self._retried_request(
                f"{self._proto}://{self._host}/datasource/{datasource.id}/",
                method="PATCH",
                data=data,
                headers=headers,
                timeout=timeout,
            )

        else:
            resp = self._retried_request(
                f"{self._proto}://{self._host}/datasource/",
                method="POST",
                data=data,
                headers=headers,
                timeout=timeout,
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

    def _stage_request(self, query, session, cache=False, retry=None):
        qhash = hashlib.sha224(
            query.model_dump_json(warnings=False).encode()
        ).hexdigest()

        url = f"{self._gateway}/oceanql/stage/"
        resp = self._retried_request(
            url,
            method="POST",
            headers=session.header,
            data=query.model_dump_json(warnings=False),
            timeout=(DATAMESH_CONNECT_TIMEOUT, DATAMESH_STAGE_READ_TIMEOUT),
        )
        # retried_request does not status-retry a POST, so gateway failures
        # arrive here untouched. Staging is the cheap, qhash-keyed half of a
        # query, so a 502 here is worth re-attempting -- but through _query's
        # shared budget, so stage and download together deliver the query at
        # most DATAMESH_QUERY_RETRIES times.
        if resp.status_code in (502, 503, 504):
            if retry is None:
                # Called outside _query (load_datasource): no loop to defer to,
                # so pace here and raise the public error.
                time.sleep(gateway_retry_delay(resp))
                raise unavailable_error(url, resp)
            raise self._gateway_failure(url, resp, retry)

        if resp.status_code >= 400:
            # A JSON body with a `detail` is the server rejecting the query.
            # Anything else (an ingress error page, an empty body) is a
            # transport-level problem wearing a status code.
            detail = None
            try:
                detail = resp.json()["detail"]
            except Exception:
                pass
            # DatameshQueryError means "your request was rejected, repeating it
            # unchanged will fail the same way", so it must be 4xx only. A 500
            # carries a `detail` too -- the query-engine renders every
            # InternalQueryError as {"detail": ...} -- and those are server-side
            # failures that may well succeed on a later attempt. Labelling them
            # terminal told callers to give up on transient faults.
            if detail is not None and resp.status_code < 500:
                raise DatameshQueryError(detail)
            raise DatameshConnectError(
                "Datamesh server error: " + (resp.text if detail is None else detail)
            )
        elif resp.status_code == 204:
            return None
        else:
            return Stage(**resp.json())

    def _gateway_failure(self, url, resp, retry):
        """Build the _GatewayOutcome for a 5xx on the query path.

        One re-attempt budget is shared by the stage POST and the download
        POST, so a single query() delivers the query to query-engine at most
        twice however the two hops fail. Giving each hop its own budget let one
        call make four staging requests and sleep four long waits.

        502 means the instance serving us died and a sibling is likely healthy,
        so it is worth re-attempting. 503/504 means no instance is available, a
        dependency is down, or the request exceeded what one worker can do --
        re-running that is what took out both replicas on 2026-09-22.
        """
        delay = gateway_retry_delay(resp)
        error = unavailable_error(url, resp, attempted=retry)
        if resp.status_code == 502 and retry + 1 < DATAMESH_QUERY_RETRIES:
            return _GatewayOutcome(delay)
        return _GatewayOutcome(delay, error)

    def _query(self, query, use_dask=False, cache_timeout=0, retry=0):
        """Run a query, re-attempting a 502 after a long jittered wait.

        The wait happens here rather than inside _query_attempt so the session
        is already closed while we sleep -- a re-attempt that held the old
        session open for half a minute would leave server-side session state
        pinned for no reason.
        """
        while True:
            try:
                return self._query_attempt(query, use_dask, cache_timeout, retry)
            except _GatewayOutcome as outcome:
                # The session is already closed by the time we get here.
                time.sleep(outcome.delay)
                if outcome.error is not None:
                    # `from None` so the internal sentinel does not appear as
                    # "During handling of the above exception..." in the user's
                    # traceback. It is an implementation detail.
                    raise outcome.error from None
                retry += 1

    def _query_attempt(self, query, use_dask=False, cache_timeout=0, retry=0):
        if not isinstance(query, Query):
            query = Query(**query)
        # Created whenever caching is on, but only *consulted* here when the
        # caller is not asking for a lazy result -- there is nothing useful to
        # serve a dask-backed query from the local cache.
        #
        # These used to be the same condition, while the lock/copy calls below
        # tested `cache_timeout` alone. use_dask is also set True further down
        # for any query over DASK_QUERY_SIZE, so a large DataFrame query with
        # caching on reached localcache.lock() with localcache undefined and
        # raised NameError.
        localcache = LocalCache(cache_timeout) if cache_timeout else None
        if localcache is not None and not use_dask:
            cached = localcache.get(query)
            if cached is not None:
                return cached
        session = Session.acquire(self)
        # Staging sits outside the try/finally below (the dask branch has to
        # keep the session alive), so its failure paths have to close the
        # session themselves or it leaks until atexit.
        try:
            stage = self._stage_request(query, session, retry=retry)
        except BaseException:
            session.close()
            raise
        if stage is None:
            session.close()
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
            mapper = ZarrClient(
                self, stage.qhash, session=session, api="query", verify=self._verify
            )
            return xarray.open_zarr(
                mapper, consolidated=True, decode_coords="all", mask_and_scale=True
            )
        else:
            # Try finally takes care of closing the session
            # in the previous use_dask case the session needs to carry on
            # in order to the zarr client to keep working
            try:
                if localcache is not None:
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
                    if localcache is not None:
                        localcache.unlock(query)
                    # 502 and 503 mean different things here and deserve
                    # different handling.
                    #
                    # 502 is the ingress reporting that the instance serving
                    # this request died. A sibling is very likely healthy, the
                    # query has to run somewhere, and the work was lost rather
                    # than completed -- so one re-attempt is worth it. But
                    # after a long jittered wait, not half a second: the pod
                    # is still being replaced, and a fast retry during a
                    # correlated failure is how a rolling restart becomes a
                    # storm.
                    #
                    # 503/504 is never worth an automatic re-attempt. It means
                    # no instance is available, a dependency is down, or the
                    # request exceeded what one worker can process -- and in
                    # that last case re-running it is precisely what took out
                    # both query-engine replicas on 2026-09-22. We still wait
                    # before raising, because that wait rate-limits whatever
                    # retry loop is wrapping this call.
                    raise self._gateway_failure(
                        f"{self._gateway}/oceanql/", resp, retry
                    )
                if resp.status_code >= 400:
                    if localcache is not None:
                        localcache.unlock(query)
                    detail = None
                    try:
                        detail = resp.json()["detail"]
                    except Exception:
                        pass  # not a JSON error body (e.g. an ingress page)
                    # 4xx only -- see the note in _stage_request. A 500 here is
                    # the query-engine's InternalQueryError handler, not a
                    # rejected query.
                    if detail is not None and resp.status_code < 500:
                        raise DatameshQueryError(detail)
                    raise DatameshConnectError(
                        "Datamesh server error: "
                        + (resp.text if detail is None else detail)
                    )
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
                        if localcache is not None:
                            localcache.copy(query, f.name, ext)
                            localcache.unlock(query)
                    return ds
            finally:
                # The download or the parse can raise between lock() and the
                # copy below; without this the next query() for the same hash
                # blocks on the lock for its full 60 s timeout. unlock() is a
                # no-op when nothing is locked, so calling it here is safe even
                # after the success path has already unlocked.
                if localcache is not None:
                    localcache.unlock(query)
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
        # As in _query_attempt: the dask/zarr branch below keeps the session
        # alive, so there is no blanket finally and the early exits have to
        # close it themselves.
        try:
            stage = self._stage_request(
                Query(datasource=datasource_id, parameters=parameters),
                session=session,
            )
        except BaseException:
            session.close()
            raise
        if stage is None:
            session.close()
            warnings.warn("No data found for query")
            return None
        if stage.container == Container.Dataset or use_dask:
            mapper = ZarrClient(
                self,
                datasource_id,
                session,
                parameters=parameters,
                api="zarr",
                verify=self._verify,
            )
            return xarray.open_zarr(
                mapper, consolidated=True, decode_coords="all", mask_and_scale=True
            )
        # Only the zarr branch above needs the session to outlive this call; the
        # /data/ GET does not use it at all, so these branches must close it or
        # it survives until atexit and holds server-side state meanwhile.
        try:
            if stage.container == Container.GeoDataFrame:
                tmpfile = self._data_request(datasource_id, "application/parquet")
                return geopandas.read_parquet(tmpfile)
            elif stage.container == Container.DataFrame:
                tmpfile = self._data_request(datasource_id, "application/parquet")
                return pandas.read_parquet(tmpfile)
        finally:
            session.close()

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
