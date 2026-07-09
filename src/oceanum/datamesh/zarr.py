import datetime
import json
import re
from collections import OrderedDict
from collections.abc import MutableMapping
import os

import numpy
import xarray
import fsspec
import urllib.parse
import requests
import time
import hashlib

from typing import Optional

from .exceptions import DatameshConnectError, DatameshWriteError
from .session import Session
from .utils import (
    retried_request,
    HTTPSession,
    DATAMESH_CONNECT_TIMEOUT,
    DATAMESH_READ_TIMEOUT,
    DATAMESH_CHUNK_READ_TIMEOUT,
    DATAMESH_CHUNK_WRITE_TIMEOUT,
)

try:
    import xarray_video as xv

    _VIDEO_SUPPORT = True
except:
    _VIDEO_SUPPORT = False


_CACHEABLE_ZARR_METADATA_KEYS = {".zgroup", ".zmetadata", ".zarray", ".zattrs"}


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


def _zarr_proxy(connection, datasource_id, parameters={}):
    try:
        mapper = fsspec.get_mapper(
            f"{connection._gateway}/zarr/{datasource_id}",
            headers={
                **connection._auth_headers,
                "X-PARAMETERS": json.dumps(parameters, default=json_serial),
            },
        )
    except Exception as e:
        raise DatameshConnectError(str(e))
    return mapper


class ZarrClient(MutableMapping):
    def __init__(
        self,
        connection,
        datasource,
        session,
        parameters={},
        method="post",
        retries=10,
        read_timeout=DATAMESH_CHUNK_READ_TIMEOUT,
        connect_timeout=DATAMESH_CONNECT_TIMEOUT,
        write_timeout=DATAMESH_CHUNK_WRITE_TIMEOUT,
        nocache=False,
        api="query",
        reference_id=None,
        verify=True,
        storage_backend=None,
        presigned_writes=False,
    ):
        """
        presigned_writes (bool, optional): Upload zarr chunks directly to
            storage via a presigned URL instead of proxying the bytes
            through the write endpoint. Opt-in and defaults to False:
            presigned writes are serial per-key and currently slower than
            the classic path until client-side write concurrency lands.
        """
        self.datasource = datasource
        self.session = session
        self.method = method
        self.api = api
        self.presigned_writes = presigned_writes
        self.headers = {**connection._auth_headers}
        self.headers = session.add_header(self.headers)
        if nocache:
            self.headers["cache-control"] = "no-transform,no-cache"
        if parameters:
            self.headers["X-PARAMETERS"] = json.dumps(parameters)
        if self.api == "zarr":
            self._proxy = connection._gateway + "/zarr"
        elif self.api == "query":
            self._proxy = connection._gateway + "/zarr/query"
        else:
            raise DatameshConnectError(f"Unknown api: {self.api}")
        self.retries = retries
        self.read_timeout = read_timeout
        self.connect_timeout = connect_timeout
        self.write_timeout = write_timeout
        self.verify = verify
        if storage_backend is not None:
            self.headers["X-DATAMESH-STORAGE-BACKEND"] = storage_backend
        self.http_session = HTTPSession(headers=self.headers)
        if self.api == "zarr":
            self._data_cache = OrderedDict()
            self._data_cache_maxsize = 64
            self._cache_ttl = 30

    def _retried_request(
        self,
        path,
        method="GET",
        data=None,
        connect_timeout=DATAMESH_CONNECT_TIMEOUT,
        read_timeout=DATAMESH_CHUNK_READ_TIMEOUT,
        headers=None,
        allow_redirects=True,
    ):
        try:
            resp = retried_request(
                url=path,
                method=method,
                data=data,
                headers=headers,
                retries=self.retries,
                timeout=(connect_timeout, read_timeout),
                verify=self.verify,
                http_session=self.http_session,
                allow_redirects=allow_redirects,
            )
        except requests.RequestException as e:
            raise DatameshConnectError(str(e))

        if resp.status_code == 401:
            raise DatameshConnectError(f"Not Authorized {resp.text}")
        if resp.status_code == 410:
            raise DatameshConnectError(
                f"Datasource no longer exists or was deleted within your session"
            )
        if resp.status_code >= 500:
            raise DatameshConnectError(f"Server error {resp.status_code}: {resp.text}")
        return resp

    def _get_item(self, item):
        encoded_item = urllib.parse.quote(item, safe="/")
        resp = self._retried_request(
            f"{self._proxy}/{self.datasource}/{encoded_item}",
            connect_timeout=self.connect_timeout,
            read_timeout=self.read_timeout,
            headers={"prefer": "redirect"},
            allow_redirects=False,
        )
        if resp.status_code in (301, 302, 303, 307, 308):
            location = resp.headers.get("location")
            if not location:
                raise DatameshConnectError(f"Redirect response for {item} missing Location header")
            # Fetch the redirect target with retry/backoff but no
            # http_session -- the Location here points at S3/GCS, not the
            # datamesh gateway, and requests' session headers (baked-in
            # Authorization/X-DATAMESH-TOKEN/X-DATAMESH-SESSIONID) would
            # otherwise be sent to that third-party host if this were
            # auto-followed. requests only auto-strips Authorization on a
            # cross-host redirect, not the custom X-DATAMESH-* headers.
            redirect_resp = retried_request(
                url=location,
                method="GET",
                retries=self.retries,
                timeout=(self.connect_timeout, self.read_timeout),
                verify=self.verify,
            )
            if redirect_resp.status_code == 404:
                raise KeyError(item)
            if redirect_resp.status_code >= 400:
                raise DatameshConnectError(
                    f"Failed to fetch redirected item {item}: {redirect_resp.text}"
                )
            return redirect_resp.content
        if resp.status_code == 404:
            raise KeyError(item)
        if resp.status_code >= 400:
            raise DatameshConnectError(f"Failed to get item {item}: {resp.text}")
        return resp.content

    @staticmethod
    def _is_cacheable_metadata_key(item):
        basename = item.rsplit("/", 1)[-1]
        return basename in _CACHEABLE_ZARR_METADATA_KEYS

    def _cache_get(self, item):
        entry = self._data_cache.get(item)
        if entry is None:
            return None
        if time.time() - entry["fetched_at"] > self._cache_ttl:
            self._data_cache.pop(item, None)
            return None
        self._data_cache.move_to_end(item)
        return entry["data"]

    def _cache_set(self, item, data):
        if not self._is_cacheable_metadata_key(item):
            return
        self._data_cache[item] = {"data": data, "fetched_at": time.time()}
        self._data_cache.move_to_end(item)
        while len(self._data_cache) > self._data_cache_maxsize:
            self._data_cache.popitem(last=False)

    def __getitem__(self, item):
        if self.api == "zarr":
            data = self._cache_get(item)
            if data is not None:
                return data
            data = self._get_item(item)
            self._cache_set(item, data)
            return data
        encoded_item = urllib.parse.quote(item, safe="/")
        resp = self._retried_request(
            f"{self._proxy}/{self.datasource}/{encoded_item}",
            connect_timeout=self.connect_timeout,
            read_timeout=self.read_timeout,
        )
        if resp.status_code >= 300:
            raise KeyError(item)
        return resp.content

    def __contains__(self, item):
        if self.api == "zarr":
            if self._cache_get(item) is not None:
                return True
            try:
                data = self._get_item(item)
                self._cache_set(item, data)
                return True
            except KeyError:
                return False
        encoded_item = urllib.parse.quote(item, safe="/")
        resp = self._retried_request(
            f"{self._proxy}/{self.datasource}/{encoded_item}",
            method="HEAD",
            connect_timeout=self.connect_timeout,
            read_timeout=self.read_timeout,
        )
        if resp.status_code != 200:
            return False
        return True

    def _presigned_set_item(self, item, value):
        encoded_item = urllib.parse.quote(item, safe="/")

        resp = self._retried_request(
            f"{self._proxy}/_presign_write/{self.datasource}/{encoded_item}",
            method="POST",
            connect_timeout=DATAMESH_CONNECT_TIMEOUT,
            read_timeout=DATAMESH_READ_TIMEOUT,
        )
        if resp.status_code >= 400:
            raise DatameshWriteError(
                f"Failed to get presigned write URL for {item}: {resp.status_code} - {resp.text}"
            )
        presigned = resp.json()
        upload_url = presigned["url"]
        physical_address = presigned.get("physical_address", upload_url)

        checksum = hashlib.md5(value).hexdigest()
        size_bytes = len(value)

        try:
            scheme = urllib.parse.urlparse(upload_url).scheme
            if scheme in ("http", "https"):
                # self.http_session's underlying requests.Session has the
                # Datamesh auth/session headers permanently baked in (for
                # calls to the gateway). Passing those same keys as None
                # here strips them for this request only (requests removes
                # a header when the per-request value is None), so they
                # aren't leaked to the presigned storage URL while still
                # reusing the pooled connection.
                upload_resp = retried_request(
                    url=upload_url, method="PUT", data=value,
                    headers={k: None for k in self.headers},
                    timeout=(self.write_timeout, self.write_timeout),
                    verify=self.verify, retries=self.retries,
                    http_session=self.http_session,
                )
                if upload_resp.status_code >= 300:
                    raise DatameshWriteError(
                        f"Failed to upload {item}: upload returned {upload_resp.status_code}"
                    )
            else:
                with fsspec.open(upload_url, "wb") as f:
                    f.write(value)
        except DatameshWriteError:
            raise
        except Exception as e:
            raise DatameshWriteError(f"Failed to upload data to presigned URL: {e}")

        confirm_body = {
            "physical_address": physical_address,
            "checksum": checksum,
            "size_bytes": size_bytes,
            "content_type": "application/octet-stream",
            "user_metadata": {},
            "force": False,
        }
        try:
            confirm = retried_request(
                url=f"{self._proxy}/_confirm_write/{self.datasource}/{encoded_item}",
                method="POST",
                data=json.dumps(confirm_body),
                headers={**self.headers, "Content-Type": "application/json"},
                timeout=(self.write_timeout, self.write_timeout),
                verify=self.verify, retries=self.retries,
                http_session=self.http_session,
            )
        except requests.RequestException as e:
            raise DatameshWriteError(f"Failed to confirm write: {e}")
        if confirm.status_code >= 400:
            raise DatameshWriteError(
                f"Failed to confirm write {item}: {confirm.status_code} - {confirm.text}"
            )

        self._data_cache.pop(item, None)

    def __setitem__(self, item, value):
        if self.api == "query":
            raise DatameshConnectError("Query api does not support write operations")
        if self.api == "zarr" and self.presigned_writes:
            return self._presigned_set_item(item, value)
        encoded_item = urllib.parse.quote(item, safe="/")
        res = self._retried_request(
            f"{self._proxy}/{self.datasource}/{encoded_item}",
            method=self.method,
            data=value,
            connect_timeout=self.write_timeout,
            read_timeout=self.write_timeout,
        )
        if res.status_code >= 300:
            raise DatameshWriteError(
                f"Failed to write {item}: {res.status_code} - {res.text}"
            )

    def __delitem__(self, item):
        if self.api == "query":
            raise DatameshConnectError("Query api does not support delete operations")
        encoded_item = urllib.parse.quote(item, safe="/")
        self._retried_request(
            f"{self._proxy}/{self.datasource}/{encoded_item}",
            method="DELETE",
            connect_timeout=self.connect_timeout,
            read_timeout=10,
        )
        if self.api == "zarr":
            self._data_cache.pop(item, None)

    def __iter__(self):
        # Use the original zarr proxy endpoint (not presigned) for both APIs.
        # The proxy path is already set in __init__ based on self.api.
        resp = self._retried_request(
            f"{self._proxy}/{self.datasource}/",
            connect_timeout=self.connect_timeout,
            read_timeout=self.read_timeout,
        )
        if not resp:
            return
        ex = re.compile(r"""<(a|A)\s+(?:[^>]*?\s+)?(href|HREF)=["'](?P<url>[^"']+)""")
        links = [u[2] for u in ex.findall(resp.text)]
        for link in links:
            yield link

    def __len__(self):
        return 0

    def clear(self):
        if self.api == "zarr":
            self._data_cache.clear()
        self.__delitem__("")


def _to_zarr(data, store, **kwargs):
    if _VIDEO_SUPPORT:
        data.video.to_zarr(store, **kwargs)
    else:
        data.to_zarr(store, **kwargs)


def zarr_write(
    connection,
    datasource_id,
    data,
    append=None,
    overwrite=False,
    group: Optional[str] = None,
):
    def _is_monotonic_non_decreasing(values) -> bool:
        if len(values) < 2:
            return True
        return bool(numpy.all(values[:-1] <= values[1:]))

    with Session.acquire(connection) as session:
        store = ZarrClient(
            connection, datasource_id, session, api="zarr", nocache=True,
            presigned_writes=connection._presigned_writes,
        )
        if overwrite is True:
            store.clear()
            append = None
        else:
            ds = connection.get_datasource(datasource_id)
        if append and ds._exists:
            if append not in ds.dataschema.coords:
                raise DatameshWriteError(
                    f"Append coordinate {append} not in existing zarr"
                )
            with xarray.open_zarr(store) as dexist:
                cexist = dexist[append]
                if len(cexist.dims) > 1:
                    raise DatameshWriteError(
                        f"Append coordinate {append} has more than one dimension"
                    )
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
                    replace_slice = slice(replace_range[0], replace_range[-1] + 1)
                    replace_coord = replace_section[append]
                    existing_coord = cexist[replace_slice]
                    if not numpy.array_equal(replace_coord.values, existing_coord.values):
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
                    _to_zarr(
                        replace_section,
                        store,
                        mode="a",
                        region={append_dim: replace_slice},
                        group=group,
                    )
                if len(data[append]) > len(replace_range):
                    append_chunk = data.isel(
                        **{append_dim: slice(len(replace_range), None)}
                    )
                    _to_zarr(
                        append_chunk,
                        store,
                        mode="a",
                        append_dim=append_dim,
                        consolidated=True,
                        group=group,
                    )
        else:
            _to_zarr(data, store, mode="w", consolidated=True, group=group)
            ds = connection.get_datasource(datasource_id)
            ds.dataschema = data.to_dict(data=False)
        return ds
