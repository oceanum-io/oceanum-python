import datetime
import json
import re
from collections.abc import MutableMapping
import os

import numpy
import xarray
import fsspec

from .exceptions import DatameshConnectError, DatameshWriteError
from .session import Session
from .utils import retried_request, DATAMESH_CONNECT_TIMEOUT

try:
    import xarray_video as xv

    _VIDEO_SUPPORT = True
except:
    _VIDEO_SUPPORT = False

# Different default for zarr client
DATAMESH_READ_TIMEOUT = os.getenv("DATAMESH_READ_TIMEOUT", 60)
DATAMESH_READ_TIMEOUT = None if DATAMESH_READ_TIMEOUT == "None" else float(DATAMESH_READ_TIMEOUT)


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
        timeout=DATAMESH_READ_TIMEOUT,
        nocache=False,
        api="query",
        reference_id=None
    ):
        self.datasource = datasource
        self.session = session
        self.method = method
        self._is_v1 = connection._is_v1
        self.api = api if connection._is_v1 else "zarr"
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
        self.timeout = timeout

    def _retried_request(self, path, method="GET", data=None, timeout=None):
        resp = retried_request(
            url=path,
            method=method,
            data=data,
            headers=self.headers,
            retries=self.retries,
            timeout=(DATAMESH_CONNECT_TIMEOUT, timeout),
        )
        if resp.status_code == 401:
            raise DatameshConnectError(f"Not Authorized {resp.text}")
        return resp

    def __getitem__(self, item):
        resp = self._retried_request(f"{self._proxy}/{self.datasource}/{item}",
                                     timeout=self.timeout)
        if resp.status_code >= 300:
            raise KeyError(item)
        return resp.content

    def __contains__(self, item):
        resp = self._retried_request(f"{self._proxy}/{self.datasource}/{item}",
                                     method="HEAD" if self._is_v1 else "GET",
                                     timeout=self.timeout)
        if resp.status_code != 200:
            return False
        return True

    def __setitem__(self, item, value):
        if self.api == "query":
            raise DatameshConnectError("Query api does not support write operations")
        res = self._retried_request(f"{self._proxy}/{self.datasource}/{item}",
                                    method=self.method,
                                    data=value,
                                    timeout=None)
        if res.status_code >= 300:
            raise DatameshWriteError(f"Failed to write {item}: {res.status_code} - {res.text}")

    def __delitem__(self, item):
        if self.api == "query":
            raise DatameshConnectError("Query api does not support delete operations")
        self._retried_request(f"{self._proxy}/{self.datasource}/{item}",
                              method="DELETE",
                              timeout=10)

    def __iter__(self):
        resp = self._retried_request(f"{self._proxy}/{self.datasource}/",
                                     timeout=self.timeout)
        if not resp:
            return
        ex = re.compile(r"""<(a|A)\s+(?:[^>]*?\s+)?(href|HREF)=["'](?P<url>[^"']+)""")
        links = [u[2] for u in ex.findall(resp.text)]
        for link in links:
            yield link

    def __len__(self):
        return 0

    def clear(self):
        self.__delitem__("")

def _to_zarr(data, store, **kwargs):
    if _VIDEO_SUPPORT:
        data.video.to_zarr(store, **kwargs)
    else:
        data.to_zarr(store, **kwargs)


def zarr_write(connection, datasource_id, data, append=None, overwrite=False):
    with Session.acquire(connection) as session:
        store = ZarrClient(connection, datasource_id, session, api="zarr", nocache=True)
        if overwrite is True:
            store.clear()
            append = None
        else:
            ds = connection.get_datasource(datasource_id)
        if append and ds._exists:
            if append not in ds.dataschema.coords:
                raise DatameshWriteError(f"Append coordinate {append} not in existing zarr")
            with xarray.open_zarr(store) as dexist:
                cexist = dexist[append]
                if len(cexist.dims) > 1:
                    raise DatameshWriteError(
                        f"Append coordinate {append} has more than one dimension"
                    )
                append_dim = cexist.dims[0]
                (replace_range,) = numpy.nonzero(
                    ((cexist >= data[append][0]) & (cexist <= data[append][-1])).values
                )  # Get range in new data which overlaps - this just replaces everything >= first value in the new data
                if len(replace_range):
                    # Fail if the replacement range is larger than incomign data
                    if len(replace_range) > len(data[append]):
                        raise DatameshWriteError(
                            f"Cannot append to zarr with a region that would be smaller than the original"
                        )

                    drop_coords = [c for c in data.coords if c != append]
                    replace_section = data.isel(
                        **{append_dim: slice(0, len(replace_range))}
                    ).drop(drop_coords)
                    replace_slice = slice(replace_range[0], replace_range[-1] + 1)
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
                    )
        else:
            _to_zarr(data, store, mode="w", consolidated=True)
            ds = connection.get_datasource(datasource_id)
            ds.dataschema = data.to_dict(data=False)
        return ds
