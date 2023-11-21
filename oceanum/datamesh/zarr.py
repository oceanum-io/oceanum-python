import requests
import re
import time
import datetime
import xarray
import numpy
from collections.abc import MutableMapping

from .exceptions import DatameshWriteError

try:
    import xarray_video as xv

    _VIDEO_SUPPORT = True
except:
    _VIDEO_SUPPORT = False

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))

def _zarr_proxy(self, datasource_id, parameters={}):
        try:
            mapper = fsspec.get_mapper(
                f"{self._gateway}/zarr/{datasource_id}",
                headers={
                    **self._auth_headers,
                    "X-PARAMETERS": json.dumps(parameters, default=json_serial),
                },
            )
        except Exception as e:
            raise DatameshConnectError(str(e))
        return mapper

class ZarrClient(MutableMapping):
    def __init__(self, connection, datasource, parameters={}, method="post", retries=5, nocache=False):
        self.datasource = datasource
        self.method = method
        self.headers = {**connection._auth_headers}
        if nocache:
            self.headers["cache-control"]= "no-transform"
        if parameters:
            self.headers["X-PARAMETERS"] = json.dumps(parameters)
        self.gateway = connection._gateway + "/zarr"
        self.retries = retries

    def _get(self, path):
        retries = 0
        while retries < self.retries:
            try:
                resp = requests.get(path, headers=self.headers)
            except requests.RequestException:
                time.sleep(0.1 * 2**retries)
                retries += 1
            else:
                return resp

    def __getitem__(self, item):
        resp = self._get(f"{self.gateway}/{self.datasource}/{item}")
        if resp.status_code >= 300:
            raise KeyError(item)
        return resp.content

    def __setitem__(self, item, value):
        if self.method == "put":
            requests.put(
                f"{self.gateway}/{self.datasource}/{item}",
                data=value,
                headers=self.headers,
            )
        else:
            requests.post(
                f"{self.gateway}/{self.datasource}/{item}",
                data=value,
                headers=self.headers,
            )

    def __delitem__(self, item):
        requests.delete(
            f"{self.gateway}/{self.datasource}/{item}", headers=self.headers
        )

    def __iter__(self):
        resp = self._get(f"{self.gateway}/{self.datasource}")
        if not resp:
            return
        ex = re.compile(r"""<(a|A)\s+(?:[^>]*?\s+)?(href|HREF)=["'](?P<url>[^"']+)""")
        links = [u[2] for u in ex.findall(resp.text)]
        for link in links:
            yield link

    def __len__(self):
        return 0


def _to_zarr(data, store, **kwargs):
    if _VIDEO_SUPPORT:
        data.video.to_zarr(store, **kwargs)
    else:
        data.to_zarr(store, **kwargs)


def zarr_write(connection, datasource_id, data, append=None, overwrite=False):
    if overwrite is True:
        append = None
    else:
        ds = connection.get_datasource(datasource_id)
    store = ZarrClient(connection, datasource_id, nocache=True)
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
