import datetime
import json
import re
import time
from collections.abc import MutableMapping

import numpy
import requests
import xarray
import fsspec

from typing import Optional, Dict, Union
from pydantic import BaseModel, validator, Field

from .query import Query
from .datasource import Datasource
from .exceptions import DatameshConnectError, DatameshWriteError
from .session import Session

try:
    import xarray_video as xv

    _VIDEO_SUPPORT = True
except:
    _VIDEO_SUPPORT = False

class SliceClaude(BaseModel):
    start: Optional[int] = None
    stop: Optional[int] = None
    step: Optional[int] = 1

    @validator('start', 'stop', 'step', pre=True)
    def validate_slice_components(cls, v):
        if v is None:
            return v
        if not isinstance(v, int):
            raise ValueError(f"Slice components must be integers or None, got {type(v)}")
        return v

    @classmethod
    def validate(cls, v: Union[slice, str, tuple]) -> 'SliceModel':
        if isinstance(v, slice):
            return cls(start=v.start, stop=v.stop, step=v.step)
        elif isinstance(v, str):
            parts = v.split(':')
            if len(parts) > 3:
                raise ValueError("Invalid slice string format")
            slice_dict = {}
            for i, part in enumerate(parts):
                if part:
                    slice_dict[['start', 'stop', 'step'][i]] = int(part)
            return cls(**slice_dict)
        elif isinstance(v, tuple):
            if len(v) > 3:
                raise ValueError("Slice tuple must have at most 3 elements")
            return cls(**dict(zip(['start', 'stop', 'step'], v)))
        else:
            raise ValueError(f"Expected slice, string, or tuple, got {type(v)}")

    def to_slice(self) -> slice:
        return slice(self.start, self.stop, self.step)

class Slice2(BaseModel):
    start: Optional[int] = None
    stop: Optional[int] = None
    step: Optional[int] = 1

    def to_python(self) -> slice:
        return slice(self.start, self.stop, self.step)
    
    @classmethod
    def validate(cls, v: slice):
        return cls(start=v.start, stop=v.stop, step=v.step)
    
    @classmethod
    def from_python(cls, v: slice):
        return cls(start=v.start, stop=v.stop, step=v.step)

class Selector2(Dict):
    def __init__(self, v):
        super().__init__({k:Slice.from_python(v) if isinstance(v, slice) else v for k,v in v.items()})

#        super().__init__({k:Slice.from_python(v) for k,v in v.items()})

    @classmethod
    def from_python(cls, v: dict|None):
        if v is None:
            return None
        return cls({k:Slice.from_python(v) for k,v in v.items()})


    def to_python(self) -> dict:
        if self == {}:
            return None
        return {k: v.to_python() for k,v in self.items()}

class Slice(BaseModel):
    start: Optional[int] = None
    stop: Optional[int] = None
    step: Optional[int] = 1

    @classmethod
    def from_python(cls, v: slice):
        return cls(start=v.start, stop=v.stop, step=v.step)

    def to_python(self):
        return slice(self.start, self.stop, self.step)

class Selector(BaseModel):
    selector: Optional[Dict[str, Slice]] = None

    @classmethod
    def from_python(cls, v: dict|None):
        if v is None:
            return None
        return cls(selector={k:Slice.from_python(v) for k,v in v.items()})

    def to_python(self) -> dict:
        if self.selector is None:
            return None
        return {k: v.to_python() for k,v in self.selector.items()}
    
class DriverQuery(BaseModel):
    query: Query
    datasource_last_modified: str
    driver_args: Dict = {}
    driver_parameters: Optional[Dict] = {}
    chunks: Optional[Dict] = {}
    coordinates: Optional[Dict] = {}
    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "query": Query(datasource="test_datasource"),
                    "driver_args": {"urlpath": "/data/tide/nz_2km"},
                    "chunks": {},
                    "coords": {},
                }
            ]
        }
    }


    
class ZarrProxyGetRequestParams(BaseModel):
    query: Query
    datasource: Optional[Datasource] = None
    chunks: Optional[Dict[str, int]] = {}
    downsample: Optional[Dict[str, int]] = {}
    selector: Optional[Selector] = {}
    nearest_chunk_selector: Optional[Selector] = {}
    filtered: Optional[bool] = False
    #session_id: Optional[str]
    
#    class Config:
#        arbitrary_types_allowed = True

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
        retries=8,
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

    def _get(self, path, retrieve_data=True):
        retries = 0
        while retries < self.retries:
            try:
                if retrieve_data or not self._is_v1:
                    resp = requests.get(path, headers=self.headers)
                else:
                    resp = requests.head(path, headers=self.headers)
            except requests.RequestException:
                time.sleep(0.1 * 2**retries)
                retries += 1
            else:
                return resp

    def __getitem__(self, item):
        resp = self._get(f"{self._proxy}/{self.datasource}/{item}")
        if resp.status_code >= 300:
            raise KeyError(item)
        return resp.content

    def __contains__(self, item):
        #if not self._is_v1:
        #    raise NotImplementedError
        resp = self._get(f"{self._proxy}/{self.datasource}/{item}",
                         retrieve_data=False)
        if resp.status_code != 200:
            return False
        return True

    def __setitem__(self, item, value):
        if self.api == "query":
            raise DatameshConnectError("Query api does not support write operations")
        if self.method == "put":
            res = requests.put(
                f"{self._proxy}/{self.datasource}/{item}",
                data=value,
                headers=self.headers,
            )
        else:
            res = requests.post(
                f"{self._proxy}/{self.datasource}/{item}",
                data=value,
                headers=self.headers,
            )
        if res.status_code >= 300:
            raise DatameshWriteError(f"Failed to write {item}: {res.status_code} - {res.text}")

    def __delitem__(self, item):
        if self.api == "query":
            raise DatameshConnectError("Query api does not support delete operations")
        requests.delete(
            f"{self._proxy}/{self.datasource}/{item}",
            headers=self.headers
        )

    def __iter__(self):
        resp = self._get(f"{self._proxy}/{self.datasource}")
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
