from dateutil.parser import parse
import datetime
import pandas
import geopandas
import xarray
import asyncio
from shapely.geometry import shape
from .query import Query


class DatasourceException(Exception):
    pass


def parse_period(self, period):
    if period:
        m = re.match(
            r"^P(?:(\d+)Y)?(?:(\d+)M)?(?:(\d+)D)?T?(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:.\d+)?)S)?$",
            period,
        )
        if m is None:
            raise serializers.ValidationError("invalid ISO 8601 duration string")
        days = 0
        hours = 0
        minutes = 0
        if m[3]:
            days = int(m[3])
        if m[4]:
            hours = int(m[4])
        if m[5]:
            minutes = int(m[5])
        return datetime.timedelta(days=days, hours=hours, minutes=minutes)


class Datasource(object):
    """Datasource class"""

    @classmethod
    def _init(cls, connector, id, asynchronous=False):
        meta = connector._metadata_request(id)
        if meta.status_code == 404:
            raise DatasourceException(f"Datasource {id} not found")
        elif meta.status_code == 401:
            raise DatasourceException(f"Datasource {id} not Authorized")
        elif meta.status_code != 200:
            raise DatasourceException(meta.text)
        meta_dict = meta.json()
        ds = cls(id, **{"geometry": meta_dict["geometry"], **meta_dict["properties"]})
        ds._connector = connector
        return ds

    def __init__(
        self,
        datasource_id,
        geometry=None,
        name=None,
        description=None,
        tstart=None,
        tend=None,
        parchive=None,
        schema={},
        coordinates={},
        tags=[],
        links=[],
        info={},
        details=None,
        last_modified=None,
        **extra_kwargs,
    ):
        """Constructor for Datasource class

        Args:
            datasource_id (string): Unique datasource ID
            geometry (dict, optional): Datasource geometry as valid geojson dictionary or None. Defaults to None.
            name (string, optional): Datasource human readable name. Defaults to None.
            description (string, optional): Datasource description. Defaults to None.
            tstart (string, optional): Earliest time in datasource. Must be a valid ISO8601 datetime string. Defaults to "1970-01-01T00:00:00Z".
            tend (string, optional): Latest time in datasource. Must be a valid ISO8601 datetime string or None. Defaults to None.
            parchive (string, optional): Datasource rolling archive period. Must be a valid ISO8601 interval string or None. Defaults to None.
            schema (dict, optional): Datasource schema. Defaults to {}.
            coordinates (dict, optional): Coordinates key. Defaults to {}.
            tags (list, optional): List of keyword tags. Defaults to [].
            links (list, optional): List of additional external URL links. Defaults to [].
            info (dict, optional): Dictionary of additional information. Defaults to {}.
            details (string, optional): URL link to additional details. Defaults to None.
            last_modified (string, optional): Latest time datasource metadata was modified. Must be a valid ISO8601 datetime string or None. Defaults to None.
        """
        self.id = datasource_id
        self._name = name
        self._description = description
        self._tstart = tstart
        self._tend = tend
        self._parchive = parchive
        self._schema = schema
        self._coordinates = coordinates
        self._tags = tags
        self._links = links
        self._info = info
        self._details = details
        self._last_modified = last_modified or datetime.datetime.utcnow()
        self._connector = None
        self._geometry = shape(geometry)

    def __str__(self):
        return f"""
    {self._name} [{self.id}]
        Extent: {self.bounds}
        Timerange: {self.tstart} to {self.tend}
        {len(self.attributes)} attributes
        {len(self.variables)} {"properties" if "g" in self._coordinates else "variables"}
        Container: {str(self.container)}
    """

    def ___repr__(self):
        return

    @property
    def name(self):
        """str: Human readable name of datasource"""
        return self._name or "Datasource with ID " + self.id

    @property
    def description(self):
        """str: Datasource description"""
        return self._description

    @property
    def tstart(self):
        """:obj:`datetime` Earliest time in datasource"""
        if self._tstart is None:
            return datetime.datetime.utcnow() - parse_period(self.parchive)
        else:
            return parse(self._tstart)

    @property
    def tend(self):
        """:obj:`datetime` Latest time in datasource"""
        if self._tend is None:
            return datetime.datetime.utcnow()
        else:
            return parse(self._tend) if self._tend else None

    @property
    def container(self):
        """str: Container type for datasource
        Is one of:
            - :obj:`xarray.Dataset`
            - :obj:`pandas.DataFrame`
            - :obj:`geopandas.GeoDataFrame`
        """
        if "g" in self._coordinates:
            return geopandas.GeoDataFrame
        elif "x" in self._coordinates and "y" in self._coordinates:
            return xarray.Dataset
        else:
            return pandas.DataFrame

    @property
    def geometry(self):
        """:obj:`shapely.geometry.Geometry`: Geometry of datasource extent or location"""
        return self._geometry

    @property
    def bounds(self):
        """list[float]: Bounding box of datasource geographical extent"""
        return self._geometry.bounds

    @property
    def variables(self):
        """Datasource variables (or properties)"""
        return self._schema["data_vars"]

    @property
    def attributes(self):
        """Datasource global attributes"""
        return self._schema.get("attrs", {})

    def load(self):
        """Load the datasource into an in memory container or open zarr dataset

        For datasources which load into DataFrames or GeoDataFrames, this returns an in memory instance of the DataFrame.
        For datasources which load into an xarray Dataset, an open zarr backed dataset is returned.
        """
        if self.container == xarray.Dataset:
            mapper = self._connector._zarr_proxy(self.id)
            return xarray.open_zarr(mapper, consolidated=True, decode_coords="all")
        elif self.container == geopandas.GeoDataFrame:
            tmpfile = self._connector._data_request(self.id, "application/parquet")
            return geopandas.read_parquet(tmpfile)
        elif self.container == pandas.DataFrame:
            tmpfile = self._connector._data_request(self.id, "application/parquet")
            return pandas.read_parquet(tmpfile)
