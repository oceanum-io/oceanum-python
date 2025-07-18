from dateutil.parser import parse
import datetime
import re
import pandas
import geopandas
import pyproj
import xarray
import rioxarray
import asyncio
import shapely
import warnings
from pydantic import (
    ConfigDict,
    BaseModel,
    Field,
    AnyHttpUrl,
    PrivateAttr,
    constr,
    BeforeValidator,
    field_validator,
)
from pydantic_core import core_schema
from pydantic.json import timedelta_isoformat
from typing_extensions import Annotated
from typing import Optional, Dict, Union, List, NamedTuple
from enum import Enum
from .query import Query, Timestamp


class DatasourceException(Exception):
    pass


def parse_period(period):
    try:
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
    except:
        raise "Period string not valid"


def to_datetime(v):
    if isinstance(v, xarray.DataArray):
        v = v.values
    return pandas.Timestamp(v).to_pydatetime()


Timeperiod = Annotated[datetime.timedelta, BeforeValidator(parse_period)]


LonField = Annotated[
    Union[float, int],
    Field(
        title="Coordinate longitude",
        ge=-180,
        le=360,
    ),
]

LatField = Annotated[
    Union[float, int],
    Field(
        title="Coordinate latitude",
        ge=-90,
        le=90,
    ),
]


class _GeometryAnnotation:
    @classmethod
    def __get_pydantic_core_schema__(cls, source, handler):
        def validate(geometry):
            if isinstance(geometry, str):
                try:
                    geometry = shapely.from_geojson(geometry)
                except:
                    "Not a valid GeoJSON string"
            if isinstance(geometry, dict):
                try:
                    geometry = shapely.geometry.shape(geometry)
                except:
                    "Not a valid GeoJSON dictionary"
            if not geometry.within(shapely.geometry.box(-180, -90, 360, 90)):
                raise ValueError("Geometry must be in WGS84 coordinates")
            if (
                isinstance(geometry, shapely.geometry.Point)
                or isinstance(geometry, shapely.geometry.MultiPoint)
                or isinstance(geometry, shapely.geometry.Polygon)
            ):
                return geometry
            else:
                raise BaseException(f"Geometry must be Point, MultiPoint or Polygon: {geometry}")

        from_geometry_schema = core_schema.no_info_plain_validator_function(validate)

        return core_schema.json_or_python_schema(
            json_schema=from_geometry_schema,
            python_schema=core_schema.union_schema(
                [
                    core_schema.is_instance_schema(shapely.geometry.Point),
                    core_schema.is_instance_schema(shapely.geometry.MultiPoint),
                    core_schema.is_instance_schema(shapely.geometry.Polygon),
                    from_geometry_schema,
                ]
            ),
            serialization=core_schema.plain_serializer_function_ser_schema(
                lambda x: shapely.to_geojson(x)
            ),
        )

    @classmethod
    def __get_pydantic_json_schema__(cls, schema, handler):
        return {"type": "object"}


Geometry = Annotated[
    Union[
        shapely.geometry.Point, shapely.geometry.MultiPoint, shapely.geometry.Polygon
    ],
    _GeometryAnnotation,
]


class Schema(BaseModel):
    attrs: Optional[dict] = Field(title="Global attributes", default={})
    dims: Optional[dict] = Field(title="Dimensions", default={})
    coords: Optional[dict] = Field(title="Coordinates", default={})
    data_vars: Optional[dict] = Field(title="Data variables", default={})


class Coordinates(Enum):
    """Coordinate keys"""

    Ensemble = "e"
    Rasterband = "b"
    Category = "c"
    Quantile = "q"
    Season = "s"
    Month = "m"
    Time = "t"
    Vertical = "z"
    Northing = "y"
    Easting = "x"
    Station = "s"  # (locations assumed stationary, datasource multigeometry coordinate indexed by station coordinate)
    Geometry = "g"  # (Abstract coordinate - a 2 or 3D geometry that defines a feature location)
    Frequency = "f"
    Direction = "d"
    Otheri = "i"
    Otherj = "j"
    Otherk = "k"


COORD_MAPPING = {
    "lon": Coordinates.Easting,
    "x": Coordinates.Easting,
    "eas": Coordinates.Easting,
    "lat": Coordinates.Northing,
    "y": Coordinates.Northing,
    "nor": Coordinates.Northing,
    "dep": Coordinates.Vertical,
    "lev": Coordinates.Vertical,
    "z": Coordinates.Vertical,
    "ens": Coordinates.Ensemble,
    "tim": Coordinates.Time,
    "ban": Coordinates.Rasterband,
    "mon": Coordinates.Month,
    "sta": Coordinates.Station,
    "sit": Coordinates.Station,
    "fre": Coordinates.Frequency,
    "dir": Coordinates.Direction,
    "cat": Coordinates.Category,
    "sea": Coordinates.Season,
    "geo": Coordinates.Geometry,
}


class Datasource(BaseModel):
    """Datasource"""

    id: str = Field(
        title="Datasource ID",
        description="Unique ID for the datasource",
        min_length=3,
        max_length=80,
        pattern=r"^[a-z0-9-_]+$",
    )
    name: str = Field(
        title="Datasource name",
        description="Human readable name for the datasource",
        max_length=128,
    )
    description: Optional[str] = Field(
        title="Datasource description",
        description="Description of datasource",
        default="",
        max_length=1500,
    )
    parameters: Optional[dict] = Field(
        title="Datasource parameters",
        description="Additional parameters for accessing datasource",
        default={},
    )
    geom: Optional[Geometry] = Field(
        title="Datasource geometry",
        description="Valid shapely or geoJSON geometry describing the spatial extent of the datasource",
        default=None,
    )
    tstart: Optional[datetime.datetime] = Field(
        title="Start time of datasource",
        description="Earliest time in datasource. Must be a valid ISO8601 datetime string",
        default=None,
    )
    tend: Optional[datetime.datetime] = Field(
        title="End time of datasource",
        description="Latest time in datasource. Must be a valid ISO8601 datetime string",
        default=None,
    )
    pforecast: Optional[Timeperiod] = Field(
        title="Datasource forecast horizon period",
        description="Duration of a forecast horizon (time after present). Must be a valid ISO8601 interval string or None.",
        default=None,
    )
    parchive: Optional[Timeperiod] = Field(
        title="Datasource rolling archive period",
        description="Duration of a rolling archive (time before present). Must be a valid ISO8601 interval string or None.",
        default=None,
    )
    tags: Optional[list] = Field(
        title="Datasource tags",
        description="Metadata keyword tags related to the datasource",
        default=[],
    )
    labels: Optional[list] = Field(
        title="Datasource labels",
        description="Metadata labels related to the datasource. A given label can only be owned by one organisation.",
        default=[],
    )
    info: Optional[dict] = Field(
        title="Datasource metadata",
        description="Additional datasource descriptive metadata",
        default={},
    )
    dataschema: Optional[Schema] = Field(
        alias="schema",
        title="Schema",
        description="Datasource schema",
        default=Schema(attrs={}, dims={}, coords={}, data_vars={}),
    )
    coordinates: Dict[Coordinates, str] = Field(
        title="Coordinate keys",
        description="""
        Coordinates in datasource, referenced by standard coordinate keys. The dictionary keys map to coordinates variables in the datasource.
            Ensemble: "e"
            Rasterband: "b"
            Category: "c"
            Quantile: "q"
            Season: "s"
            Month: "m"
            Time: "t"
            Vertical: "z"
            Northing: "y"
            Easting: "x"
            Station: "s"  (Locations assumed stationary, datasource with a multigeometry indexed by station coordinate)
            Geometry: "g"  (Abstract coordinate - a 2 or 3D geometry that defines a feature location)
            Frequency: "f"
            Direction:"d"

        Example {"t":"time","x":"longitude","y":"latitude"}
        """,
        default={},
    )
    details: Optional[AnyHttpUrl] = Field(
        title="Details",
        description="URL to further details about the datasource",
        default=None,
    )
    modified: Optional[datetime.datetime] = Field(
        title="Last modified time",
        description="Last time datasource was modified",
        default=datetime.datetime.now(tz=datetime.timezone.utc),
        frozen=True,
    )
    created: Optional[datetime.datetime] = Field(
        title="Time of creation",
        description="Time when the datasource was first created",
        default=datetime.datetime.now(tz=datetime.timezone.utc),
        frozen=True,
    )
    expires: Optional[datetime.datetime] = Field(
        title="Expiry time",
        description="Time when the datasource expires and should get automatically deleted",
        default=None,
        frozen=True,
    )
    driver_args: Optional[dict] = Field(
        alias="args",
        title="Driver arguments",
        description="Driver arguments for datasource. These are driver dependent.",
        frozen=True,
        default={},
    )
    driver: str = Field(frozen=True)

    _exists: bool = PrivateAttr(default=False)
    _detail: bool = PrivateAttr(default=False)
    # TODO[pydantic]: The following keys were removed: `json_encoders`.
    # Check https://docs.pydantic.dev/dev-v2/migration/#changes-to-config for more information.
    model_config = ConfigDict(use_enum_values=True, validate_assignment=True)

    @field_validator("id")
    @classmethod
    def validate_id(cls, v: str) -> str:
        return v.lower().strip()

    def __str__(self):
        if self._detail:
            return f"""
        {self.name} [{self.id}]
            Extent: {None if self.geom is None else self.bounds}
            Timerange: {self.tstart} to {self.tend}
            {len(self.attributes)} attributes
            {len(self.variables)} {"properties" if "g" in self.coordinates else "variables"}
        """
        else:
            return f"""
        {self.name} [{self.id}]
            Extent: {None if self.geom is None else self.bounds}
            Timerange: {self.tstart} to {self.tend}
        """

    def __repr__(self):
        return self.__str__()

    @property
    def bounds(self):
        """list[float]: Bounding box of datasource geographical extent"""
        return self.geometry.bounds

    @property
    def variables(self):
        """Datasource variables (or properties). Note that these are None (undefined) for a summary dataset."""
        return self.dataschema.data_vars if self._detail else None

    @property
    def attributes(self):
        """Datasource global attributes. Note that these are None (undefined) for a summary dataset."""
        return self.dataschema.attrs if self._detail else None

    @property
    def geometry(self):
        return self.geom

    def _guess_props(self, data, crs=None, append=False):
        if isinstance(data, pandas.DataFrame):
            data = data.reset_index()
        if self.dataschema.dims == {}:
            _data = (
                data if isinstance(data, xarray.Dataset) else data.head(1).to_xarray()
            )
            self.dataschema = _data.to_dict(data=False)
        if isinstance(data, xarray.Dataset) and data.rio.crs:
            crs = crs or data.rio.crs
        if len(self.coordinates) == 0:  # Try to guess the coordinate mapping
            coords = {}
            ds_index = (
                data.coords if isinstance(data, xarray.Dataset) else data.index.names
            )
            for c in ds_index:
                if c is None:
                    continue
                pref = c[:3].lower()
                if pref in COORD_MAPPING:
                    coords[COORD_MAPPING[pref]] = c
            self.coordinates = coords
        if self.geom is None or self.geom == shapely.geometry.Point(0, 0):
            if "x" in self.coordinates and "y" in self.coordinates:
                warnings.warn("Setting geometry as a bbox from x and y coordinates")
                self.geom = shapely.geometry.box(
                    float(data[self.coordinates["x"]].min()),
                    float(data[self.coordinates["y"]].min()),
                    float(data[self.coordinates["x"]].max()),
                    float(data[self.coordinates["y"]].max()),
                )
                if crs:
                    self.geom = shapely.ops.transform(
                        pyproj.Transformer.from_crs(
                            crs, 4326, always_xy=True
                        ).transform,
                        self.geom,
                    )
        if not self.tstart:
            if "t" in self.coordinates:
                self.tstart = to_datetime(data[self.coordinates["t"]].min())
            else:
                self.tstart = datetime.datetime(1970, 1, 1, tzinfo=None)
                warnings.warn("Setting tstart to 1970-01-01T00:00:00Z")
        if not self.tend and not self.pforecast and not append:
            if "t" in self.coordinates:
                self.tend = to_datetime(data[self.coordinates["t"]].max())
            else:
                self.tend = datetime.datetime.now(tz=datetime.timezone.utc)
                warnings.warn("Setting tend to current time")
        return self

    def _check_coordinates(self):
        badcoords = []
        for c in self.coordinates:
            if (
                self.coordinates[c] not in self.dataschema.coords
                and self.coordinates[c] not in self.dataschema.data_vars
            ):
                badcoords.append(self.coordinates[c])
        return badcoords if len(badcoords) > 0 else None

    def _set_crs(self, crs):
        if crs.to_epsg() != "4326":
            self.dataschema.attrs["crs"] = crs.to_epsg()
            return crs


def _datasource_driver(data):
    if isinstance(data, xarray.Dataset):
        return "onzarr"
    elif isinstance(data, geopandas.GeoDataFrame):
        return "postgis"
    elif isinstance(data, pandas.DataFrame):
        return "onsql"
