from dateutil.parser import parse
import datetime
import pandas
import geopandas
import xarray
import asyncio
from datetime import datetime
from shapely.geometry import shape
from pydantic import BaseModel, Field, AnyHttpUrl
from typing import Optional, Dict, Union, List
from enum import Enum
from .query import Query, Timestamp


class DatasourceException(Exception):
    pass


class Timeperiod(datetime.timedelta):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, period):
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


class Geometry(dict):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, geojson):
        try:
            return shape(geojson)
        except:
            raise "Invalid geometry"


class Schema(BaseModel):
    attrs: Optional[dict] = Field(title="Global attributes")
    dims: dict = Field(title="Dimensions")
    coords: dict = Field(title="Coordinates")
    data_vars: dict = Field(title="Data variables")


class Coordinates(Enum):
    Ensemble = "e"
    Rasterband = "b"
    Category = "c"
    Quantile = "q"
    Season = "s"
    Month = "m"
    Time = "t"
    Vertical = "z"
    HorizontalNorth = "y"
    HorizontalEast = "x"
    Station = "s"  # (locations assumed stationary, datasource multigeometry coordinate indexed by station coordinate)
    Geometry = "g"  # (Abstract coordinate - a 2 or 3D geometry that defines a feature location)
    Frequency = "f"
    Direction = "d"
    Otheri = "i"
    Otherj = "j"
    Otherk = "k"


class Datasource(BaseModel):
    """Datasource"""

    datasource_id: str = Field(
        title="Datasource ID", description="Unique ID for the datasource"
    )
    name: str = Field(
        title="Datasource name", description="Human readable name for the datasource"
    )
    description: Optional[str] = Field(
        title="Datasource description",
        description="Description of datasource",
        default=None,
    )
    geometry: Geometry = Field(
        title="Datasource geometry",
        description="Valid geoJSON geometry describing the spatial extent of the datasource",
    )
    tstart: Optional[Timestamp] = Field(
        title="Start time of datasource",
        description="Earliest time in datasource. Must be a valid ISO8601 datetime string",
        default=None,
    )
    tend: Optional[Timestamp] = Field(
        title="End time of datasource",
        description="Latest time in datasource. Must be a valid ISO8601 datetime string",
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
    info: Optional[dict] = Field(
        title="Datasource metadata",
        description="Additional datasource descriptive metadata",
    )
    schema: Schema = Field(title="Schema", description="Datasource schema")
    coordinates: Dict[Coordinates, str] = Field(
        title="Coordinate keys",
        description="Coordinates in datasource, referenced by standard keys",
    )
    details: Optional[AnyHttpUrl] = Field(
        title="Details",
        description="URL to further details about the datasource",
        default=None,
    )
    last_modified: Optional[datetime] = Field(
        title="Last modified time",
        description="Last time datasource was modified",
        default=datetime.utcnow(),
    )

    def __str__(self):
        return f"""
    {self.name} [{self.id}]
        Extent: {self.bounds}
        Timerange: {self.tstart} to {self.tend}
        {len(self.attributes)} attributes
        {len(self.variables)} {"properties" if "g" in self._coordinates else "variables"}
        Container: {str(self.container)}
    """

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
        elif len(self._coordinates) > 2:
            return xarray.Dataset
        else:
            return pandas.DataFrame

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


def _get_schema(data):
    pass


def _guess_coordinates(data):
    pass


def _get_geometry(data):
    pass
