import datetime
import pandas as pd
import numpy as np
import shapely
import geojson_pydantic
from pydantic import (
    field_validator,
    ConfigDict,
    BaseModel,
    Field,
    BeforeValidator,
    WithJsonSchema,
)
from typing import Optional, Dict, Union, List, Any
from typing_extensions import Annotated
from enum import Enum
from geojson_pydantic import (
    Feature,
    FeatureCollection,
    Point,
    MultiPoint,
    Polygon,
)

# Monkey patch the set of allowed geometries to include only Point, MultiPoint and Polygon
geojson_pydantic.geometries.Geometry = Annotated[
    Union[
        Point,
        MultiPoint,
        Polygon,
    ],
    Field(discriminator="type"),
]


class QueryError(Exception):
    pass


def parse_time(v):
    if v is None:
        return None
    if not (
        isinstance(v, str)
        or isinstance(v, datetime.datetime)
        or isinstance(v, datetime.date)
        or isinstance(v, pd.Timestamp)
        or isinstance(v, np.datetime64)
    ):
        raise ValueError("datetime or time string required")
    try:
        if isinstance(v, np.datetime64):
            v = str(v)
        time = pd.Timestamp(v)
        if not time.tz:
            time = time.tz_localize("UTC")
        return pd.to_datetime(time.tz_convert(None))
    except Exception as e:
        raise ValueError(f"Timestamp format not valid: {e}")


Timestamp = Annotated[
    datetime.datetime,
    Field(
        default=None,
        title="Timestamp",
        description="Timestamp as python datetime, numpy datetime64 or pandas Timestamp",
    ),
    BeforeValidator(parse_time),
    WithJsonSchema({"type": "string", "format": "date-time"}),
]


def parse_timedelta(v):
    if v is None:
        return None
    if not (
        isinstance(v, str)
        or isinstance(v, datetime.timedelta)
        or isinstance(v, pd.Timedelta)
        or isinstance(v, np.timedelta64)
    ):
        raise ValueError("timedelta or time period string required")
    try:
        if isinstance(v, np.timedelta64):
            v = str(v)
        dt = pd.Timedelta(v)
        return dt.to_pytimedelta()
    except Exception as e:
        raise ValueError(f"Timedelta format not valid: {e}")


Timedelta = Annotated[
    datetime.timedelta,
    Field(
        default=None,
        title="Timedelta",
        description="Timedelta as python timedelta, numpy timedelta64 or pandas Timedelta",
    ),
    BeforeValidator(parse_timedelta),
    WithJsonSchema({"type": "string", "format": "time-period"}),
]


class GeoFilterType(str, Enum):
    feature = "feature"
    # radius = "radius"
    bbox = "bbox"


class GeoFilterInterp(str, Enum):
    """
    Interpolation method for geofilter. Can be one of:
    - 'nearest': Nearest neighbor
    - 'linear': Linear interpolation
    """

    nearest = "nearest"
    linear = "linear"


class LevelFilterInterp(str, Enum):
    """
    Interpolation method for levelfilter. Can be one of:
    - 'nearest': Nearest neighbor
    - 'linear': Linear interpolation

    Linear interpolation does not extrapolate outside the bounds of the level coordinate.
    """

    nearest = "nearest"
    linear = "linear"


class TimeFilterType(str, Enum):
    """Time filter type
    range: Select times within a range - times parameter must have 2 values
    series: Select times in a series - times parameter must have 1 or more value(s)
    trajectory: Select times along a trajectory - times parameter must have same number of values as subfeatures in a feature filter. For example same number of points as in a multipoints feature.
    """

    range = "range"
    series = "series"
    trajectory = "trajectory"


class LevelFilterType(str, Enum):
    """Level filter type
    range: Select levels within a range - levels parameter must have 2 values
    series: Select levels in a series - levels parameter must have 1 or more value(s)
    trajectory: Select levels along a trajectory - levels parameter must have same number of values as subfeatures in a feature filter. For example same number of points as in a multipoints feature.
    """

    range = "range"
    series = "series"
    trajectory = "trajectory"


class ResampleType(str, Enum):
    mean = "mean"
    nearest = "nearest"
    slinear = "linear"


class FilterGeometry(BaseModel):
    id: str = Field(title="Datasource ID")
    parameters: Optional[Dict] = Field(
        title="Optional parameters to access datasource", default={}
    )


class GeoFilter(BaseModel):
    """GeoFilter class
    Describes a spatial subset or interpolation
    """

    type: GeoFilterType = Field(
        title="Geofilter type",
        default=GeoFilterType.bbox,
        description="""
        Type of the geofilter. Can be one of:
            - 'feature': Select with a geojson feature
            - 'bbox': Select with a bounding box
        """,
        # - 'radius': Select within radius of point
    )
    geom: Union[List[float], Feature] = Field(
        title="Selection geometry",
        description="""
            - For type='feature', geojson feature as dict or shapely Geometry.
            - For type='bbox', list[x_min,y_min,x_max,y_max] in CRS units.
        """,
        # - For type='radius', list[x0,y0,radius] in CRS units.
    )
    interp: Optional[GeoFilterInterp] = Field(
        title="Interpolation method",
        default=GeoFilterInterp.linear,
        description="Interpolation method to use for feature filters",
    )
    resolution: Optional[float] = Field(
        title="Maximum spatial resolution of data",
        default=0.0,
        description="Maximum resolution of the data for downsampling in CRS units. Only works for feature datasources.",
    )
    alltouched: Optional[bool] = Field(
        title="Include all touched grid pixels", default=None
    )

    @field_validator("geom", mode="before")
    @classmethod
    def validate_geom(cls, v):
        if isinstance(v, list):
            if len(v) != 4:
                raise ValueError(
                    "bbox must be a list of length 4: [x_min,y_min,x_max,y_max]"
                )
        elif isinstance(v, dict):
            if "properties" not in v:
                v["properties"] = {}
            v = Feature(**v)
        elif isinstance(v, shapely.Geometry):
            v = Feature(type="Feature", geometry=v.__geo_interface__, properties={})
        else:
            raise TypeError(
                "geofilter geom must be a geojson feature, a list of length 4 or a shapely geometry"
            )
        return v


class LevelFilter(BaseModel):
    """LevelFilter class
    Describes a vertical subset or interpolation
    """

    type: LevelFilterType = Field(
        title="Levelfilter type",
        default=LevelFilterType.range,
        description="""
        Type of the levelfilter. Can be one of:
            - 'range': Select levels within a range, levels are a list of [levelstart, levelend]
            - 'series': Select levels in a series, levels are a list of levels
            - 'trajectory': Select levels along a trajectory, levels are a list of levels corresponding to subfeatures in a feature filter
        """,
    )
    levels: List[Union[float, None]] = Field(
        title="Selection levels",
        description="""
            - For type='range', [levelstart, levelend].
        """,
    )
    interp: Optional[LevelFilterInterp] = Field(
        title="Interpolation method",
        default=LevelFilterInterp.linear,
        description="Interpolation method to use for series type level filters",
    )


class TimeFilter(BaseModel):
    """TimeFilter class
    Describes a temporal subset or interpolation
    """

    type: TimeFilterType = Field(
        title="Timefilter type",
        default=TimeFilterType.range,
        description="""
        Type of the timefilter. Can be one of:
            - 'range': Select times within a range, times are a list of [timestart, tend]
            - 'series': Select times in a series, times are a list of times
            - 'trajectory': Select times along a trajectory, times are a list of times corresponding to subfeatures in a feature filter
        """,
    )
    times: List[Union[Timestamp, Timedelta, None]] = Field(
        title="Selection times",
        description="""
            - For type='range', [timestart, tend].
        """,
    )
    resolution: Optional[str] = Field(
        title="Temporal resolution of data",
        default="native",
        description=""""
            Maximum resolution of the data for temporal downsampling. 
            Must be a valid pandas [DateOffset.freqstr](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.tseries.offsets.DateOffset.freqstr.html#pandas.tseries.offsets.DateOffset.freqstr). Only valid with range type""",
    )
    resample: Optional[ResampleType] = Field(
        title="Temporal resampling method",
        default=ResampleType.slinear,
        description="Resampling method applied when reducing tempral resolution. Only valid with range type",
    )


class AggregateOps(str, Enum):
    mean = "mean"
    min = "min"
    max = "max"
    std = "std"
    sum = "sum"


class Aggregate(BaseModel):
    operations: List[AggregateOps] = Field(
        title="Aggregate operations to perform",
        default=[AggregateOps.mean],
        description="List of aggregation operators to apply, from "
        + ",".join(AggregateOps.__members__.keys()),
    )
    spatial: Optional[bool] = Field(
        title="Aggregate over spatial filter",
        default=True,
        description="Aggregate over spatial dimensions (default True)",
    )
    temporal: Optional[bool] = Field(
        title="Aggregate over temporal filter",
        default=True,
        description="Aggregate over temporal dimension (default True)",
    )


class Function(BaseModel):
    id: str = Field(title="Function id")
    args: Dict[str, Any] = Field(title="function arguments")
    vselect: Optional[List[str]] = Field(
        title="Apply function to variables", default=None
    )
    replace: Optional[bool] = Field(title="Replace input dataset", default=False)


# Geofilter selection process
# a features select can either be a Feature/FeatureCollection or the geometry of another datasource
# grid    ∩  bbox -> subgrid (optional resolution)
# grid    ∩  feature -> feature with added properties from grid interpolated onto the geometry
# geodf   ∩  bbox -> clipped geodf (optional resolution)
# geodf   ∩  feature -> intersecting features from geodf (optional resolution)
# Dataframe with x and y coordinates will be supported (not implemented yet)
# df      ∩  bbox -> subset df within bbox
# df      ∩  features -> subset of df within (resolution) of features


class CoordSelector(BaseModel):
    coord: str = Field(title="Coordinate name")
    values: List[str | int | float] = Field(
        title="List of coordinate values to select by"
    )


class Query(BaseModel):
    """
    Datamesh query
    """

    datasource: str = Field(
        title="The id of the datasource",
        description="Datasource ID",
        min_length=3,
        max_length=80,
    )
    parameters: Optional[Dict] = Field(
        title="Datasource parameters",
        default={},
        description="Dictionary of driver parameters to pass to datasource",
    )
    description: Optional[str] = Field(
        title="Optional description of this query",
        default=None,
        description="Human readable description of this query",
    )
    variables: Optional[List[str]] = Field(
        title="List of selected variables",
        default=None,
        description="List of requested variables.",
    )
    timefilter: Optional[TimeFilter] = Field(
        title="Time filter",
        default=None,
        description="Temporal filter or interplator",
    )
    geofilter: Optional[GeoFilter] = Field(
        title="Spatial filter or interpolator", default=None
    )
    levelfilter: Optional[LevelFilter] = Field(
        title="Vertical filter or interpolator", default=None
    )
    coordfilter: Optional[List[CoordSelector]] = Field(
        title="List of additional coordinate filters", default=None
    )
    crs: Optional[Union[str, int]] = Field(
        title="Spatial reference for filter and output",
        default=None,
        description="Valid CRS string for returned data",
    )
    aggregate: Optional[Aggregate] = Field(
        title="Aggregation operators to apply",
        default=None,
        description="Optional aggregation operators to apply to query after filtering",
    )
    functions: Optional[List[Function]] = Field(title="Functions", default=[])
    limit: Optional[int] = Field(title="Limit size of response", default=None)
    id: Optional[str] = Field(title="Unique ID of this query", default=None)

    def __bool__(self):
        for k,v in self.__dict__.items():
            if not k in ["datasource", "description", "id", "limit", "crs", "aggregate"] and v:
                return True
        return False

    def __hash__(self):
        return hash(self.model_dump_json(warnings=False))


class Workspace(BaseModel):
    data: List[Query] = Field(title="Datamesh queries")
    id: Optional[str] = Field(title="Unique ID of this package", default=None)
    name: Optional[str] = Field(title="Package name", default="OceanQL package")
    description: Optional[str] = Field(title="Package description", default="")


class Workspace(BaseModel):
    data: List[Query] = Field(title="Datamesh queries")
    id: Optional[str] = Field(title="Unique ID of this package", default=None)
    name: Optional[str] = Field(title="Package name", default="OceanQL package")
    description: Optional[str] = Field(title="Package description", default="")


class Container(str, Enum):
    GeoDataFrame = "geodataframe"
    DataFrame = "dataframe"
    Dataset = "dataset"


class Stage(BaseModel):
    query: Query = Field(title="OceanQL query")
    qhash: str = Field(title="Query hash")
    formats: List[str] = Field(title="Available download formats")
    size: int = Field(title="Request size")
    dlen: int = Field(title="Domain size")
    coordmap: dict = Field(title="coordinates map")
    coordkeys: dict = Field(title="coordinates keys")
    container: Container = Field(title="Data container type")
    sig: str = Field(title="Signature hash")
