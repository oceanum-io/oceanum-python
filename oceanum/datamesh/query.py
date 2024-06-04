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
from typing import Optional, Dict, Union, List
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
    ):
        raise TypeError("datetime or time string required")
    try:
        time = pd.Timestamp(v)
        if not time.tz:
            time = time.tz_localize("UTC")
        return pd.to_datetime(time.tz_convert(None))
    except Exception as e:
        raise TypeError(f"Timestamp format not valid: {e}")


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


class GeoFilterType(Enum):
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
    """

    range = "range"
    series = "series"


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
    times: List[Union[Timestamp, None]] = Field(
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
    values: List[Union[str, int, float]] = Field(title="Coordinate value")


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
