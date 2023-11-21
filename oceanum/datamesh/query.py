import datetime
import pandas as pd
import numpy as np
import shapely
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
from geojson_pydantic import Feature, FeatureCollection

class QueryError(Exception):
    pass


def parse_time(v):
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


class TimeFilterType(str, Enum):
    range = "range"


class ResampleType(str, Enum):
    mean = "mean"


class DatasourceGeom(BaseModel):
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
    geom: Union[List, Feature] = Field(
        title="Selection geometry",
        description="""
            - For type='feature', geojson feature as dict or shapely Geometry.
            - For type='bbox', list[x_min,y_min,x_max,y_max] in CRS units.
        """,
        # - For type='radius', list[x0,y0,radius] in CRS units.
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
            v = Feature(type="Feature",geometry=v.__geo_interface__, properties={})
        else:
            raise TypeError(
                "geofilter geom must be a geojson feature, a list of length 4 or a shapely geometry"
            )
        return v


class TimeFilter(BaseModel):
    """TimeFilter class
    Describes a temporal subset or interpolation
    """

    type: TimeFilterType = Field(
        title="Timefilter type",
        default=TimeFilterType.range,
        description="""
        Type of the timefilter. Can be one of:
            - 'range': Select times within a range
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
        default=ResampleType.mean,
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
    coords: dict = Field(title="coordinates dictionary")
    container: Container = Field(title="Data container type")
