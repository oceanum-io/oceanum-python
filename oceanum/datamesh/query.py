import datetime
import orjson
import pandas as pd
import numpy as np
from pydantic import BaseModel, Field
from typing import Optional, Dict, Union, List
from enum import Enum
from geojson_pydantic import Feature, FeatureCollection


def _orjson_dumps(val, *, default):
    return orjson.dumps(val, default=default).decode()


class QueryError(Exception):
    pass


class Timestamp(pd.Timestamp):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if not (isinstance(v, str) or isinstance(v, datetime.datetime)):
            raise TypeError("datetime or time string required")
        try:
            return cls(v, tz="UTC").tz_convert(None).to_datetime64()
        except:
            raise "Timestamp format not valid"


class GeoFilterType(Enum):
    # datasource = "datasource"
    feature = "feature"
    radius = "radius"
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
            - 'radius': Select within radius of point
        """,
    )
    geom: Union[List, Feature] = Field(
        title="Selection geometry",
        description="""
            - For type='feature', geojson feature.
            - For type='bbox', list[x_min,y_min,x_max,y_max] in CRS units.
            - For type='radius', list[x0,y0,radius] in CRS units.
        """,
    )
    resolution: Optional[float] = Field(
        title="Maximum spatial resolution of data",
        default=0.0,
        description="Maximum resolution of the data for downsampling in CRS units",
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
        description="Maximum resolution of the data for temporal downsampling. Only valid with range type",
    )
    resample: Optional[ResampleType] = Field(
        title="Temporal resampling method",
        default=ResampleType.mean,
        description="Resampling method applied when reducing tempral resolution. Only valid with range type",
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


class Query(BaseModel):
    """
    Datamesh query
    """

    datasource: str = Field(
        title="The id of the datasource", description="Datasource ID"
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
    crs: Optional[Union[str, int]] = Field(
        title="Spatial reference for filter and output",
        default="EPSG:4326",
        description="Valid CRS string for returned data",
    )

    class Config:
        json_loads = orjson.loads
        json_dumps = _orjson_dumps
        json_encoders = {np.datetime64: str}
