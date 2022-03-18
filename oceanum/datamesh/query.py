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


class GeoFilterType(Enum):
    # datasource = "datasource"
    feature = "feature"
    radius = "radius"
    bbox = "bbox"


class RequestType(Enum):
    """Request Type"""

    schema = "schema"  # Just return schema with no data
    coords = "coords"  # Return schema with coordinate arrays
    data = "data"  # Return all data


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


# Geofilter selection process
# a features select can either be a Feature/FeatureCollection or the geometry of another datasource
# grid    ∩  bbox -> subgrid (optional resolution)
# grid    ∩  feature -> feature with added properties from grid interpolated onto the geometry
# geodf   ∩  bbox -> clipped geodf (optional resolution)
# geodf   ∩  feature -> intersecting features from geodf (optional resolution)
# Dataframe with x and y coordinates will be supported (not implemented yet)
# df      ∩  bbox -> subset df within bbox
# df      ∩  features -> subset of df within (resolution) of features


class Timestamp(pd.Timestamp):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if not isinstance(v, str):
            raise TypeError("datetime string required")
        try:
            return cls(v, tz="UTC").tz_convert(None).to_datetime64()
        except:
            raise "Timestamp format not valid"


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
    timefilter: Optional[List[Union[Timestamp, None]]] = Field(
        title="Time filter",
        default=None,
        description="Start and end of requested time period",
    )
    geofilter: Optional[GeoFilter] = Field(
        title="Spatial filter or interpolator", default=None
    )
    spatialref: Optional[str] = Field(
        title="Spatial reference for filter and output",
        default="EPSG:4326",
        description="Valid CRS string for returned data",
    )
    request: Optional[RequestType] = Field(
        title="Request type",
        default=RequestType.data,
        description="""
        Type of request: can be one of:
            - 'data': Return complete data request (default)
            - 'schema': Only return datasource schema
            - 'coords': Only return schema and coordinates
        """,
    )

    class Config:
        json_loads = orjson.loads
        json_dumps = _orjson_dumps
        json_encoders = {np.datetime64: str}
