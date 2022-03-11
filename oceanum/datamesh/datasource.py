from dateutil.parser import parse
import datetime
import pandas
import geopandas
import xarray
from .query import Query


class Datasource(object):
    """Datasource class"""

    @classmethod
    def _init(connector, id):
        meta = connector._metadata(id)
        if resp.status_code == 404:
            raise DatasourceException("Not found")
        elif meta.status_code == 401:
            raise DatasourceException("Not Authorized")
        elif meta.status_code != 200:
            raise DatameshException(meta.text)
        ds = Datasource(id, **meta.json)
        ds._connector = connector

    def __init__(
        self,
        datasource_id,
        geometry=None,
        name=None,
        description=None,
        tstart="1970-01-01T00:00:00Z",
        tend=None,
        schema={},
        coordinates={},
        tags=[],
        links=[],
        info={},
        details=None,
        last_modified=None,
    ):
        """Constructor for Datasource class

        Args:
            datasource_id (string): Unique datasource ID
            geometry (dict, optional): Datasource geometry as valid geojson dictionary or None. Defaults to None.
            name (string, optional): Datasource human readable name. Defaults to None.
            description (string, optional): Datasource description. Defaults to None.
            tstart (string, optional): Earliest time in datasource. Must be a valid ISO8601 datetime string. Defaults to "1970-01-01T00:00:00Z".
            tend (string, optional): Latest time in datasource. Must be a valid ISO8601 datetime string or None. Defaults to None.
            schema (dict, optional): Datasource schema. Defaults to {}.
            coordinates (dict, optional): Coordinates key. Defaults to {}.
            tags (list, optional): List of keyword tags. Defaults to [].
            links (list, optional): List of additional external URL links. Defaults to [].
            info (dict, optional): Dictionary of additional information. Defaults to {}.
            details (string, optional): URL link to additional details. Defaults to None.
            last_modified (string, optional): Latest time datasource metadata was modified. Must be a valid ISO8601 datetime string or None. Defaults to None.
        """
        self.id = datasource_id
        self._geometry = geometry
        self._name = name
        self._description = description
        self._tstart = tstart
        self._tend = tend
        self._schema = schema
        self._coordinates = coordinates
        self._tags = tags
        self._links = links
        self._info = info
        self._details = details
        self._last_modified = last_modified or datetime.datetime.utcnow()
        self._connector = None

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
        return parse(self._tstart)

    @property
    def tend(self):
        """:obj:`datetime` Latest time in datasource"""
        return parse(self._tend) if self._tend else None

    @property
    def container(self):
        """str: Container type for datasource
        Is one of:
            - `xarray.Dataset`
            - `pandas.DataFrame`
            - `geopandas.GeoDataFrame`
        """
        if "g" in self._coordinates:
            return "geopandas.GeoDataFrame"
        elif "x" in self._coordinates and "y" in self._coordinates:
            return "xarray.Dataset"
        else:
            return "pandas.DataFrame"

    def load(self):
        """Load the datasource into an in memory container or open zarr dataset

        For datasources which load into DataFrames or GeoDataFrames, this returns an in memory instance of the DataFrame.
        For datasources which load into an xarray Dataset, an open zarr backed dataset is returned.
        """
        if self.container == "xarray.Dataset":
            mapper = self._connector._zarr_proxy(self.id)
        else:
            resp = self._connector._data_request(self.id, "application/parquet")

    def query(self, query):
        """Query a datasource with optional time and/or spatial filters

        Args:
            query (Union[:obj:`oceanum.datamesh.query.Query`, dict]): Datamesh query as a query object or a valid query dictionary

        Returns:
            Union[:obj:`pandas.DataFrame`, :obj:`geopandas.GeoDataFrame`, :obj:`xarray.Dataset`]: The datasource container
        """

        if not isinstance(query, Query):
            query = Query(query)
        transfer_format = (
            "application/x-netcdf4"
            if self.container == "xarray.Dataset"
            else "application/parquet"
        )

        return self._connector._query_request(query, data_format=transfer_format)
