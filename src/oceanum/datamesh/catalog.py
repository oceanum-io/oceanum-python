from geojson_pydantic import Feature, FeatureCollection
from .datasource import Datasource


class Catalog(object):
    """Datamesh catalog
    This class behaves like an immutable dictionary with the datasource ids as keys
    """

    def __init__(self, json):
        """Constructor for Catalog class"""
        self._geojson = FeatureCollection(**json)
        self._ids = [ds.id for ds in self._geojson.features]

    def __len__(self):
        return len(self._geojson.features)

    def __str__(self):
        datasources = [
            f" {ds.properties['name']} [{ds.id}]" for ds in self._geojson.features
        ]
        return f"Datamesh catalog with {len(datasources)} datasources:\n" + "\n".join(
            datasources
        )

    def __getitem__(self, item):
        if item in self._ids:
            index = self._ids.index(item)
            feature = self._geojson.features[index]
            return Datasource(
                id=feature.id, geom=feature.geometry.model_dump(), **feature.properties
            )
        else:
            raise IndexError(f"Datasource {item} not in catalog")

    def __setitem__(self, item):
        raise ValueError("Datamesh catalog is read only")

    def __iter__(self):
        for item in self._ids:
            yield self[item]

    @property
    def ids(self):
        """Return a list of datasource ids"""
        return self._ids

    def keys(self):
        """Return a list of datasource ids"""
        return self._ids

    def load(self, id):
        """Load datasource

        Args:
            id: Datasource id

        Returns:
            Union[:obj:`pandas.DataFrame`, :obj:`geopandas.GeoDataFrame`, :obj:`xarray.Dataset`]: The datasource container
        """
        return self._connector.load_datasource(id)

    async def load_async(self, id):
        """Load datasource asynchronously

        Args:
            id: Datasource id

        Returns:
            Couroutine<Union[:obj:`pandas.DataFrame`, :obj:`geopandas.GeoDataFrame`, :obj:`xarray.Dataset`]>: The datasource container
        """
        return await self._connector.load_datasource_async(id)

    def query(self, query):
        """Make a query on the catalog

        Args:
            query (Union[:obj:`oceanum.datamesh.Query`, dict]): Datamesh query as a query object or a valid query dictionary

        Returns:
            Union[:obj:`pandas.DataFrame`, :obj:`geopandas.GeoDataFrame`, :obj:`xarray.Dataset`]: The datasource container

        Raises:
            IndexError: Datasource not in catalog
        """
        if query.datasource not in self.ids:
            raise IndexError(f"Datasource {query['datasource']} not in catalog")
        else:
            return self._connection.query(query)

    async def query_async(self, query):
        """Make an asynchronous query on the catalog

        Args:
            query (Union[:obj:`oceanum.datamesh.Query`, dict]): Datamesh query as a query object or a valid query dictionary

        Returns:
            Coroutine<Union[:obj:`pandas.DataFrame`, :obj:`geopandas.GeoDataFrame`, :obj:`xarray.Dataset`]>: The datasource container

        Raises:
            IndexError: Datasource not in catalog
        """
        if query["datasource"] not in self.ids:
            raise IndexError(f"Datasource {query['datasource']} not in catalog")
        else:
            return await self._connection.query_async(query)
