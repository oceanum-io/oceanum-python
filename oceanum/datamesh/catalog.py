from geojson_pydantic import Feature, FeatureCollection
from .datasource import Datasource


class Catalog(object):
    """Datamesh catalog
    This class behaves like an immutable dictionary with the datasource ids as keys
    """

    @classmethod
    def _init(cls, connector):
        meta = connector._metadata()
        if resp.status_code == 404:
            raise DatasourceException("Not found")
        elif meta.status_code == 401:
            raise DatasourceException("Not Authorized")
        elif meta.status_code != 200:
            raise DatameshException(meta.text)
        cat = cls(meta.json)
        cat._connector = connector
        return ds

    def __init__(self, json):
        """Constructor for Catalog class"""
        self._geojson = FeatureCollection(json)
        self._ids = [ds["id"] for ds in self._geojson.features]

    def __len__(self):
        return len(self._geojson["features"])

    def __str__(self):
        datasources = [
            f" {ds['properties']['name']} [{ds['id']}]"
            for ds in self._geojson["features"]
        ]
        return (
            f"Datamesh catalog with {len(datasources)} datasources:\n"
            + datasources.join("\n")
        )

    def __repr__(self):
        return self._geojson.json()

    def __getitem__(self, item):
        if item in self._ids:
            return Datasource._init(self._connector, item)
        else:
            raise IndexError(f"Datasource {item} not in catalog")

    def __setitem__(self, item):
        raise ValueError("Datamesh catalog is read only")

    @property
    def ids(self):
        """Return a list of datasource ids"""
        return self._ids

    def keys(self):
        """Return a list of datasource ids"""
        return self._ids

    def load(self, id):
        """Load datasource"""
        return Datasource._init(self._connector, item).load()

    def query(self, query):
        """Make a query on the catalog

        Args:
            query (Union[:obj:`oceanum.datamesh.Query`, dict]): Datamesh query as a query object or a valid query dictionary

        Returns:
            Union[:obj:`pandas.DataFrame`, :obj:`geopandas.GeoDataFrame`, :obj:`xarray.Dataset`]: The datasource container

        Raises:
            IndexError: Datasource not in catalog
        """
        if query["datasource"] not in self.ids:
            raise IndexError(f"Datasource {query['datasource']} not in catalog")
        else:
            return self._connection.query(query)
