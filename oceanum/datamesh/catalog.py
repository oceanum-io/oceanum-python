from geojson_pydantic import Feature, FeatureCollection


class Catalog(object):
    """Datamesh catalog"""

    def __init__(self, json):
        """Constructor for Catalog class"""
        self._geojson = FeatureCollection(json)
