from dateuitl.parser import parse
import datetime


class Datasource(object):
    @classmethod()
    def init(connector, id):
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
            tend (_type_, optional): Latest time in datasource. Must be a valid ISO8601 datetime string or None. Defaults to None.
            schema (dict, optional): Datasource schema. Defaults to {}.
            coordinates (dict, optional): Coordinates key. Defaults to {}.
            tags (list, optional): List of keyword tags. Defaults to [].
            links (list, optional): List of additional external URL links. Defaults to [].
            info (dict, optional): Dictionary of additional information. Defaults to {}.
            details (string, optional): URL link to additional details. Defaults to None.
            last_modified (string, optional): Latest time datasource metadata was modified. Must be a valid ISO8601 datetime string or None. Defaults to None.
        """
        self._geometry = geometry
        self.name = name or datasource_id
        self.description = description
        self._tstart = tstart
        self._tend = tend
        self._schema = schema
        self._coordinates = coordinates
        self.tags = tags
        self.links = links
        self.info = info
        self.details = details
        self._last_modified = last_modified or datetime.datetime.utcnow()
        self._connector = None

    @property
    def tstart(self):
        return parse(self._tstart)

    @property
    def tend(self):
        return parse(self._tend) if self._tend else None