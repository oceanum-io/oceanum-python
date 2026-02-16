=====
Usage
=====

To use oceanum in a project::

    import oceanum

Or to import a subpackage::

    import oceanum.datamesh as datamesh


Work with Datamesh
------------------

Initialising the Connector
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The :class:`~oceanum.datamesh.Connector` is the main entry point for all datamesh operations.
You need a valid datamesh token to create a connector, which you can get from https://home.oceanum.io/account/.

Pass the token directly::

    from oceanum.datamesh import Connector
    datamesh = Connector(token="your_datamesh_token")

Or set the ``DATAMESH_TOKEN`` environment variable and omit the token argument::

    export DATAMESH_TOKEN=your_datamesh_token

::

    datamesh = Connector()

You can also specify optional parameters (see :meth:`~oceanum.datamesh.Connector.__init__` for full details)::

    datamesh = Connector(
        token="your_datamesh_token",
        session_duration=7200,  # Session length in seconds (default 3600)
        verify=False,           # Disable SSL verification if needed
    )


Browsing the Catalog
~~~~~~~~~~~~~~~~~~~~

Use :meth:`~oceanum.datamesh.Connector.get_catalog` to retrieve a
:class:`~oceanum.datamesh.Catalog` of all datasources available to you::

    cat = datamesh.get_catalog()
    print(cat)

The :class:`~oceanum.datamesh.Catalog` behaves like an immutable dictionary
with datasource IDs as keys. Each entry is a :class:`~oceanum.datamesh.Datasource`::

    # List all datasource IDs
    print(cat.ids)

    # Access a specific datasource from the catalog
    dsrc = cat["oceanum_wave_glob05_era5_v1_grid"]

You can filter the catalog using search terms, time ranges and spatial extents.
Time and spatial filters accept :class:`~oceanum.datamesh.query.TimeFilter` and
:class:`~oceanum.datamesh.query.GeoFilter` objects or shorthand forms::

    # Filter by search term
    cat = datamesh.get_catalog(search="wave")

    # Filter by time range
    cat = datamesh.get_catalog(timefilter=["2020-01-01", "2021-01-01"])

    # Filter by bounding box (as a shapely geometry)
    import shapely
    bbox = shapely.geometry.box(165, -48, 180, -34)
    cat = datamesh.get_catalog(geofilter=bbox)

    # Limit the number of results
    cat = datamesh.get_catalog(search="wave", limit=10)


Inspecting a Datasource
~~~~~~~~~~~~~~~~~~~~~~~~

Use :meth:`~oceanum.datamesh.Connector.get_datasource` to get detailed metadata
for a specific datasource as a :class:`~oceanum.datamesh.Datasource` instance::

    dsrc = datamesh.get_datasource("oceanum_wave_glob05_era5_v1_grid")
    print(dsrc)

Inspect the variables and attributes::

    print(dsrc.variables)
    print(dsrc.attributes)

Check the spatial and temporal extent::

    print(dsrc.bounds)
    print(dsrc.tstart, dsrc.tend)


Loading a Datasource
~~~~~~~~~~~~~~~~~~~~

Use :meth:`~oceanum.datamesh.Connector.load_datasource` to load the full datasource
into memory. The return type depends on the datasource --
an ``xarray.Dataset``, a ``pandas.DataFrame`` or a ``geopandas.GeoDataFrame``::

    ds = datamesh.load_datasource("oceanum_wave_glob05_era5_v1_grid")

For large gridded datasources, use dask-backed lazy loading::

    ds = datamesh.load_datasource("oceanum_wave_glob05_era5_v1_grid", use_dask=True)

Plot a timeseries from the dataset::

    ds["hs"].sel(longitude=0, latitude=0).plot()


Querying Data
~~~~~~~~~~~~~

Use :meth:`~oceanum.datamesh.Connector.query` to subset and transform data
server-side before downloading. The query can be passed as a
:class:`~oceanum.datamesh.Query` object, a dictionary, or as keyword arguments.

**Basic query with time and spatial filters**

Uses :class:`~oceanum.datamesh.query.TimeFilter` and
:class:`~oceanum.datamesh.query.GeoFilter` in dictionary form::

    result = datamesh.query(
        datasource="oceanum_wave_glob05_era5_v1_grid",
        variables=["hs", "dp"],
        timefilter={
            "times": ["2010-01-01", "2011-01-01"]
        },
        geofilter={
            "type": "feature",
            "geom": {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [170.2, -35.3]},
                "properties": {}
            }
        }
    )
    result["dp"].plot()

**Query with a bounding box**::

    result = datamesh.query(
        datasource="oceanum_wave_glob05_era5_v1_grid",
        variables=["hs"],
        geofilter={
            "type": "bbox",
            "geom": [165, -48, 180, -34]
        },
        timefilter={
            "times": ["2020-01-01", "2020-02-01"]
        }
    )

**Using** :class:`~oceanum.datamesh.Query` **objects** for more control::

    from oceanum.datamesh import Query

    q = Query(
        datasource="oceanum_wave_glob05_era5_v1_grid",
        variables=["hs"],
        timefilter={
            "type": "series",
            "times": ["2020-01-15", "2020-02-15", "2020-03-15"]
        },
        geofilter={
            "type": "feature",
            "geom": {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [174.8, -41.3]},
                "properties": {}
            }
        }
    )
    result = datamesh.query(q)

**Local query caching** to avoid repeated downloads::

    # Cache results for 1 hour (3600 seconds)
    result = datamesh.query(
        datasource="oceanum_wave_glob05_era5_v1_grid",
        variables=["hs"],
        timefilter={"times": ["2020-01-01", "2020-02-01"]},
        cache_timeout=3600
    )


Writing Data
~~~~~~~~~~~~

Use :meth:`~oceanum.datamesh.Connector.write_datasource` to write data to
datamesh from an ``xarray.Dataset``, a ``pandas.DataFrame`` or a
``geopandas.GeoDataFrame``. The datasource ID must only contain lowercase
letters, numbers, dashes and underscores. The method returns a
:class:`~oceanum.datamesh.Datasource` instance representing the written datasource.

**Writing an xarray Dataset**::

    import xarray as xr
    import numpy as np
    import pandas as pd

    ds = xr.Dataset(
        {"temperature": (["time", "latitude", "longitude"], np.random.rand(10, 5, 5))},
        coords={
            "time": pd.date_range("2020-01-01", periods=10),
            "latitude": np.linspace(-40, -35, 5),
            "longitude": np.linspace(170, 175, 5),
        }
    )
    datamesh.write_datasource("my_temperature_data", ds)

Coordinates, geometry and time range are automatically inferred from the data
when possible. You can also specify them explicitly::

    import shapely

    datamesh.write_datasource(
        "my_temperature_data",
        ds,
        name="My Temperature Data",
        description="Gridded temperature observations",
        geom=shapely.geometry.box(170, -40, 175, -35),
        tags=["temperature", "observations"],
    )

**Writing a pandas DataFrame**::

    import pandas as pd

    df = pd.DataFrame({
        "time": pd.date_range("2020-01-01", periods=100, freq="h"),
        "temperature": np.random.rand(100),
        "pressure": np.random.rand(100),
    }).set_index("time")

    datamesh.write_datasource("my_station_data", df)

**Writing a GeoDataFrame**::

    import geopandas as gpd
    from shapely.geometry import Point

    gdf = gpd.GeoDataFrame(
        {"name": ["Auckland", "Wellington"], "population": [1657000, 215400]},
        geometry=[Point(174.76, -36.85), Point(174.78, -41.29)],
        crs="EPSG:4326",
    )
    datamesh.write_datasource("nz_cities", gdf)

**Appending data** along a coordinate (e.g. extending a time series)::

    datamesh.write_datasource("my_temperature_data", new_ds, append="time")

**Overwriting** an existing datasource completely::

    datamesh.write_datasource("my_temperature_data", ds, overwrite=True)

**Writing data in a non-WGS84 CRS** -- the geometry and data are transformed
automatically::

    datamesh.write_datasource(
        "my_projected_data",
        ds,
        geom=projected_bbox,
        crs="EPSG:2193",
    )

**Updating metadata only** without changing the stored data, using
:meth:`~oceanum.datamesh.Connector.update_metadata`::

    datamesh.update_metadata(
        "my_temperature_data",
        description="Updated temperature observations",
        tags=["temperature", "observations"],
    )


Deleting a Datasource
~~~~~~~~~~~~~~~~~~~~~~

Use :meth:`~oceanum.datamesh.Connector.delete_datasource` to delete a datasource
and all its stored data::

    datamesh.delete_datasource("my_temperature_data")


Async Operations
~~~~~~~~~~~~~~~~

Most :class:`~oceanum.datamesh.Connector` methods have async variants for use
in asynchronous workflows::

    cat = await datamesh.get_catalog_async()
    dsrc = await datamesh.get_datasource_async("oceanum_wave_glob05_era5_v1_grid")
    ds = await datamesh.load_datasource_async("oceanum_wave_glob05_era5_v1_grid")
    result = await datamesh.query_async(query)
    await datamesh.write_datasource_async("my_data", data)
    await datamesh.delete_datasource_async("my_data")


Work with Storage
-----------------

The :class:`~oceanum.storage.FileSystem` provides cloud storage access following the
`fsspec <https://filesystem-spec.readthedocs.io/>`_ specification.

Initialising the FileSystem
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Create a :class:`~oceanum.storage.FileSystem` with your token::

    from oceanum.storage import FileSystem
    fs = FileSystem(token="your_datamesh_token")

Or use the ``DATAMESH_TOKEN`` environment variable::

    fs = FileSystem()

You can also use fsspec's protocol-based access with the ``oceanum://`` protocol::

    import fsspec
    of = fsspec.open("oceanum://myfolder/myfile.txt", token="your_datamesh_token")

Listing and Navigating
~~~~~~~~~~~~~~~~~~~~~~

List contents of a directory with :meth:`~oceanum.storage.FileSystem.ls`::

    contents = fs.ls("/myfolder")

    # List with detailed info
    contents = fs.ls("/myfolder", detail=True)

Check if a path exists or is a file/directory with
:meth:`~oceanum.storage.FileSystem.exists`,
:meth:`~oceanum.storage.FileSystem.isfile` and
:meth:`~oceanum.storage.FileSystem.isdir`::

    fs.exists("/myfolder/myfile.txt")
    fs.isfile("/myfolder/myfile.txt")
    fs.isdir("/myfolder")

Uploading and Downloading
~~~~~~~~~~~~~~~~~~~~~~~~~

Download a file from storage with :meth:`~oceanum.storage.FileSystem.get`::

    fs.get("/myfolder/myfile.txt", "local_file.txt")

Upload a file to storage with :meth:`~oceanum.storage.FileSystem.put`::

    fs.put("local_file.txt", "/myfolder/myfile.txt")

Read file contents directly with :meth:`~oceanum.storage.FileSystem.cat`::

    data = fs.cat("/myfolder/myfile.txt")

Managing Files and Directories
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Create a directory with :meth:`~oceanum.storage.FileSystem.mkdir`::

    fs.mkdir("/myfolder/newdir")

Copy a file within storage with :meth:`~oceanum.storage.FileSystem.cp`::

    fs.cp("/myfolder/source.txt", "/myfolder/dest.txt")

Remove a file or directory with :meth:`~oceanum.storage.FileSystem.rm`::

    fs.rm("/myfolder/myfile.txt")

    # Remove a directory recursively
    fs.rm("/myfolder/olddir", recursive=True)

Convenience Functions
~~~~~~~~~~~~~~~~~~~~~

The storage module also provides standalone convenience functions
(:func:`~oceanum.storage.filesystem.ls`, :func:`~oceanum.storage.filesystem.get`,
:func:`~oceanum.storage.filesystem.put`, :func:`~oceanum.storage.filesystem.rm`,
:func:`~oceanum.storage.filesystem.exists`)::

    from oceanum.storage import ls, get, put, rm, exists

    # List storage contents
    contents = ls("/myfolder", recursive=False)

    # Download a file
    get("/myfolder/myfile.txt", "./local_copy.txt")

    # Upload a file
    put("./local_file.txt", "/myfolder/remote_file.txt")

    # Remove a file
    rm("/myfolder/old_file.txt")

    # Check if a path exists
    exists("/myfolder/myfile.txt")
