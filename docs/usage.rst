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


Errors and Retries
~~~~~~~~~~~~~~~~~~

Errors raised by datamesh *requests* inherit from
:class:`~oceanum.datamesh.DatameshError`, so one ``except`` clause covers them::

    from oceanum.datamesh import Connector, DatameshError

    try:
        ds = datamesh.query(query)
    except DatameshError as e:
        print(f"datamesh request failed: {e}")

Two things this does **not** catch, both raised before any request is made:

* :class:`ValueError` -- constructing a :class:`~oceanum.datamesh.Connector`
  with no token, or an invalid ``session_duration``.
* pydantic ``ValidationError`` -- a malformed query. ``query()`` builds a
  :class:`~oceanum.datamesh.Query` from a dict, so an invalid field is rejected
  locally rather than by the server.

Catch those separately if you accept queries from outside your own code.

What the exceptions mean
^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
   :widths: 28 72
   :header-rows: 1

   * - Exception
     - What it means
   * - :class:`~oceanum.datamesh.DatameshQueryError`
     - The query was rejected. Re-running it unchanged will fail the same way.
   * - :class:`~oceanum.datamesh.DatameshUnavailableError`
     - A service was reachable but could not serve the request. May succeed
       later, but **not immediately** -- see ``retry_after`` below.
   * - :class:`~oceanum.datamesh.DatameshConnectError`
     - The service could not be reached, or the request failed in transport.
   * - :class:`~oceanum.datamesh.DatameshWriteError`
     - A write to a datasource failed.
   * - :class:`~oceanum.datamesh.DatameshSessionError`
     - A session could not be created, used or finalised.

``DatameshUnavailableError`` subclasses ``DatameshConnectError``, so existing
handlers keep working. Catch the more specific class first where the difference
matters.

.. note::

   This full taxonomy applies to :meth:`~oceanum.datamesh.Connector.query` and
   :meth:`~oceanum.datamesh.Connector.load_datasource`. Catalog and metadata
   calls -- :meth:`~oceanum.datamesh.Connector.get_catalog`,
   :meth:`~oceanum.datamesh.Connector.get_datasource` and the write-metadata
   paths -- are coarser: every response status of 400 or above raises a plain
   ``DatameshConnectError``, with ``retry_after`` unset. A
   ``DatameshUnavailableError`` handler will not fire for those, so catch
   ``DatameshConnectError`` if you need to cover them.

The client already retries
^^^^^^^^^^^^^^^^^^^^^^^^^^

**Do not wrap datamesh calls in a tight retry loop.** The client retries
internally, with bounded attempts and jittered backoff, and it deliberately does
*not* retry the cases where a retry cannot help or would make things worse:

* Failures that never reached the service are retried for any method.
* A request that may already have been delivered is **not** re-sent unless the
  method is idempotent. Re-running an expensive query that just exhausted a
  worker's memory would simply exhaust the next one.
* On the query path, a gateway failure waits 15--30 seconds before raising.
  That wait is deliberate: if the service is struggling, every client retrying
  in the same second is what keeps it down. Catalog and metadata calls are not
  paced this way -- they back off between attempts (up to 15 s) and then raise
  immediately, so a loop of your own around those can run hot.
* A rejected TLS certificate, and a query the server rejected, are terminal --
  no attempt is repeated.

If you do add your own retry, honour ``retry_after`` rather than a schedule of
your own::

    import time
    from oceanum.datamesh import DatameshUnavailableError, DatameshQueryError

    try:
        ds = datamesh.query(query)
    except DatameshQueryError:
        raise                                   # the query is wrong; fix it
    except DatameshUnavailableError as e:
        time.sleep(e.retry_after or 300)        # may be None on other paths
        ds = datamesh.query(query)

``retry_after`` is present on every datamesh exception, defaulting to ``None``.
``None`` means the client has no opinion -- it does **not** mean "retry now", so
guard the sleep as above. ``DatameshUnavailableError`` also carries
``status_code``.

One case deserves care: if a request exceeded what a single worker can process,
it is reported as unavailable and **will fail again however long you wait**. The
message says so. The fix is to make the request smaller -- a shorter time range,
a smaller area, or fewer variables -- not to repeat it.

Timeouts
^^^^^^^^

Timeouts are set per operation, because the work behind them differs by orders
of magnitude. All are seconds, and all can be overridden by environment
variable. Setting one to ``"None"`` disables it.

.. list-table::
   :widths: 40 12 48
   :header-rows: 1

   * - Variable
     - Default
     - Applies to
   * - ``DATAMESH_CONNECT_TIMEOUT``
     - 3.05
     - Establishing any connection
   * - ``DATAMESH_READ_TIMEOUT``
     - 10
     - Small JSON responses
   * - ``DATAMESH_METADATA_READ_TIMEOUT``
     - 20
     - Catalog search and datasource metadata
   * - ``DATAMESH_STAGE_READ_TIMEOUT``
     - 900
     - Resolving a query against the catalog
   * - ``DATAMESH_DOWNLOAD_TIMEOUT``
     - 900
     - Downloading a query result
   * - ``DATAMESH_CHUNK_BUDGET``
     - 900
     - The platform's own budget for generating one zarr chunk. Not a timeout
       itself -- it is the anchor the chunk read timeout is derived from.
   * - ``DATAMESH_CHUNK_READ_TIMEOUT``
     - 1020
     - One zarr chunk. Defaults to ``DATAMESH_CHUNK_BUDGET`` + 120, so setting
       the budget to ``"None"`` disables this timeout too.
   * - ``DATAMESH_CHUNK_WRITE_TIMEOUT``
     - 600
     - Writing one zarr chunk
   * - ``DATAMESH_WRITE_TIMEOUT``
     - ``None``
     - Datasource writes

A chunk read can legitimately block for the whole server-side generation of that
chunk, which is why its timeout is derived from the chunk budget rather than
picked independently. Lowering it below the budget makes the client give up on
work the platform is still doing: the read is abandoned with a
``DatameshConnectError`` on the first timeout, because a read timeout is not
retried.

Tuning the retry behaviour
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
   :widths: 40 12 48
   :header-rows: 1

   * - Variable
     - Default
     - Effect
   * - ``DATAMESH_QUERY_RETRIES``
     - 2
     - Total attempts at a query when the gateway reports 502. Set to ``1`` to
       surface the first failure immediately.
   * - ``DATAMESH_GATEWAY_RETRY_DELAY``
     - 30
     - Base wait before re-attempting or raising on a gateway failure; jittered.
   * - ``DATAMESH_GATEWAY_RETRY_MIN``
     - 5
     - Floor applied to a server-supplied ``Retry-After``. A server value is
       also capped at 120 s, so a longer one is reported and honoured as 120.
   * - ``DATAMESH_UNAVAILABLE_RETRY_AFTER``
     - 300
     - ``retry_after`` reported when the server gives no guidance.
   * - ``DATAMESH_CONNECTION_POOL_LIFETIME``
     - ``None``
     - Recycle pooled connections after this many seconds. Useful for
       long-running processes where an idle connection may be dropped silently
       by a load balancer between requests.


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

