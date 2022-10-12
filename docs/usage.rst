=====
Usage
=====

To use oceanum in a project::

    import oceanum

Or to import a subpackage::

    import oceanum.datamesh as datamesh


Work with Datamesh
------------------

Import and instantiate the Datamesh connector::

    from oceanum.datamesh import Connector
    datamesh=Connector(token=<your_datamesh_token>)


Get your Datamesh token from: https://home.oceanum.io/account/

Get and list a catalog of all datasources::

    cat=datamesh.get_catalog()
    print(cat)

Get more info on a datasource::

    dsrc=datamesh.get_datasource('oceanum_wave_glob05_era5_v1_grid')
    print(dsrc)

Look at the variables schema::

    print(dsrc.variables)


Load the datasource as an xarray dataset::

    ds=datamesh.load_datasource('oceanum_wave_glob05_era5_v1_grid')

Plot a timeseries from the datsource::

    ds['hs'].sel(longitude=0, latitude=0).plot()

Make a query using a timefilter and geofilter::

    dsq=datamesh.query({"datasource":"oceanum_wave_glob05_era5_v1_grid",
                        "variables":["hs","dp"],
                        "timefilter":{"times":["2010-01-01","2011-01-01"]},
                        "geofilter":{
                            "type":"feature",
                            "geom":{
                                "type":"Feature","geometry":{"type":"Point","coordinates":[170.2,-35.3]}
                            }
                        }
                   })
    dsq['dp'].plot()

