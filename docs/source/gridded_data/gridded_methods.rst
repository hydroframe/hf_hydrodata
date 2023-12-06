.. _gridded_methods:

``get_gridded_data``
-----------------------
The get_gridded_data method returns a numpy ndarray of the information
identified by the filter attributes passed to the function.

.. code-block:: python

      import hf_hydrodata as hf

      options = {
            "dataset": "NLDAS2", "variable": "precipitation", "temporal_resolution": "hourly",
            "start_time": "2005-10-1", "end_time": "2005-10-2", "grid_bounds": [100, 100, 200, 200]
      }
      data = hf.get_gridded_data(options)

``get_raw_file``
-------------------
The get_raw_file method returns the raw file from the server that
is idenfied by the filter attributes.

.. code-block:: python

    import hf_hydrodata as hf

    options = {
        "dataset": "huc_mapping", "grid": "conus2", "level": "4"}
    }
    hf.get_raw_file("huc4.tiff", options)

``get_date_range``
--------------------
The get_date_range method returns an array of [start_date, end_date] 
of the range of dates available for the dataset identified by
the same filter attributes passed to get_gridded_data.

.. code-block:: python

    import hf_hydrodata as hf
    options = {"dataset": "NLDAS2", "temporal_resolution": "daily", "variable": "precipitation",
            "start_time":"2005-09-30", "end_time":"2005-10-03",
            "grid_bounds":[200, 200, 300, 250]
    }
    range = hf.get_date_range(options)

``get_catalog_entry``
-----------------------
The get_catalog_entry method returns a python dict with attributes
describing the data identified by
the same filter attributes passed to get_gridded_data.

.. code-block:: python

    import hf_hydrodata as hf

    options = {
        "dataset": "NLDAS2", "temporal_resolution": "daily",
        "variable": "precipitation", "start_time": "2005-7-1"
    }
    entry = hf.get_catalog_entry(options)

``get_catalog_enties``
-----------------------
The get_catalog_enties method returns an array of catalog entries
filtered by the attributes passed to the function.

.. code-block:: python

    import hf_hydrodata as hf

    entries = hf.get_catalog_entries(dataset="NLDAS2", temporal_resolution="daily")

    options = {"dataset": "NLDAS2", "temporal_resolution": "daily"}
    entries = hf.get_catalog_entries(options)
    assert len(entries) == 20
    entry = entries[0]
    assert entry["dataset"] == "NLDAS2"    

``get_citations``
-----------------
Get a citations string about a dataset.

    .. code-block:: python

        import hf_hydrodata as hf  

        citations = hf.get_citations("NLDAS2")      
        print(citations)

        citations = hf.get_citations(dataset = "NLDAS2")
        print(citations)

        options = {"dataset": "NLDAS2", "temporal_resolution": "daily"}
        citations = hf.get_citations(options)
        
``get_datasets``
------------------
The get_datasets method returns an array of dataset names available
to be specified as the "dataset" filter attribute. Any filter
attribute can be passed as arguments to return only datasets with
data matching the filter attributes.

.. code-block:: python

    import hf_hydrodata as hf

    datasets = hf.get_datasets()
    assert len(datasets) == 13
    assert datasets[0] == "CW3E"

    datasets = hf.get_datasets(variable = "air_temp")
    assert len(datasets) == 5
    assert datasets[0] == "CW3E"

    datasets = hf.get_datasets(grid = "conus2")
    assert len(datasets) == 5
    assert datasets[0] == "CW3E"

    options = {"variable": "air_temp", "grid": "conus1"}
    datasets = hf.get_datasets(options)
    assert len(datasets) == 3
    assert datasets[0] == "NLDAS2"    

``get_variables``
------------------
The get_datasets method returns an array of variable names available
to be specified as the "variable" filter attribute.
Any filter
attribute can be passed as arguments to return only variables with
data matching the filter attributes.

.. code-block:: python

    import hf_hydrodata as hf

    variables = hf.get_variables()
    assert len(variables) == 63
    assert variables[0] == "air_temp"

    variables = hf.get_variables(dataset = "CW3E")
    assert len(variables) == 8
    assert variables[0] == "air_temp"

    variables = hf.get_variables(grid = "conus2")
    assert len(variables) == 30
    assert variables[0] == "air_temp"

    options = {"dataset": "NLDAS2", "grid": "conus1"}
    variables = hf.get_variables(options)
    assert len(variables) == 8
    assert variables[0] == "air_temp"    

``from_latlon``
-------------------
This converts a lat/lon point or an array of lat/lon points to grid coordiates.

.. code-block:: python

    import hf_hydrodata as hf

    (x, y) = hf.from_latlon("conus1", 31.759219, -115.902573)
    latlon_bounds = hf.from_latlon("conus1", *[31.651836, -115.982367, 31.759219, -115.902573])
    
``to_latlon``
----------------
This converts an x,y grid point or an array of x,y grid points to lat/lon coordinates.

.. code-block:: python

    import hf_hydrodata as hf

    (lat, lon) = hf.to_latlon("conus1", 10, 10)
    latlon_bounds = hf.to_latlon("conus1", *[0, 0, 20, 20])
    (lat, lon) = hf.to_latlon("conus1", 10.5, 10.5)

``get_huc_from_xy``
--------------------
This returns a HUC id that contains an x,y grid point.

.. code-block:: python

    import hf_hydrodata as hf

    huc_id = hf.get_huc_from_xy("conus1", 6, 300, 100)
    assert huc_id == "181001"

``get_huc_from_latlon``
------------------------
This returns a HUC id from a lat/lon coordinate.

.. code-block:: python

    import hf_hydrodata as hf

    huc_id = hf.get_huc_from_latlon("conus1", 6, 34.48, -115.63)
    assert huc_id == "181001"

``get_huc_bbox``
-----------------
This returns the bounding box of a list of HUC ids in grid coordinates.

.. code-block:: python

    import hf_hydrodata as hf

    bbox = hf.get_huc_bbox("conus1", ["181001"])
    assert bbox == (1, 167, 180, 378)
