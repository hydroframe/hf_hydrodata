.. __quickstart:

QuickStart to Accessing Data
=======================================
The hf_hydrodata package supports both gridded and point observations data. The below examples contain 
quick syntax for getting started. Further developed examples are available at :ref:`examples_index` and
the :ref:`api` can be used to see the full list of available features. 


Accessing Gridded Data
------------------------
You can read data from any file in the hydrodata repository using python to get a numpy array with::

    from hf_hydrodata.gridded import get_numpy,get_catalog_entry

    filters = {"dataset":"NLDAS2", "variable":"precipitation", "period":"daily", "start_time": "2005-03-01"}
    data = get_numpy(filters)
    print(data.shape)

    # Get the metadata about the returned data
    metadata = get_catalog_entry(filters)
    print(metadata)

Many of the files are very large so parameters can be provided to subset the files by space and/or time before
returning the data. See the sub-section about "Gridded Data" for details about the available parameters
that can be passed to the functions to filter data by space and/or time.

Accessing Point Data
------------------------