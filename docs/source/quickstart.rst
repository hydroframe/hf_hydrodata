.. _quickstart:

QuickStart Guide
=======================================
The ``hf_hydrodata`` package supports both gridded and point observations data. The examples below contain 
some simple syntax for getting started. Further developed examples are available at :ref:`examples_index`. 
Additionally, the :ref:`api` can be used to see the full list of available features. 

Please see :ref:`getting_started` to make sure you have properly signed up and registered your API PIN.

Accessing Gridded Data
------------------------

The example below will return daily NLDAS2 precipitation files for March 1, 2005. Without specification,
gridded data will be returned on the CONUS1 grid (citation?/reference?) but a different grid or grid subset
can be specified by the user. Please see :ref:`api` for a full list of available parameters and supported
features.

The user can also request the metadata for the specified file. This includes information on the 
variable units, time zone, overall time availability for this data source, any relevant DOI citations,
and many other fields. A full description of the metadata returned can be found in :ref:`gridded_data_index` ::

    import hf_hydrodata as hf

    # Define filters and return as NumPy array
    filters = {"dataset":"NLDAS2", "variable":"precipitation", "temporal_resolution":"daily", "start_time": "2005-03-01"}
    data = hf.get_gridded_data(filters)
    print(data.shape)

    # Get the metadata about the returned data
    metadata = hf.get_catalog_entry(filters)
    print(metadata)

Many of the files are very large so parameters can be provided to subset the files by space and/or time before
returning the data. See the sub-section :ref:`gridded_data_index` for details about the available parameters
that can be passed to the functions to filter data by space and/or time.


Accessing Point Observations
------------------------------
``hf_hydrodata`` supports access to a collection of site-level data from a variety of sources. 
Please see :ref:`data_collection` for a full list of what is available and details on our 
data collection process.

The below syntax will return daily USGS streamflow data from January 1, 2022 through January 5, 2022 
for sites that are within the bounding box with latitude bounds of (45, 50) and longitude bounds
of (-75, -50). :: 

    # Import package
    import hf_hydrodata as hf

    # Define filters and return as pandas DataFrame
    data_source = 'usgs_nwis'
    variable = 'streamflow'
    temporal_resolution = 'daily'
    aggregation = 'average'

    data = hf.get_point_data(data_source, variable, temporal_resolution, aggregation,
                    start_date="2022-01-01", end_date="2022-01-05", 
                    latitude_range = (45, 50),
                    longitude_range = (-75, -50))
    data.head(5)

    # Get the metadata about the sites with returned data
    metadata = hf.get_point_metadata(data_source, variable, temporal_resolution, aggregation,
                            start_date="2022-01-01", end_date="2022-01-05", 
                            latitude_range = (45, 50),
                            longitude_range = (-75, -50))
    metadata.head(5)

Please see :ref:`examples_index` for additional example workflows.