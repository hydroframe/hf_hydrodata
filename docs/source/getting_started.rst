.. _getting_started:

Getting Started
======================

Installation
------------
The best way to install ``hf_hydrodata`` is using pip. This installs our 
latest stable release with fully-supported features. ::

    pip install hf_hydrodata

.. _api_pin:

Creating a HydroFrame API Account
----------------------------------
Users must create a HydroFrame API account and register their PIN before using the 
``hf_hydrodata`` package.

1. If it's your first time using this package you will need to sign up for a HydroFrame account on the `HydroFrame Signup Page <https://hydrogen.princeton.edu/signup>`_ (*Note: This only needs to be done once*)

2. Visit our `HydroFrame PIN Page <https://hydrogen.princeton.edu/pin>`_ to create a 4-digit PIN.

3. After creating your PIN, you must register that PIN on the machine that you intend
to use. You can run the following code one time to register your PIN::  

    import hf_hydrodata as hf

    hf.register_api_pin("<your_email>", "<your_pin>")

Your PIN will expire after 7 days of non-use. If your PIN expires, you must return to
the `HydroFrame PIN Page <https://hydrogen.princeton.edu/pin>`_ and create a new PIN. 
You only need to re-register this PIN with the ``register_api_pin`` method if the 
new 4-digit PIN is different from your previous 4-digit PIN (the PIN is allowed
to stay the same).

.. _quickstart:

QuickStart Guide
---------------------
The ``hf_hydrodata`` package supports both gridded and point observations data. The examples below contain 
some simple syntax for getting started. Further developed examples are available at :ref:`point_examples_index`
for accessing and working with point observations data. 
Additionally, the :ref:`api` can be used to see the full list of available features. 

Please see :ref:`api_pin` to make sure you have properly signed up and registered your API PIN.

Accessing Gridded Data
^^^^^^^^^^^^^^^^^^^^^^^^^

The example below will return daily NLDAS2 precipitation files for March 1, 2005. Without specification,
gridded data will be returned on the CONUS1 grid (`ParFlow-CONUS1 <https://hydroframe.org/parflow-conus1>`_) 
but a different grid or grid subset
can be specified by the user. Please see :ref:`api` for a full list of available parameters and supported
features.

The user can also request the metadata for the specified file. This includes information on the 
variable units, time zone, overall time availability for this data source, any relevant DOI citations,
and many other fields. A full description of the metadata returned can be found in :ref:`available_metadata`. ::

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
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
``hf_hydrodata`` supports access to a collection of site-level data from a variety of sources. 
Please see :ref:`available_datasets` for a full list of what is available and details on our 
data collection process.

The below syntax will return daily USGS streamflow data from January 1, 2022 through January 5, 2022 
for sites that are within the bounding box with latitude bounds of (45, 50) and longitude bounds
of (-75, -50). :: 

    # Import package
    import hf_hydrodata as hf

    data = hf.get_point_data(dataset = "usgs_nwis", variable = "streamflow", 
                             temporal_resolution = "daily", aggregation = "mean",
                             date_start = "2022-01-01", date_end = "2022-01-05", 
                             latitude_range = (45, 50), longitude_range = (-75, -50))

    # View first five records
    data.head(5)

    # Get the metadata about the sites with returned data
    metadata = hf.get_point_metadata(dataset = "usgs_nwis", variable = "streamflow", 
                                     temporal_resolution = "daily", aggregation = "mean",
                                     date_start = "2022-01-01", date_end = "2022-01-05", 
                                     latitude_range = (45, 50), longitude_range = (-75, -50))

    # View first five records
    metadata.head(5)


Alternately, it is possible to define a single dictionary containing all desired input parameter values,
and pass this dictionary in to the ``get_point_data`` and ``get_point_metadata`` functions. The example below
shows an alternate syntax to obtain identical output to the example shown above. ::

    # Import package
    import hf_hydrodata as hf

    # Define input parameters in a dictionary
    my_parameters = {"dataset": "usgs_nwis", "variable": "streamflow", "temporal_resolution": "daily",
                     "aggregation": "mean", "date_start": "2022-01-01", "date_end": "2022-01-05",
                     "latitude_range": (45, 50), "longitude_range": (-75, -50)}

    # Request point observations data and view first five records
    data = hf.get_point_data(my_parameters)
    data.head(5)

    # Request the metadata about the sites with returned data and view the first five records
    metadata = hf.get_point_metadata(my_parameters)
    metadata.head(5)


Please see :ref:`point_examples_index` for additional example workflows.