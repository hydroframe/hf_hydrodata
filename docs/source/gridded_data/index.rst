.. _gridded_data_index:

Working with Gridded Data
==================================

The ``hf_hydrodata`` module provides functionality to get gridded data and metadata from the hydrodata data store.
This is a list of the most useful functions for working with gridded data.
See the Python API Reference for details about each function.

Gridded Observation Methods
-----------------

.. toctree::
   :maxdepth: 1
   :titlesonly:

   gridded_methods

Filter Parameters
-----------------
Most gridded functions accept filter parameters that identify data using
attributes that index the data. These filter parameters may be passed by name or using a python dict with filter parameters options.
All the files are organized by dataset and variable (see ``Available Datasets and Data Products``).

You can get the available datasets and variables using functions:

.. code-block:: python

      import hf_hydrodata as hf

      datasets = hf.get_datasets()
      variables = hf.get_variables()

You can pass filter parameter using named parameters.

.. code-block:: python

      datasets = hf.get_datasets(variable = "air_temp")

You can use pass filter parameters as dict options as well.

.. code-block:: python

      options = {"dataset": "NLDAS2", "grid": "conus1"}
      variables = hf.get_variables(options)

Get Gridded Data
--------
You can get data from hydrodata as a numpy array using the get_gridded_data() function.

.. code-block:: python

      import hf_hydrodata as hf

      options = {
            "dataset": "NLDAS2", "variable": "precipitation", "temporal_resolution": "hourly",
            "start_time": "2005-10-1", "end_time": "2005-10-2", "grid_bounds": [100, 100, 200, 200]
      }
      data = hf.get_gridded_data(options)

See the Python API Reference or the Available Datasets and Data Products for a list of all the filter parameters that may be provided.

Get Meta Data
-------------
You can get metadata about the files in hydrodata.

.. code-block:: python

      import hf_hydrodata as hf

      options = {
         "dataset": "NLDAS2", "variable": "precipitation", "period": "hourly",
         "start_time": "2005-10-1", "end_time": "2005-11-1"
      }
      metadata = hf.get_catalog_entry(options)
      print(metadata["units"], metadata["paper_dois"], metadata["grid"], metadata["description"])

You can get the date range available for a data set.

.. code-block:: python

      (start_date, end_date) = hf.get_date_range(options)







.. toctree::
   :maxdepth: 1
