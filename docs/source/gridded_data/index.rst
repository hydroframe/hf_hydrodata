.. _gridded_data_index:

Working with Gridded Data
==================================

The ``hf_hydrodata`` module provides functionality to get gridded data and metadata from the HydroData data catalog.
This is a list of the most useful functions for working with gridded data.
See the :ref:`api` for details about each function.

Gridded Observation Methods
-----------------------------

.. toctree::
   :maxdepth: 1
   :titlesonly:

   gridded_methods

Filter Parameters
-----------------
Most gridded functions accept filter parameters that identify data using
attributes that index the data. These filter parameters may be passed by name or using a python dict with filter parameters options.
All the files are organized by dataset and variable (see :ref:`available_datasets`).

You can get the available datasets and variables using functions:

.. code-block:: python

      import hf_hydrodata as hf

      datasets = hf.get_datasets()
      variables = hf.get_variables()

You can pass filter parameter using named parameters.

.. code-block:: python

      datasets = hf.get_datasets(variable = "air_temp")

You use pass filter parameters as dict options as well.

.. code-block:: python

      options = {"dataset": "NLDAS2", "grid": "conus1"}
      variables = hf.get_variables(options)

See the :ref:`api` or :ref:`available_datasets` for a list of all the filter parameters that may be provided.

Dataset Versions
^^^^^^^^^^^^^^^^^
There are sometimes multiple versions of the same dataset that are available. When this is the case, the default function call
will return the latest version of the dataset that is available. However, a user may explicitly specify which version of the dataset
they want using the ``dataset_version`` filter parameter. An example of how different versions of the
`CW3E dataset <https://hf-hydrodata.readthedocs.io/en/latest/gen_CW3E.html>`_ get returned is shown in the code block below. 

.. code-block:: python

      import hf_hydrodata as hf

      # This function call will return version 0.9 of the CW3E dataset
      options = {
            "dataset": "CW3E", "variable": "precipitation",
            "temporal_resolution": "hourly",
            "start_time": "2001-10-01", "end_time": "2001-10-02",
            "grid_bounds": [3660, 1657, 3732, 1837],
            "grid": "conus2",
            "dataset_version": "0.9"
      }
      data = hf.get_gridded_data(options)

      # This function call will explicitly return the new version 1.0 CW3E dataset
      options = {
            "dataset": "CW3E", "variable": "precipitation",
            "temporal_resolution": "hourly",
            "start_time": "2001-10-01", "end_time": "2001-10-02",
            "grid_bounds": [3660, 1657, 3732, 1837],
            "grid": "conus2",
            "dataset_version": "1.0"
      }
      data = hf.get_gridded_data(options)

      # If NO dataset_version parameter is specified, this function call will implicitly
      # return the latest version of the CW3E dataset, version 1.0
      options = {
            "dataset": "CW3E", "variable": "precipitation",
            "temporal_resolution": "hourly",
            "start_time": "2001-10-01", "end_time": "2001-10-02",
            "grid_bounds": [3660, 1657, 3732, 1837],
            "grid": "conus2"
      }
      data = hf.get_gridded_data(options)


Metadata Descriptions
-----------------------
You can get metadata about the files in hydrodata using the ``get_catalog_entry`` function.

.. code-block:: python

      import hf_hydrodata as hf

      options = {
         "dataset": "NLDAS2", "variable": "precipitation", "period": "hourly",
         "start_time": "2005-10-1", "end_time": "2005-11-1"
      }
      metadata = hf.get_catalog_entry(options)
      print(metadata["units"], metadata["paper_dois"], metadata["grid"], metadata["description"])

See :ref:`available_metadata` for documentation of the metadata returned by ``get_catalog_entry``.

You can get the date range available for a data set using the ``get_date_range`` function.

.. code-block:: python

      (start_date, end_date) = hf.get_date_range(options)







.. toctree::
   :maxdepth: 1
