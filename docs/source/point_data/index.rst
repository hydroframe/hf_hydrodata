.. _point_data_index:

Working with Point Observations
==================================

The ``hf_hydrodata.point`` module provides tools to access point measurements of a variety of hydrologic variables 
from a multiple observation networks. All source data comes from public sources that have been compiled in order
for users to access the data with a single common Python syntax. Refer to :ref:`available_datasets` for the complete 
list of point observations datasets available and for more details on each dataset.  Please also see :ref:`point_methods` for 
descriptions of the available methods.

.. _point_methods:

Point Observation Methods
----------------------------

.. toctree::
   :maxdepth: 1
   :titlesonly:

   point_methods

Filter Parameters
-----------------
The point observation functions accept filter parameters that identify data using
attributes that index the data. These filter parameters may be passed by name or using a 
python dict with filter parameters options.
All the files are organized by dataset, variable, temporal_resolution, and aggregation (see :ref:`available_datasets`).

You can pass filter parameter using named parameters.

.. code-block:: python

      from hf_hydrodata import get_point_data
      
      data_df = get_point_data(dataset = "usgs_nwis", variable = "streamflow", 
                               temporal_resolution = "daily", aggregation = "mean",
                               start_date = "2022-01-01", end_date = "2022-01-05", 
                               latitude_range = (45, 50),
                               longitude_range = (-75, -50))

You use pass filter parameters as dict options as well.

.. code-block:: python

      from hf_hydrodata import get_point_data

      my_parameters = {"dataset": "usgs_nwis", "variable": "streamflow",
                       "temporal_resolution": "daily", "aggregation": "mean",
                       "start_date": "2022-01-01", "end_date": "2022-01-05",
                       "latitude_range": (45, 50), "longitude_range": (-75, -50)}

      data_df = get_point_data(my_parameters)

See the :ref:`api` and :ref:`available_datasets` for a list of all the filter parameters that may be provided.
The :ref:`point_examples_index` also provides a range of examples for querying point observations data.

Metadata Descriptions
-----------------------
You can get metadata about the point observations in hydrodata using the ``get_point_metadata`` function.

.. code-block:: python

      from hf_hydrodata import get_point_data

      my_parameters = {"dataset": "usgs_nwis", "variable": "streamflow",
                       "temporal_resolution": "daily", "aggregation": "mean",
                       "start_date": "2022-01-01", "end_date": "2022-01-05",
                       "latitude_range": (45, 50), "longitude_range": (-75, -50)}

      metadata_df = get_point_metadata(my_parameters)

See `Point Observations Metadata <https://hf-hydrodata.readthedocs.io/en/latest/available_metadata.html#point-observations-metadata>`_ 
for documentation of the metadata returned by ``get_point_metadata``.

.. _point_examples_index:

How To
----------------------------

.. toctree::
   :maxdepth: 2
   :titlesonly:

   examples/example_explore_data.ipynb
   examples/example_get_data.ipynb
   examples/example_site_networks.ipynb
   examples/example_shapefile.ipynb
   examples/example_plot_data.ipynb  
   examples/example_pandas.ipynb