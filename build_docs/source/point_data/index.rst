.. _point_data_index:

Point Data
======================

The ``hf_hydrodata.point`` module provides functionality to compile site-level observations data for a 
variety of hydrologic variables. All source data comes from public sources that have been compiled in order
for users to access the data with a single common Python syntax. Please see :ref:`point_inputs` for a list
of the available parameters.

Note that raw hourly data is saved in UTC while raw daily data is saved with respect to the local site time zone. 

*Coming soon*: the ability for a user to specify whether data gets returned in UTC or local time, regardless of 
how the raw data is structured.

.. toctree::
   :maxdepth: 1

   point_inputs
   data_collection
   metadata_definitions