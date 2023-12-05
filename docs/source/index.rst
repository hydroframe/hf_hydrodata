HydroData
=======================================
The HydroData data catalog, associated python functions ``hf_hydrodata``, and API are products of the [HydroFrame project](https://hydroframe.org) and are designed to provide easy access to national hydrologic simulations generated using the National ParFlow model ([ParFlow-CONUS1](https://hydroframe.org/parflow-conus1) and [ParFlow-CONUS2](https://hydroframe.org/parflow-conus2)) as well as a variety of other gridded model input datasets and point observations.   Some of the datasets provided here are direct observations (e.g. USGS streamflow observations) while other are model outputs (e.g. ParFlow-CONUS2) or data products (e.g. remote sensing products). 

This documentation provides: 
1. Details on all of the datasets and variables that are available in the HydroData catalog
2. A set of tools and examples for how to access and work with this data for your own purposes. 

Please note that to access any data you will need to first sign up for an account and get an API pin (this is summarized in the :ref:`getting_started` section). Please also refer to our data use policy :ref:`data_use_policy` and make sure to cite any data sources that you use (examples of how to obtain citations are provided in the how to sections).

:ref:`getting_started` contains installation instructions and a quick-start guide for obtaining 
gridded datasets and point observations. The sections on :ref:`available_data`, :ref:`gridded_data_index` 
and :ref:`point_data_index` provide more detail on the datasets and fields that are available. 
Full example jupyter workbook workflows for working with point observations are in :ref:`examples_index` and 
the :ref:`api` contains the full list of available functions.

.. toctree::
   :maxdepth: 2
   :caption: Table of Contents

   getting_started
   available_data
   gridded_data/index
   point_data/index
   api_reference
