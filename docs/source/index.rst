HydroData
=======================================
The HydroData data catalog, associated python functions ``hf_hydrodata``, and API are products of 
the `HydroFrame project <https://hydroframe.org>`_ and are designed to provide easy access to national 
hydrologic simulations generated using the National ParFlow model (`ParFlow-CONUS1 <https://hydroframe.org/parflow-conus1>`_ 
and `ParFlow-CONUS2 <https://hydroframe.org/parflow-conus2>`_) as well as a variety of other gridded model 
input datasets and point observations.   Some of the datasets provided here are direct observations 
(e.g. USGS streamflow observations) while other are model outputs (e.g. ParFlow-CONUS2) or data products 
(e.g. remote sensing products). 

This documentation provides: 
1. Details on all of the datasets and variables that are available in the HydroData catalog
2. A set of tools and examples for how to access and work with this data for your own purposes. 

Please note that to access any data you will need to first sign up for an account and get an API pin 
(this is summarized in the :ref:`getting_started` section). Please also refer to our :ref:`data_policy` 
and make sure to cite any data sources that you use (examples of how 
to obtain citations are provided in the `API <https://hf-hydrodata.readthedocs.io/en/latest/hf_hydrodata.data_catalog.html#hf_hydrodata.data_catalog.get_citations>`_ 
and the How To sections).

:ref:`getting_started` contains installation instructions and a quick-start guide for obtaining 
gridded datasets and point observations. The sections on :ref:`available_data`, :ref:`gridded_data_index` 
and :ref:`point_data_index` provide more detail on the datasets and fields that are available. 
Full example jupyter workbook workflows for working with point observations are in :ref:`point_examples_index` and 
the :ref:`api` contains the full list of available functions.

Citing hf_hydrodata
-------------------
If you use our tools please cite this package in your work. You can cite the package by citing this paper |joss_doi|

In addition please make sure to cite all of the datasets that you subset. 
Examples for obtaining the DOIs for any dataset you use are provided in the examples. 

.. toctree::
   :maxdepth: 2
   :caption: Table of Contents

   getting_started
   available_data
   gridded_data/index
   point_data/index
   api_reference
   data_policy
   faq


.. |joss_doi| image:: https://joss.theoj.org/papers/10.21105/joss.06623/status.svg
   :target: https://doi.org/10.21105/joss.06623
   :alt: DOI