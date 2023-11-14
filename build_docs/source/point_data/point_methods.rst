.. _point_methods:

Point Observations Methods
==================================
There are four main functions available from the ``hf_hydrodata.point`` module. These are described briefly 
below, along with specific Jupyter notebooks in :ref:`examples_index` that showcase that functionality.

``get_data``
--------------
The ``get_data`` method returns a pandas DataFrame of site-level observations time series for a specific variable, 
from a specific data source. The ``get_data`` method requires four mandatory input parameters and an optional
number of additional geographic, temporal, or other filters. Both sets of available parameters are described
in :ref:`point_inputs`.

* Examples using ``get_data``:  

  * :ref:`/examples/example_get_data.ipynb`
  * :ref:`/examples/example_plot_data.ipynb`
  * :ref:`/examples/example_shapefile.ipynb`

``get_metadata``
------------------
The ``get_metadata`` method returns a pandas DataFrame of site-level attributes for sites that have observations 
for a specific variable, from a specific data source. The ``get_metadata`` method requires four mandatory input 
parameters and an optional number of additional geographic, temporal, or other filters. Both sets of available 
parameters are described in :ref:`point_inputs`. Both ``get_data`` and ``get_metadata`` accept the same set of 
mandatory and optional parameters and it is recommended to use the same inputs across these functions for a given
workflow. Descriptions of the returned attributes are available in :ref:`Metadata Description`.

* Examples using ``get_metadata``:  

  * :ref:`/examples/example_get_data.ipynb`
  * :ref:`/examples/example_plot_data.ipynb`
  * :ref:`/examples/example_shapefile.ipynb`

``get_citations``
-------------------
The ``get_citations`` method returns a Python dictionary containing information about site-level DOIs 
(if applicable) and requested attribution information for using each type of data. The ``get_citations`` method
requires the same four mandatory input parameters as ``get_data`` and ``get_metadata``. For site-level DOI's 
(currently only applicable for ``data_source='ameriflux'``), an additional ``site_ids`` parameter may be provided.

* Examples using ``get_citations``:  

  * :ref:`/examples/example_get_data.ipynb`

``get_site_variables``
------------------------
The ``get_site_variables`` method returns a pandas DataFrame that is unique for a site ID, variable combination.
The returned attributes include high-level information about the site location and period of record for each 
variable. The ``get_site_variables`` method does not require any mandatory parameters, though we advise including
as many filters as relevant to increase processing speed and keep the returned information as relevant as possible.
Any of the input parameters available to ``get_data`` (either mandatory or optional) may be supplied to 
``get_site_variables``.

* Examples using ``get_site_variables``: 

  * :ref:`/examples/example_explore_data.ipynb`