``get_point_data``
----------------------
The ``get_point_data`` method returns a pandas DataFrame of site-level observations time series for a specific variable, 
from a specific data source. The ``get_point_data`` method requires four mandatory input parameters and an optional
number of additional geographic, temporal, or other filters. Both sets of available parameters are described
in the function's API reference: :ref:`api`. ::

    # Import package
    from hf_hydrodata import get_point_data

    # Define filters and return point observations data as pandas DataFrame
    data = get_point_data(dataset = "usgs_nwis", variable = "streamflow", 
                          temporal_resolution = "daily", aggregation = "mean",
                          start_date = "2022-01-01", end_date = "2022-01-05", 
                          latitude_range = (45, 50),
                          longitude_range = (-75, -50))
    
    # View first five records
    data.head(5)

* Examples using ``get_point_data``:  

  * :ref:`/point_data/examples/example_get_data.ipynb`
  * :ref:`/point_data/examples/example_plot_data.ipynb`
  * :ref:`/point_data/examples/example_shapefile.ipynb`

``get_point_metadata``
--------------------------
The ``get_point_metadata`` method returns a pandas DataFrame of site-level attributes for sites that have observations 
for a specific variable, from a specific data source. The ``get_point_metadata`` method requires four mandatory input 
parameters and an optional number of additional geographic, temporal, or other filters. Both sets of available 
parameters are described in the function's API reference: :ref:`api`. 
Both ``get_point_data`` and ``get_point_metadata`` accept a similar set of mandatory and optional parameters and 
it is recommended to use the same inputs across these functions for a given workflow. Descriptions of the returned 
attributes are available in :ref:`point_obs_metadata`. ::

    # Import package
    from hf_hydrodata import get_point_metadata

    # Define filters and return as pandas DataFrame
    data_source = 'usgs_nwis'
    variable = 'streamflow'
    temporal_resolution = 'daily'
    aggregation = 'mean'

    # Get the metadata about the sites with returned data
    metadata = get_point_metadata(dataset = "usgs_nwis", variable = "streamflow", 
                                  temporal_resolution = "daily", aggregation = "mean",
                                  start_date = "2022-01-01", end_date = "2022-01-05", 
                                  latitude_range = (45, 50),
                                  longitude_range = (-75, -50))

    # View first five records                          
    metadata.head(5)

* Examples using ``get_point_metadata``:  

  * :ref:`/point_data/examples/example_get_data.ipynb`
  * :ref:`/point_data/examples/example_plot_data.ipynb`
  * :ref:`/point_data/examples/example_shapefile.ipynb`

``get_site_variables``
------------------------
The ``get_site_variables`` method returns a pandas DataFrame that is unique for a site ID, variable combination.
The returned attributes include high-level information about the site location and period of record for each 
variable. The ``get_site_variables`` method does not require any mandatory parameters, though we advise including
as many filters as relevant to increase processing speed and keep the returned information as relevant as possible.
Any of the input parameters available to ``get_point_data`` (either mandatory or optional) may be supplied to 
``get_site_variables``. ::

    # Import package
    from hf_hydrodata import get_site_variables

    # Explore what streamflow data is available from the state of Colorado for sites that operated during WY2019
    df = get_site_variables(variable = "streamflow", 
                            state="CO", 
                            date_start = "2018-10-01", date_end = "2019-09-30")

    # View first five records
    df.head(5)


* Examples using ``get_site_variables``: 

  * :ref:`/point_data/examples/example_explore_data.ipynb`