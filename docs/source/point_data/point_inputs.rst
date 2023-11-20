.. _point_inputs:

Input Fields
======================

Mandatory Parameters
--------------------
The first four columns of the following table outline the four mandatory input parameters that can be used 
to access data via the ``hf_hydrodata.point`` module: 

* ``data_source`` 
* ``variable`` 
* ``temporal_resolution``  
* ``aggregation`` 


Note that the parameter ``depth_level`` only needs to be provided when querying soil 
moisture data. Returned variable units are provided here for reference.

Please see the next section, :ref:`data_collection`, for descriptions on our data sources and data collection
process.
 
.. container::
   :name: point_input_parameters

   .. table:: Input parameters available for accessing site-level data.

      +----------------------------+----------------------------+---------------------------+------------------------+----------------------------------+------------------------+
      | data_source                | variable                   | temporal_resolution       | aggregation            | depth_level                      | units                  |                         
      +============================+============================+===========================+========================+==================================+========================+
      | 'usgs_nwis'                | 'streamflow'               | 'hourly'                  | 'average'              |                                  | :math:`m^{3} s^{-1}`   |
      +----------------------------+----------------------------+---------------------------+------------------------+----------------------------------+------------------------+
      | 'usgs_nwis'                | 'streamflow'               | 'daily'                   | 'average'              |                                  | :math:`m^{3} s^{-1}`   |
      +----------------------------+----------------------------+---------------------------+------------------------+----------------------------------+------------------------+
      | 'usgs_nwis'                | 'wtd'                      | 'hourly'                  | 'average'              |                                  | m                      |
      +----------------------------+----------------------------+---------------------------+------------------------+----------------------------------+------------------------+
      | 'usgs_nwis'                | 'wtd'                      | 'daily'                   | 'average'              |                                  | m                      |
      +----------------------------+----------------------------+---------------------------+------------------------+----------------------------------+------------------------+
      | 'usgs_nwis'                | 'wtd'                      | 'instantaneous'           | 'instantaneous'        |                                  | m                      |
      +----------------------------+----------------------------+---------------------------+------------------------+----------------------------------+------------------------+
      | 'usda_nrcs'                | 'swe'                      | 'daily'                   | 'start-of-day'         |                                  | mm                     |
      +----------------------------+----------------------------+---------------------------+------------------------+----------------------------------+------------------------+
      | 'usda_nrcs'                | 'precipitation'            | 'daily'                   | 'accumulated'          |                                  | mm                     |
      +----------------------------+----------------------------+---------------------------+------------------------+----------------------------------+------------------------+
      | 'usda_nrcs'                | 'precipitation'            | 'daily'                   | 'total'                |                                  | mm                     |
      +----------------------------+----------------------------+---------------------------+------------------------+----------------------------------+------------------------+
      | 'usda_nrcs'                | 'precipitation'            | 'daily'                   | 'total, snow-adjusted' |                                  | mm                     |
      +----------------------------+----------------------------+---------------------------+------------------------+----------------------------------+------------------------+
      | 'usda_nrcs'                | 'temperature'              | 'daily'                   | 'minimum'              |                                  | C                      |
      +----------------------------+----------------------------+---------------------------+------------------------+----------------------------------+------------------------+
      | 'usda_nrcs'                | 'temperature'              | 'daily'                   | 'maximum'              |                                  | C                      |
      +----------------------------+----------------------------+---------------------------+------------------------+----------------------------------+------------------------+
      | 'usda_nrcs'                | 'temperature'              | 'daily'                   | 'average'              |                                  | C                      |
      +----------------------------+----------------------------+---------------------------+------------------------+----------------------------------+------------------------+
      | 'usda_nrcs'                | 'soil moisture'            | 'daily'                   | 'start-of-day'         | 2, 4, 8, 20, or 40 (inches)      | pct                    |
      +----------------------------+----------------------------+---------------------------+------------------------+----------------------------------+------------------------+
      | 'ameriflux'                | 'latent heat flux'         | 'hourly'                  | 'total'                |                                  | :math:`W m^{-2}`       |
      +----------------------------+----------------------------+---------------------------+------------------------+----------------------------------+------------------------+
      | 'ameriflux'                | 'sensible heat flux'       | 'hourly'                  | 'total'                |                                  | :math:`W m^{-2}`       |
      +----------------------------+----------------------------+---------------------------+------------------------+----------------------------------+------------------------+
      | 'ameriflux'                | 'shortwave radiation'      | 'hourly'                  | 'average'              |                                  | :math:`W m^{-2}`       |
      +----------------------------+----------------------------+---------------------------+------------------------+----------------------------------+------------------------+
      | 'ameriflux'                | 'longwave radiation'       | 'hourly'                  | 'average'              |                                  | :math:`W m^{-2}`       |
      +----------------------------+----------------------------+---------------------------+------------------------+----------------------------------+------------------------+
      | 'ameriflux'                | 'vapor pressure deficit'   | 'hourly'                  | 'average'              |                                  | hPa                    |
      +----------------------------+----------------------------+---------------------------+------------------------+----------------------------------+------------------------+
      | 'ameriflux'                | 'temperature'              | 'hourly'                  | 'average'              |                                  | C                      |
      +----------------------------+----------------------------+---------------------------+------------------------+----------------------------------+------------------------+
      | 'ameriflux'                | 'wind speed'               | 'hourly'                  | 'average'              |                                  | :math:`m s^{-1}`       |
      +----------------------------+----------------------------+---------------------------+------------------------+----------------------------------+------------------------+

We are under active development and anticipate regularly incorporating additional sources.

Optional Parameters
--------------------
In addition to the mandatory parameters described above, there are optional parameters that can be
used to further filter requests on time and/or geography.

* ``date_start``: A date provided as a string in 'YYYY-MM-DD' format. If provided, data will be returned
  from this date forward (inclusive).
* ``date_end``: A date provided as a string in 'YYYY-MM-DD' format. If provided, data will be returned
  up through this date (inclusive). 
* ``latitude_range``: A tuple where the values represent the minimum and maximum degrees latitude, respectively
  (example: ``(45, 50)``).
* ``longitude_range``: A tuple where the two values represent the minimum and maximum degress longitude, respectively
  (example: ``(-75, -50)``).
* ``site_ids``: A list of string values repsenting one or multiple specific site IDs (example: ``['01011000']``).
* ``state``: The 2-digit state postal code (example: ``'NJ'``).
* ``polygon``: The path to a ``.shp`` shapefile containing a single shape geometry. This file must be readable by
  PyShp's ``shapefile.Reader()``.
* ``polygon_crs``: If ``polygon`` is provided, a user must also provide its associated CRS. This must be in a format 
  accepted by ``pyproj.CRS.from_user_input()``.
* ``site_networks``: A list containing strings that represent common site networks. 
  When ``data_source=='usgs_nwis`` and ``variable=='streamflow'``, options include 'gagesii', 'gagesii_reference', 
  'hcdn2009', and 'camels'. when ``data_source == 'usgs_nwis'`` and ``variable=='wtd'``, options include 
  'climate_response_network'.
* ``min_num_obs``: A positive integer value. If provided, data will be returned only for sites that have at least
  this number of non-NaN observation records within the requested date range (if supplied).
