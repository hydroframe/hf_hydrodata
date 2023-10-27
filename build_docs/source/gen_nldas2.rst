.. _gen_NLDAS2:

NLDAS2
^^^^^^^^^^^^^^^^^^

National Land Data Assimilation Product V2

References
^^^^^^^^^^
Papers describing the generation of some variables in the dataset.

* `10.5194/gmd-14-7223-2021`_
.. _`10.5194/gmd-14-7223-2021`: https://doi.org/10.5194/gmd-14-7223-2021
* `10.1002/2016GL069964`_
.. _`10.1002/2016GL069964`: https://doi.org/10.1002/2016GL069964
Projections
^^^^^^^^^^^^^^^^^^

The projected coordinate system is Lambert Conformal Conic.

The underlying datum of grid 'conus1' is an elipsoid with equatorial radius 6378137.0 meters and polar radius 6356752.31 meters.

The first parallel is 33. The second parallel is 45. The origin latitude is 39. The origin longitude is -96.0. The false easting is 1885055.4995 meters. The false northing is 604957.0654 meters.

The grid 'conus1' has dimensions X=3342,  Y=1888,  Z=5

Data Variables in Dataset
^^^^^^^^^^^^^^^^^^

This describes the available variables of the dataset.
Use the dataset, variables and periods in python access functions as described in the QuickStart Guide and Examples.

.. list-table:: Atmospheric Variables in Dataset
    :widths: 25 80 30 20 20 20
    :header-rows: 1

    * - Variable
      - Description
      - Periods
      - Aggregation
      - Z Dim
      - Grids
    * - precipitation
      - Precipitation
      - hourly, daily, monthly_clim, annual_clim, monthly
      - sum
      - no
      - conus1
    * - downward_longwave
      - Downward longwave radiation
      - hourly, daily
      - mean
      - no
      - conus1
    * - downward_shortwave
      - Downward shortwave radiation
      - hourly, daily
      - mean
      - no
      - conus1
    * - specific_humidity
      - Specific humidity
      - hourly, daily
      - mean
      - no
      - conus1
    * - air_temp
      - Air temperature
      - hourly, daily, monthly_clim, annual_clim, monthly
      - min, max, mean
      - no
      - conus1
    * - east_windspeed
      - Zonal windspeed
      - hourly, daily
      - mean
      - no
      - conus1
    * - north_windspeed
      - Meridional windspeed
      - hourly, daily
      - mean
      - no
      - conus1
    * - atmospheric_pressure
      - Atmospheric pressure
      - hourly, daily
      - mean
      - no
      - conus1


