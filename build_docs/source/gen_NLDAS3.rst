.. _gen_NLDAS3:

NLDAS3
^^^^^^^^^^^^^^^^^^



Projections
^^^^^^^^^^^^^^^^^^

The projected coordinate system is Lambert Conformal Conic.

The underlying datum of grid 'conus2' is a sphere with radius 6370000.0 meters.

The first parallel is 30. The second parallel is 60. The origin latitude is 40.0000076294444. The origin longitude is -97.0. The false easting is 2208000.30881173 meters. The false northing is 1668999.65483222 meters.

The grid 'conus2' has dimensions X=4442,  Y=3256,  Z=10

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
      - hourly, daily, monthly
      - sum
      - no
      - conus2
    * - downward_longwave
      - Downward longwave radiation
      - hourly, daily
      - mean
      - no
      - conus2
    * - downward_shortwave
      - Downward shortwave radiation
      - hourly, daily
      - mean
      - no
      - conus2
    * - specific_humidity
      - Specific humidity
      - hourly, daily
      - mean
      - no
      - conus2
    * - air_temp
      - Air temperature
      - hourly, daily, monthly
      - min, max, mean
      - no
      - conus2
    * - east_windspeed
      - Zonal windspeed
      - hourly, daily
      - mean
      - no
      - conus2
    * - north_windspeed
      - Meridional windspeed
      - hourly, daily
      - mean
      - no
      - conus2
    * - atmospheric_pressure
      - Atmospheric pressure
      - hourly, daily
      - mean
      - no
      - conus2


