.. _gen_conus1_baseline_mod:

conus1_baseline_mod
^^^^^^^^^^^^^^^^^^

Modern CONUS1 simulations WY 2003-2006

References
^^^^^^^^^^
Papers describing the generation of some variables in the dataset.

* `10.5194/gmd-14-7223-2021`_
.. _`10.5194/gmd-14-7223-2021`: https://doi.org/10.5194/gmd-14-7223-2021
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

.. list-table:: Land Surface Variables in Dataset
    :widths: 25 80 30 20 20 20
    :header-rows: 1

    * - Variable
      - Description
      - Periods
      - Aggregation
      - Z Dim
      - Grids
    * - evapotranspiration
      - Evapotranspiration
      - daily, hourly
      - sum
      - no
      - conus1
    * - streamflow
      - Streamflow
      - daily
      - 
      - no
      - conus1
    * - swe_melt
      - Difference in swe over a given time period
      - daily
      - 
      - no
      - conus1
    * - latent_heat
      - Latent heat flux from canopy height to atmosphere
      - hourly
      - 
      - no
      - conus1
    * - outward_longwave_radiation
      - Outgoing long-wave radiation from ground+canopy
      - hourly
      - 
      - no
      - conus1
    * - sensible_heat
      - Sensible heat from canopy height to atmosphere
      - hourly
      - 
      - no
      - conus1
    * - ground_heat
      - Ground heat flux
      - hourly
      - 
      - no
      - conus1
    * - ground_evap
      - Ground surface evaporation rate
      - hourly
      - 
      - no
      - conus1
    * - ground_evap_heat
      - Evaporation heat flux from ground
      - hourly
      - 
      - no
      - conus1
    * - transpiration_leaves
      - Evaporation+transpiration from leaves
      - hourly
      - 
      - no
      - conus1
    * - transpiration
      - Transpiration rate
      - hourly
      - 
      - no
      - conus1


.. list-table:: Subsurface Variables in Dataset
    :widths: 25 80 30 20 20 20
    :header-rows: 1

    * - Variable
      - Description
      - Periods
      - Aggregation
      - Z Dim
      - Grids
    * - soil_moisture
      - Soil moisture
      - daily
      - mean
      - yes
      - conus1
    * - water_table_depth
      - Water table depth
      - daily
      - mean
      - no
      - conus1
    * - pressure_head
      - Map 3d grid point to pressure head
      - daily, hourly
      - mean
      - yes
      - conus1
    * - pressure_next
      - Pressure head at the following timestep
      - daily
      - tbd
      - yes
      - conus1
    * - saturation
      - Map 3d grid point to saturation value
      - hourly
      - 
      - yes
      - conus1
    * - infiltration
      - Infiltration
      - hourly
      - 
      - no
      - conus1
    * - ground_temp
      - Ground temperature
      - hourly
      - 
      - no
      - conus1
    * - soil_temp
      - Soil temperature
      - hourly
      - 
      - yes
      - conus1


.. list-table:: Surface Water Variables in Dataset
    :widths: 25 80 30 20 20 20
    :header-rows: 1

    * - Variable
      - Description
      - Periods
      - Aggregation
      - Z Dim
      - Grids
    * - swe
      - Snow water equivalent
      - daily, hourly
      - eod
      - no
      - conus1
    * - irrigation
      - Irrigation applied at surface
      - hourly
      - 
      - no
      - conus1


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
      - daily
      - ?
      - no
      - conus1
    * - air_temp
      - Air temperature
      - daily
      - max, min
      - no
      - conus1


.. list-table:: Run File Variables in Dataset
    :widths: 25 80 30 20 20 20
    :header-rows: 1

    * - Variable
      - Description
      - Periods
      - Aggregation
      - Z Dim
      - Grids
    * - parflow_run
      - Run scripts and other model configuration files
      - static
      - 
      - no
      - conus1
    * - clm_run
      - No description
      - static
      - 
      - no
      - conus1


