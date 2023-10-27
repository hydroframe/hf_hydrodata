.. _gen_conus1_baseline_85:

conus1_baseline_85
^^^^^^^^^^^^^^^^^^

WY 1985 Baseline CONUS1 simulation

References
^^^^^^^^^^
Papers describing the generation of some variables in the dataset.

* `10.1126/science.aaf7891`_
.. _`10.1126/science.aaf7891`: https://doi.org/10.1126/science.aaf7891
* `10.5194/hess-21-1117-2017`_
.. _`10.5194/hess-21-1117-2017`: https://doi.org/10.5194/hess-21-1117-2017
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

.. list-table:: Subsurface Variables in Dataset
    :widths: 25 80 30 20 20
    :header-rows: 1

    * - Variable
      - Description
      - Periods
      - Z Dim
      - Grids
    * - pressure_head
      - No description
      - daily, hourly
      - yes
      - conus1
    * - saturation
      - No description
      - hourly
      - yes
      - conus1
    * - infiltration
      - Infiltration
      - hourly
      - no
      - conus1
    * - ground_temp
      - Ground temperature
      - hourly
      - no
      - conus1
    * - soil_temp
      - Soil temperature
      - hourly
      - yes
      - conus1


.. list-table:: Land Surface Variables in Dataset
    :widths: 25 80 30 20 20
    :header-rows: 1

    * - Variable
      - Description
      - Periods
      - Z Dim
      - Grids
    * - latent_heat
      - Latent heat flux from canopy height to atmosphere
      - hourly
      - no
      - conus1
    * - outward_longwave_radiation
      - Outgoing long-wave radiation from ground+canopy
      - hourly
      - no
      - conus1
    * - sensible_heat
      - Sensible heat from canopy height to atmosphere
      - hourly
      - no
      - conus1
    * - ground_heat
      - Ground heat flux
      - hourly
      - no
      - conus1
    * - evapotranspiration
      - No description
      - hourly
      - no
      - conus1
    * - ground_evap
      - Ground surface evaporation rate
      - hourly
      - no
      - conus1
    * - ground_evap_heat
      - Evaporation heat flux from ground
      - hourly
      - no
      - conus1
    * - transpiration_leaves
      - Evaporation+transpiration from leaves
      - hourly
      - no
      - conus1
    * - transpiration
      - Transpiration rate
      - hourly
      - no
      - conus1


.. list-table:: Surface Water Variables in Dataset
    :widths: 25 80 30 20 20
    :header-rows: 1

    * - Variable
      - Description
      - Periods
      - Z Dim
      - Grids
    * - swe
      - No description
      - hourly
      - no
      - conus1
    * - irrigation
      - Irrigation applied at surface
      - hourly
      - no
      - conus1


.. list-table:: Run File Variables in Dataset
    :widths: 25 80 30 20 20
    :header-rows: 1

    * - Variable
      - Description
      - Periods
      - Z Dim
      - Grids
    * - parflow_run
      - Run scripts and other model configuration files
      - static
      - no
      - conus1
    * - clm_run
      - No description
      - static
      - no
      - conus1


