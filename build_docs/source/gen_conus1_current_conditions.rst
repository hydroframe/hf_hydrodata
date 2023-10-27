.. _gen_conus1_current_conditions:

conus1_current_conditions
^^^^^^^^^^^^^^^^^^

Current Conditions Conus1

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
    * - soil_moisture
      - Soil moisture
      - daily
      - yes
      - conus1
    * - water_table_depth
      - Water table depth
      - daily
      - no
      - conus1
    * - pressure_head
      - Map 3d grid point to pressure head
      - daily
      - yes
      - conus1


.. list-table:: Location Variables in Dataset
    :widths: 25 80 30 20 20
    :header-rows: 1

    * - Variable
      - Description
      - Periods
      - Z Dim
      - Grids
    * - lat_lon
      - Latitude/longitude in z dimension
      - static
      - yes
      - conus1


