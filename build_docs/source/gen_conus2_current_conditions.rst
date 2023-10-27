.. _gen_conus2_current_conditions:

conus2_current_conditions
^^^^^^^^^^^^^^^^^^

Current Conditions Conus2

Projections
^^^^^^^^^^^^^^^^^^

There are several different grids supported by this dataset.

Grid: conus2
^^^^^^^^^^^^^^^
The projected coordinate system is Lambert Conformal Conic.

The underlying datum of grid 'conus2' is a sphere with radius 6370000.0 meters.

The first parallel is 30. The second parallel is 60. The origin latitude is 40.0000076294444. The origin longitude is -97.0. The false easting is 2208000.30881173 meters. The false northing is 1668999.65483222 meters.

The grid 'conus2' has dimensions X=4442,  Y=3256,  Z=10

Grid: conus2_wtd
^^^^^^^^^^^^^^^
The projected coordinate system is Lambert Conformal Conic.

The underlying datum of grid 'conus2_wtd' is a sphere with radius 6370000.0 meters.

The first parallel is 30. The second parallel is 60. The origin latitude is 40.0000076294444. The origin longitude is -97.0. The false easting is 2848061.2936895434 meters. The false northing is 1724874.3619788198 meters.

The grid 'conus2_wtd' has dimensions X=5940,  Y=3484,  Z=10

Grid: conus2_wtd.100
^^^^^^^^^^^^^^^
The projected coordinate system is Lambert Conformal Conic.

The underlying datum of grid 'conus2_wtd.100' is a sphere with radius 6370000.0 meters.

The first parallel is 30. The second parallel is 60. The origin latitude is 40.0000076294444. The origin longitude is -97.0. The false easting is 2848511.2936895434 meters. The false northing is 1724524.3619788198 meters.

The grid 'conus2_wtd.100' has dimensions X=59400,  Y=34832,  Z=10

Grid: conus2_wtd.30
^^^^^^^^^^^^^^^
The projected coordinate system is Lambert Conformal Conic.

The underlying datum of grid 'conus2_wtd.30' is a sphere with radius 6370000.0 meters.

The first parallel is 30. The second parallel is 60. The origin latitude is 40.0000076294444. The origin longitude is -97.0. The false easting is 2848549.2233063863 meters. The false northing is 1724561.0407288047 meters.

The grid 'conus2_wtd.30' has dimensions X=246056,  Y=144287,  Z=10

Data Variables in Dataset
^^^^^^^^^^^^^^^^^^

This describes the available variables of the dataset.
Use the dataset, variables and periods in python access functions as described in the QuickStart Guide and Examples.

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
      - conus2


.. list-table:: Subsurface Variables in Dataset
    :widths: 25 80 30 20 20
    :header-rows: 1

    * - Variable
      - Description
      - Periods
      - Z Dim
      - Grids
    * - water_table_depth
      - Water table depth
      - static
      - no
      - conus2_wtd, conus2_wtd.100, conus2_wtd.30
    * - soil_moisture
      - Soil moisture
      - daily
      - yes
      - conus2


