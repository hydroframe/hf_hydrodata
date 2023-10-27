.. _gen_nasa_smap:

nasa_smap
^^^^^^^^^^^^^^^^^^

NASA Soil Moisture

Projections
^^^^^^^^^^^^^^^^^^

There are several different grids supported by this dataset.

Grid: smapgrid
^^^^^^^^^^^^^^^
The projected coordinate system is Lambert Conformal Conic.

Grid: conus2
^^^^^^^^^^^^^^^
The projected coordinate system is Lambert Conformal Conic.

The underlying datum of grid 'conus2' is a sphere with radius 6370000.0 meters.

The first parallel is 30. The second parallel is 60. The origin latitude is 40.0000076294444. The origin longitude is -97.0. The false easting is 2208000.30881173 meters. The false northing is 1668999.65483222 meters.

The grid 'conus2' has dimensions X=4442,  Y=3256,  Z=10

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
      - No description
      - daily
      - yes
      - smapgrid, conus2


.. list-table:: Location Variables in Dataset
    :widths: 25 80 30 20 20
    :header-rows: 1

    * - Variable
      - Description
      - Periods
      - Z Dim
      - Grids
    * - latitude
      - No description
      - static
      - no
      - smapgrid
    * - longitude
      - No description
      - static
      - no
      - smapgrid


