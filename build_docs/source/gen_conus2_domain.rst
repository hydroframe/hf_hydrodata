.. _gen_conus2_domain:

conus2_domain
^^^^^^^^^^^^^^^^^^

Inputs for baseline CONUS2 ParFlow Simulations

Projections
^^^^^^^^^^^^^^^^^^

There are several different grids supported by this dataset.

Grid: conus2
^^^^^^^^^^^^^^^
The projected coordinate system is Lambert Conformal Conic.

The underlying datum of grid 'conus2' is a sphere with radius 6370000.0 meters.

The first parallel is 30. The second parallel is 60. The origin latitude is 40.0000076294444. The origin longitude is -97.0. The false easting is 2208000.30881173 meters. The false northing is 1668999.65483222 meters.

The grid 'conus2' has dimensions X=4442,  Y=3256,  Z=10

Grid: conus2.250
^^^^^^^^^^^^^^^
The projected coordinate system is Lambert Conformal Conic.

Grid: conus1
^^^^^^^^^^^^^^^
The projected coordinate system is Lambert Conformal Conic.

The underlying datum of grid 'conus1' is an elipsoid with equatorial radius 6378137.0 meters and polar radius 6356752.31 meters.

The first parallel is 33. The second parallel is 45. The origin latitude is 39. The origin longitude is -96.0. The false easting is 1885055.4995 meters. The false northing is 604957.0654 meters.

The grid 'conus1' has dimensions X=3342,  Y=1888,  Z=5

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
    * - latitude
      - No description
      - static
      - no
      - conus2
    * - longitude
      - No description
      - static
      - no
      - conus2
    * - lat_lon
      - Latitude/longitude in z dimension
      - static
      - yes
      - conus2


.. list-table:: Land Surface Variables in Dataset
    :widths: 25 80 30 20 20
    :header-rows: 1

    * - Variable
      - Description
      - Periods
      - Z Dim
      - Grids
    * - veg_type_IGBP
      - Vegetation type using igbp classifications
      - static
      - yes
      - conus2, conus2.250


.. list-table:: Topography Variables in Dataset
    :widths: 25 80 30 20 20
    :header-rows: 1

    * - Variable
      - Description
      - Periods
      - Z Dim
      - Grids
    * - slope_x
      - Slope in the x direciton (grid centered)
      - static
      - no
      - conus2
    * - slope_y
      - Slope in the y direction (grid centered)
      - static
      - no
      - conus2
    * - drainage_area
      - No description
      - static
      - no
      - conus2, conus2.250
    * - elevation
      - Final processed elevation
      - static
      - no
      - conus2, conus2.250
    * - mask
      - Mask showing active domain
      - static
      - no
      - conus2


.. list-table:: Run File Variables in Dataset
    :widths: 25 80 30 20 20
    :header-rows: 1

    * - Variable
      - Description
      - Periods
      - Z Dim
      - Grids
    * - clm_run
      - No description
      - static
      - no
      - conus2
    * - pf_solid
      - File to define a domain shape for parflow
      - static
      - no
      - conus2


.. list-table:: Climate Variables in Dataset
    :widths: 25 80 30 20 20
    :header-rows: 1

    * - Variable
      - Description
      - Periods
      - Z Dim
      - Grids
    * - pme
      - Recharge forcing used for parflow spinups
      - static
      - yes
      - conus2


.. list-table:: Surface Water Variables in Dataset
    :widths: 25 80 30 20 20
    :header-rows: 1

    * - Variable
      - Description
      - Periods
      - Z Dim
      - Grids
    * - mannings
      - Mannings roughness coefficient
      - static
      - no
      - conus2


.. list-table:: Hydrogeology Variables in Dataset
    :widths: 25 80 30 20 20
    :header-rows: 1

    * - Variable
      - Description
      - Periods
      - Z Dim
      - Grids
    * - pf_indicator
      - No description
      - static
      - yes
      - conus1
    * - pf_flowbarrier
      - File defining the depth to flow barier
      - static
      - no
      - conus2


.. list-table:: Subsurface Variables in Dataset
    :widths: 25 80 30 20 20
    :header-rows: 1

    * - Variable
      - Description
      - Periods
      - Z Dim
      - Grids
    * - ss_pressure_head
      - Presssure head at the end of spinup
      - static
      - yes
      - conus2


.. list-table:: Stream Network Variables in Dataset
    :widths: 25 80 30 20 20
    :header-rows: 1

    * - Variable
      - Description
      - Periods
      - Z Dim
      - Grids
    * - distance_stream_lin
      - Shortest linear distance to streams
      - static
      - no
      - conus2, conus2.250
    * - flow_direction
      - D4 flow direcitons with the following numbering: 1=down, 2=left, 3=up, 4=right
      - static
      - no
      - conus2, conus2.250
    * - stream_order
      - Kahler stream order
      - static
      - no
      - conus2, conus2.250
    * - stream_segments
      - Stream segment id numbers (0 values outside of stream)
      - static
      - no
      - conus2, conus2.250


