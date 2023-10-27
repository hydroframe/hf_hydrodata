.. _gen_conus1_domain:

conus1_domain
^^^^^^^^^^^^^^^^^^

Inputs for baseline CONUS1 ParFlow Simulations

References
^^^^^^^^^^
Papers describing the generation of some variables in the dataset.

* `10.5194/gmd-8-923-2015`_
.. _`10.5194/gmd-8-923-2015`: https://doi.org/10.5194/gmd-8-923-2015
* `10.1016/j.advwatres.2015.04.008`_
.. _`10.1016/j.advwatres.2015.04.008`: https://doi.org/10.1016/j.advwatres.2015.04.008
* `10.1002/2014WR016774`_
.. _`10.1002/2014WR016774`: https://doi.org/10.1002/2014WR016774
* `10.1002/2015GL066916`_
.. _`10.1002/2015GL066916`: https://doi.org/10.1002/2015GL066916
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
      - conus1
    * - longitude
      - No description
      - static
      - no
      - conus1


.. list-table:: Hydrogeology Variables in Dataset
    :widths: 25 80 30 20 20
    :header-rows: 1

    * - Variable
      - Description
      - Periods
      - Z Dim
      - Grids
    * - permeability
      - No description
      - static
      - yes
      - conus1
    * - porosity
      - No description
      - static
      - yes
      - conus1
    * - van_genuchten_alpha
      - Alpha parameter for van genucten curve
      - static
      - yes
      - conus1
    * - van_genuchten_n
      - N parameter for van genuchten curve
      - static
      - yes
      - conus1
    * - pf_indicator
      - No description
      - static
      - yes
      - conus1


.. list-table:: Topography Variables in Dataset
    :widths: 25 80 30 20 20
    :header-rows: 1

    * - Variable
      - Description
      - Periods
      - Z Dim
      - Grids
    * - topographic_index
      - No description
      - static
      - no
      - conus1
    * - drainage_area
      - No description
      - static
      - no
      - conus1
    * - slope_x
      - Slope in the x direciton (grid centered)
      - static
      - no
      - conus1
    * - slope_y
      - Slope in the y direction (grid centered)
      - static
      - no
      - conus1
    * - elevation
      - Final processed elevation
      - static
      - no
      - conus1
    * - frac_stream_dist
      - Shortest distance of a given cell to a stream divided by the distance from the stream cell to the nearest topographic divide (values range from 0-1)
      - static
      - no
      - conus1


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
      - conus1


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
      - conus1
    * - aridity_index
      - Aridity index precipitation/ (precipitaiton -evaporaiton)
      - static
      - no
      - conus1


.. list-table:: Stream Network Variables in Dataset
    :widths: 25 80 30 20 20
    :header-rows: 1

    * - Variable
      - Description
      - Periods
      - Z Dim
      - Grids
    * - stream_order
      - Kahler stream order
      - static
      - no
      - conus1
    * - flow_direction_grass
      - D4 flow directions with grass numbering (2=up, 4=left, 6=down, 8=right)
      - static
      - no
      - conus1
    * - stream_segments
      - Stream segment id numbers (0 values outside of stream)
      - static
      - no
      - conus1


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
      - conus1


