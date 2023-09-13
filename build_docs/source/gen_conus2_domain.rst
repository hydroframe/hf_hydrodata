.. _gen_conus2_domain:

conus2_domain
=============

The CONUS2 domain covers the entire contiguous US and areas draining to it.
The domain extent and projection are based off of the National Water Model Grid. 
Manual corrections were made along the coastline to get rid of cells that were water 
and to prune any orphan cells (i.e. cells attached to the domain only by a corner).

References
----------
Papers describing the generation of some of the variables in this dataset can be found at:

* `10.5194/essd-13-3263-2021`_.

.. _`10.5194/essd-13-3263-2021`: https://doi.org/10.5194/essd-13-3263-2021

Projections
-----------
The projected coordinate system is Lambert Conformal Conic. 
The underlying datum is a sphere with radius 6370000m.
The following are the grids supported the dataset. Click on the grid attribute key below for projection details.

.. list-table:: Supported Grids
    :widths: 25 100
    :header-rows: 1

    * - Grid Attribute Key
      - Description
    * - :ref:`gen_conus2_grid`
      - The conus2 grid covers the contential US.

Data Files of the Dataset
=======================================

.. list-table:: Domain Extent Variables in Dataset
    :widths: 25 100
    :header-rows: 1

    * - Variable Attribute Key
      - Description
    * - lat_lng
      - The latitude and longitude of the center of every grid cell provided in decimal degree. Data are provided in two columns and the row order follows the standard ParFlow file order (i.e. looping from the lower left corner to the upper right with y as the outer loop).
    * - mask
      - A mask for the PF conus domain that extends to 1 grid cell around the active domain in every direction. Lakes and sinks are not represented in this file (0=outside  domain, 1=inside domain).

.. list-table:: Topography Variables in Dataset
    :widths: 25 100
    :header-rows: 1

    * - Variable Attribute Key
      - Description
    * - drainage_area
      - Drainage area in km2 calculated based on the flow directions.
    * - flow_direction
      - Primary flow direction for every grid cell (1=down, 2=left, 3=up, 4=right)
    * - slope_x
      - Final slopes in the x direction calculated at cell faces to be used with the OverlandKinematic formulation in ParFlow.  
    * - slope_y
      - Final slopes in the y direction calculated at cell faces to be used with the OverlandKinematic formulation in ParFlow.
    * - stream_segments
      - Mask of stream segments with their segment IDs. 
    * - subbasins
      - Map of the subbasins drainage area for each stream segment.
    * - distance_stream_lin
      - Map of distance for each cell to the stream (CONUS2.0.Final1km.RiverMask) using the function StreamDist in PriorityFlow.
       