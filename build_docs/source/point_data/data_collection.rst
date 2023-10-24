.. _data_collection:

Point Data Sources
==================

Data Sources
-------------
All source data comes from public sources that have been compiled in order for users to access the 
data with a single common Python syntax. This section describes each of those original sources.

usgs_nwis
^^^^^^^^^
Data with ``data_source='usgs_nwis'`` comes from the United States Geological Survey (USGS) National
Water Information System (NWIS) `Water Services <https://waterservices.usgs.gov/>`_ platform.

* Daily streamflow and water table depth data are obtained from the
  `Daily Values Service <https://waterservices.usgs.gov/docs/dv-service/daily-values-service-details/>`_.  

* Hourly streamflow and water table depth data are aggregated to the hourly level from the 
  `Instantaneous Values Service <https://waterservices.usgs.gov/docs/instantaneous-values/instantaneous-values-details/>`_, 
  which are frequently collected at 15-minute increments.   

* The water table depth data accessed with ``temporal_resolution='instantaneous'`` comes from the USGS
  `Groundwater Levels Service <https://waterservices.usgs.gov/docs/groundwater-levels/groundwater-levels-details/>`_. Note
  that these data usually do not have regular temporal coverage and many of the sites with data available
  through this method only have a single point-in-time observation available.  


usda_nrcs
^^^^^^^^^ 
Data with ``data_source='usda_nrcs'`` comes from the United States Department of Agriculture (USDA)
Natural Resources Conservation Service (NRCS) `Air Water 
Database <https://www.nrcs.usda.gov/wps/portal/wcc/home/dataAccessHelp/webService>`_.


ameriflux
^^^^^^^^^
Data with ``data_source='ameriflux'`` comes from the `AmeriFlux <https://ameriflux.lbl.gov/data/data-policy/>`_
network.


Data Collection
------------------
This section provides some additional details on our data collection process.