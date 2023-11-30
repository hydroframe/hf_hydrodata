.. _data_collection:

Point Observations Sources
=============================

Data Sources
-------------
All source data comes from public sources that have been compiled in order for users to access the 
data with a single common Python syntax. This section describes each of those original sources.

usgs_nwis
^^^^^^^^^
Data with ``dataset='usgs_nwis'`` comes from the United States Geological Survey (USGS) National
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


snotel
^^^^^^^^^ 
Data with ``dataset='snotel'`` represents sites that are part of the `Snow Telemetry 
(SNOTEL) network <https://www.nrcs.usda.gov/wps/portal/wcc/home/aboutUs/monitoringPrograms/automatedSnowMonitoring/#:~:text=SNOTEL%20sites%20are%20designed%20to,used%20to%20keep%20batteries%20charged.>`_.
The data itself comes from the United States Department of Agriculture (USDA)
Natural Resources Conservation Service (NRCS) `Air Water 
Database <https://www.nrcs.usda.gov/wps/portal/wcc/home/dataAccessHelp/webService>`_.

scan
^^^^^^^^^ 
Data with ``dataset='scan'`` represents sites that are part of the `Soil Climate Analysis Network
(SCAN) <https://www.nrcs.usda.gov/resources/data-and-reports/soil-climate-analysis-network>`_.
The data itself comes from the United States Department of Agriculture (USDA)
Natural Resources Conservation Service (NRCS) `Air Water 
Database <https://www.nrcs.usda.gov/wps/portal/wcc/home/dataAccessHelp/webService>`_.

ameriflux
^^^^^^^^^
Data with ``dataset='ameriflux'`` comes from the `AmeriFlux <https://ameriflux.lbl.gov/data/data-policy/>`_
network.


Data Collection
------------------
We query data from the above sources weekly, early on Sunday mornings. Each weekly job collects all observations
since the date through which we have existing data stored. For sites that are currently in operation, this
translates to collecting data for only the previous week (7 days for daily data, 168 hours for hourly data).
Because of the sparsity of the USGS `temporal_resolution='instantaneous'` groundwater measurements, those are 
not included in this weekly schedule. We plan to query that source for new observations roughly every few months.

Note that raw hourly data is saved in UTC while raw daily data is saved with respect to the local site time zone. 

To maintain the integrety and traceability back to the original sources, our team conducts very limited data 
manipulation on the queried data. This includes the following:

* Unit translation into SI units  
* Standardization of NaN/missing values

  * For example, USGS will sometimes provide strings such as "Ice" or "Dry" to indicate reasons for why certain
    observations are missing. A full list of such fields is available `here <https://help.waterdata.usgs.gov/codes-and-parameters/instantaneous-and-daily-value-status-codes>`_.
    We standardize these values into the numeric numpy.NaN to allow the entireity of the series to be interpreted
    as numeric.
* Consolidating multiple concurrent data series

  * The USGS data sometimes provides multiple concurrent observation series for the same variable for the same site.

    In these cases, we consolidate the multiple series into a single series following these prioritizations:

      * If one of the series has been verified, we prioritize that over provisional data
      * If both series are identical values, we simply reduce down to a single set of observations
      * If one of the series has non-missing data and the other series has missing data, we prioritize the non-missing data
      * If multiple series remain with conflicting values, we take the average of the resulting non-missing values