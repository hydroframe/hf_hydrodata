.. _gen_observations:

observations
^^^^^^^^^^^^^^^^^^

Observation Point

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
    * - streamflow
      - Streamflow
      - daily, weekly, monthly
      - mean, sum
      - no
      - 
    * - anomaly
      - Difference from normal
      - daily, weekly, monthly
      - mean, sum
      - no
      - 
    * - anomaly_daily_week_of_values
      - Daily amomaly
      - daily
      - mean
      - no
      - 


.. list-table:: Subsurface Variables in Dataset
    :widths: 25 80 30 20 20 20
    :header-rows: 1

    * - Variable
      - Description
      - Periods
      - Aggregation
      - Z Dim
      - Grids
    * - water_table_depth
      - Water table depth
      - daily, weekly, monthly
      - mean, sum
      - no
      - 


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
      - daily, weekly, monthly
      - mean, sum
      - no
      - 


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
      - mean
      - no
      - 
    * - air_temp
      - Air temperature
      - daily
      - mean
      - no
      - 


