.. _available_aggregations:

Aggregations
============

This is the list of available aggregations that can be passed to filters to identify data to be retrieved.

In most cases this is not required since there is only one aggregation for the variable, but some variables
such as air_temp require you to identify aggregation attributes such as max, min, or mean.

Not all aggregation are supported in all data sets. Consult the ``Datasets`` documentation for
which aggregations are supported for which variables.

.. list-table:: Aggregation
    :widths: 25 100
    :header-rows: 1

    * - Attribute
      - Description
    * - mean
      - Data that is aggregated as the mean of other data.
    * - max
      - Data that is aggregated as the max of other data.
    * - min
      - Data that is aggregated as the min of other data.
    * - eod
      - Data that aggregated as the end of the day of other data.
    * - sod
      - Data that is aggregated as start of the day of other data.
    * - sum
      - Data that is aggregated as the sum of the data over the temporal_resolution.
    * - accumulated
      - Data is aggregated as the accumulation over the temporal_resolution.
    * - sum_snow_adjusted
      - Data is the accumulation, but snow adjusted over the temporal_resolution.
