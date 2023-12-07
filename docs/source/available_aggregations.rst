.. _available_aggregations:

Aggregations
============

Some of the datasets available have been temporally aggregated (e.g. aggregating from hourly to daily). 
The following lists defines all of the temporal aggregations that are available.  Note that most variables 
will have only a few aggregation options. Refer to :ref:`available_datasets` for the list of variables and 
aggregations that are available for a specific dataset. 


.. list-table:: Aggregation
    :widths: 25 100
    :header-rows: 1

    * - Attribute
      - Description
    * - mean
      - Take the average of the data over the desired temporal_resolution.
    * - max
      - Take the max of the data over the desired temporal_resolution.
    * - min
      - Take the min of the data over the desired temporal_resolution.
    * - eod
      - End of day: select the last value on a day. 
    * - sod
      - Start of day: select the first value on a day. 
    * - sum
      - Sum the data over the desired temporal_resolution.
    * - accumulated
      - Data is aggregated as the accumulation over the temporal_resolution.
    * - sum_snow_adjusted
      - Data is the accumulation, but snow adjusted over the temporal_resolution.
