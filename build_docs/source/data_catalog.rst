Data Catalog
======================
The hydrodata data catalog is a static database of attributes that can be used to filter data in the
file share.

The attributes used to identify data are described below.
Click on an attribute name to see available attribute keys to use as filter values.

.. list-table:: Primary Attributes for Filtering
    :widths: 25 100
    :header-rows: 1

    * - Attribute Name
      - Description
    * - :ref:`gen_datasets`
      - Collected or derived products containing several data variables
    * - variable
      - A data variable in a dataset such as air_temp, streamflow or pressure
    * - period
      - A time period that the variable data is collected such as hourly, daily, monthly

.. list-table:: Secondary Attributes for Filtering
    :widths: 25 100
    :header-rows: 1

    * - Attribute Name
      - Description
    * - aggregation
      - How the data is aggregated: such as mean, max, min
    * - grid
      - The grid size and projection of the data variable
    * - file_type
      - The format that the data is stored in the GPFS file stored
Note: data is returned as a numpy array regardless of of how the data is stored.
The aggregation is not required unless the data is available in multiple aggregations such as temperature variables.
The grid is not required unless the dataset variable is available in multiple grids.

.. list-table:: Attributes for Filtering Space and Time
    :widths: 25 100
    :header-rows: 1

    * - Attribute Name
      - Description
    * - grid_bounds
      - The requested bounds of data to be returned as array of int: [x_min, y_min, x_max, y_max]
    * - lat_lng_bounds
      - The requested bounds of data to be returned as an array of float: [low_lat, low_lng, high_lat, high_lng]
    * - start_time
      - The requested start time of the data to be returned.
        This is required if the data has a period with a time dimension.
        The value can be a string or a python datetime object.
        If the value us a string it must be in the form YYYY-MM-DD or YYYY-MM-DD HH:MM:SS.
    * - end_time
      - The ending time of the range of data to be returned.
        If not specified then returns only the data at the start time.
        If specified then return data start_time <= T < end_time.


In addition, there are many other attributes that describe the data that are not used for filtering.
These other metadata attributes can be retrieved to understand more about the data that has been requested.
