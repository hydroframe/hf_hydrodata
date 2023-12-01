.. _gridded_methods:

``get_gridded_data``
--------------
The get_gridded_data method returns numpy ndarray of the information
identified by the filter attributes passed to the function.

``get_raw_data``
--------------
The get_raw_data method returns the raw file from the server that
is idenfied by the filter attributes.

``get_date_range``
--------------
The get_date_range method returns an array of [start_date, end_date] 
of the range of dates available for the dataset identified by
the same filter attributes passed to get_gridded_data.

``get_catalog_entry``
--------------
The get_catalog_entry method returns a python dict with attributes
describing the data identified by
the same filter attributes passed to get_gridded_data.

``get_catalog_enties``
--------------
The get_catalog_enties method returns an array of catalog entries
filtered by the attributes passed to the function.

``get_datasets``
--------------
The get_datasets method returns an array of dataset names available
to be specified as the "dataset" filter attribute. Any filter
attribute can be passed as arguments to return only datasets with
data matching the filter attributes.

``get_variables``
--------------
The get_datasets method returns an array of variable names available
to be specified as the "variable" filter attribute.
Any filter
attribute can be passed as arguments to return only variables with
data matching the filter attributes.

``from_latlon``
--------------
This converts a lat/lon point or an array of lat/lon points to grid coordiates.

``to_latlon``
--------------
This converts an x,y grid point or an array of x,y grid points to lat/lon coordinates.

``get_huc_from_xy``
--------------
This returns a HUC id that contains an x,y grid point.

``get_huc_from_latlon``
--------------
This returns a HUC id from a lat/lon coordinate.

``get_huc_bbox``
--------------
This returns the bounding box of a list of HUC ids in grid coordinates.