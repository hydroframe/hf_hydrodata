Welcome to Hydrodata Documentation
=======================================

Hydrodata is a large GPFS file share that contains a large
number of hydrology related data files that have been accumulated and derived. The files in hydrodata are indexed by
a data catalog.

The data catalog is provided to allow code that needs access to hydrology data
to read the data remotely and without hard coding the location of files from the file share.

Installation
------------
You can install the python package with the API to access files using pip with::

    pip install git+https://github.com/hydroframe/hf_hydrodata.git

Example Usage
-------------
You can read data from any file in the hydrodata repository using python to get a numpy array with::

    from hf_hydrodata.data_access import get_numpy_data,get_data_catalog_entry

    filters = ["dataset":"NLDAS2", "variable":"precipitation", period="daily", "start_time": "2005-03-01", "file_type": "pfb"]
    data = get_numpy_data(filters)
    print(data.shape)

    # Get the metadata about the returned data
    entry = get_data_catalog_entry(filters)
    print(entry)

See the sub-section "Data Catalog" for information about parameters that can be passed to functions
to filter and identify data in the hydrodata file share.

Many of the files are very large so parameters can be provided to subset the files by space and/or time before
returning the data. See the sub-section about "Data Catalog" for details about the available parameters
that can be passed to the functions to filter data by space and/or time.

Data Catalog Data Model
-----------------------
The arguments to get_numpy_data locate the data using a set of metadata attributes about each type of data.
You filter the data catalog using values of these attributes to locate the data you want. In addition to attributes
used for filtering there are additional attributes used to describe the data.

See the sub-section "Data Catalog" for the list of attributes and values that are availble.


.. toctree::
   :maxdepth: 1
   :caption: API Reference:

   api_reference
   data_catalog
