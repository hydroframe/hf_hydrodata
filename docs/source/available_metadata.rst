.. _available_metadata:

Metadata
========

In addition to the dataset attributes that can be used to filter datasets (e.g. dataset, variable, temporal_resolution, aggregation, and grid), we provide additional metadata for every dataset. The specific metadata variables that are provided will vary based on the data source. For example many of the site specific metadata attributes are only available for the specific point datasets that they come from. The functions ``get_catalog_entry()`` and ``get_catalog_entries()`` can return a structure with all these attributes.

The following are the additional metadata attributes describing a dataset.

.. list-table:: Metadata
    :widths: 25 100
    :header-rows: 1

    * - Attribute
      - Description
    * - paper_doi
      - A space seperated list of DOI references to papers that describe a dataset. Please cite the associated DOIs when using the data.
    * - units
      - The units of measure of the values in the data.
    * - file_type
      - The file type of the raw data file containing the data on the server.
    * - dataset_var
      - The name of a variable within a dataset. This is used for netcdf and other hierarchical file types where the names of variables within the file may not match exactly the variable name that is assigned in the data catalog.    
    * - description
      - A text description of the dataset containing the data.
    * - dataset_type
      - A catagory name associated with the dataset containing the data.
    * - structure_type
      - Distinguishes point data from gridded datasets.
    * - time_zone
      - The time zone of the data on the server. UTC-00:00 is [UTC](https://en.wikipedia.org/wiki/Coordinated_Universal_Time) time.  This is the timezone for the forcing data and other products. UTC-05:00 is EST time; (UTC-04:00 is EDT, e.g.).  *The user should check the time zone of the data and make sure they are intepreting this correctly in any simulations and analysis.*
    * - entry_start_date
      - The earliest date of available data for the dataset.
    * - entry_end_date
      - The latest date of available data for the dataset.
    * - file_grouping
      - Indicates how files of the catalog entry are grouped on the server (e.g. by water year (wy), by site id (site_id), by water year day number (wy_daynum))





