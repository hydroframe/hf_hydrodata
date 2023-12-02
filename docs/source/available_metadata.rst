.. _available_metadata:

Metadata
========

The previous sections describe attributes used to filter datasets. These include:
dataset, variable, temporal_resolution, aggregation, and grid.
In addition, there are other attributes that can be retrieved as metadata about an identified dataset.
The functions get_catalog_entry() and get_catalog_entries() can return a structure with all these attributes.

The following are the additional metadata attributes describing a dataset.

.. list-table:: Metadata
    :widths: 25 100
    :header-rows: 1

    * - Attribute
      - Description
    * - paper_doi
      - A space seperated list of DOI references to papers that describe a dataset.
    * - units
      - The units of measure of the values in the data.
    * - file_type
      - The file type of the raw data file containing the data on the server.
    * - dataset_var
      - The netcdf variable name containg the data in the raw data file on the server.    
    * - description
      - A text description of the dataset containing the data.
    * - dataset_type
      - A catagory name associated with the dataset containing the data.
    * - structure_type
      - Either point or gridded distinguish gridded data from point observation data.
    * - time_zone
      - The time zone of the data on the server. UTC-00:00 is UTC time. UTC-06:00 is EST time.
    * - entry_start_date
      - The earliest date of available data for the dataset.
    * - entry_end_date
      - The latest date of available data for the dataset.
    * - file_grouping
      - Indicates how files of the catalog entry are grouped on the server (wy, site_id, wy_daynum)





