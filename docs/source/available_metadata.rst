.. _available_metadata:

Metadata
========

In addition to the dataset attributes that can be used to filter datasets (e.g. dataset, variable, 
temporal_resolution, aggregation, and grid), we provide additional metadata for every dataset. 
The functions ``get_catalog_entry()`` and ``get_catalog_entries()`` can return a structure with all 
these attributes.
The specific metadata variables that are provided will vary based on the data source. For example many 
of the site-specific metadata attributes are only available for the specific point datasets that they come 
from. Please see :ref:`point_obs_metadata` for an itemized list of those site-specific fields.

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

.. _point_obs_metadata:

Point Observations Metadata
-----------------------------
Supplemental to the dataset-level metadata described above, the `get_point_metadata` function from hf_hydrodata's point module returns
site-level attributes, as described below. 

Standard Point Observations Metadata
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
There are several variables that are returned by all ``get_point_metadata()`` function calls.

.. list-table:: point_metadata
    :widths: 25 100
    :header-rows: 1

    * - Attribute
      - Description
    * - site_id
      -  Unique site-level identifier, as provided by the contributing agency.
    * - site_name
      - Descriptive name attributed to the site, as provided by the contributing agency.
    * - site_type
      - Broad class of site. Values include 'stream gauge', 'groundwater well', 'SNOTEL station', 'SCAN station', or 'flux tower'.
    * - agency
      - Contributing data source. Values include 'USGS', 'NRCS', or 'AmeriFlux'. 
    * - state
      - 2-digit state postal code for the site location, as provided by the contributing agency.
    * - latitude
      - Decimal degrees latitude for the site location, as provided by the contributing agency.
    * - longitude
      - Decimal degress longitude for the site location, as provided by the contributing agency.
    * - huc8
      - Hydrologic Unit Code (HUC) 8, if provided by the contributing agency.
    * - first_date_data_available
      - String 'YYYY-MM-DD' date measure of the overall earliest observation stored for a site. Note that some sites collect multiple types of variables and these dates can vary by variable type.
    * - last_date_data_available
      - String 'YYYY-MM-DD' date measure of the overall latest observation stored for a site. Note that this field is regularly updated as new data is collected each week. Note also that some sites collect multiple types of variables and these dates can vary by variable type.
    * - record_count
      - Constructed measure of the total number of observations available for a site, for the requested variable series. Note that some sites collect multiple types of variables and these counts can vary by variable type.
    * - site_query_url
      - If applicable, the query URL used to obtain site-level metadata from the contributing agency.
    * - date_metadata_last_updated
      - String 'YYYY-MM-DD' date measure of the date our site-level metadata was last queried from the contributing agency.
    * - tz_cd 
      - String representation of the site's time zone, if provided by the contributing agency.
    * - doi
      - If applicable, the DOI associated with the site, as provided by the contributing agency.
    * - conus1_x
      - Constructed value that maps the site's latitude/longitude to the CONUS1 grid.
    * - conus1_y
      - Constructed value that maps the site's latitude/longitude to the CONUS1 grid.
    * - conus2_x
      - Constructed value that maps the site's latitude/longitude to the CONUS2 grid.
    * - conus2_y
      - Constructed value that maps the site's latitude/longitude to the CONUS2 grid.


Stream Gage Metadata
^^^^^^^^^^^^^^^^^^^^^^^^
There are several site attributes that are specific to sites with ``site_type='stream gauge'``. These would
be produced when ``dataset='usgs_nwis'`` and ``variable='streamflow'``: 

.. list-table:: Stream Gage Metadata
    :widths: 25 100
    :header-rows: 1
    
    * - Attribute
      - Description
    * - gagesii_drainage_area
      -  Drainage area (square kilometers) for the site, as per the `GAGES-II metadata <https://www.sciencebase.gov/catalog/item/631405bbd34e36012efa304a>`_.
    * - gagesii_class
      - Site class, defined in the `GAGES-II metadata <https://www.sciencebase.gov/catalog/item/631405bbd34e36012efa304a>`_. Values include 'Ref' to indicate a reference gage, 'Non-ref' to indicate the site is not a reference gage, and 'nan' for sites that are not a part of the GAGES-II site network.
    * - gagesii_site_elevation
      - Site elevation (meters), as per the `GAGES-II metadata <https://www.sciencebase.gov/catalog/item/631405bbd34e36012efa304a>`_.
    * - usgs_drainage_area
      - Drainage area (square miles) for the site, as per the USGS.

Groundwater Well Metadata
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
There are several site attributes that are specific to sites with ``site_type='groundwater well'``. These would
be produced when ``dataset='usgs_nwis'`` and ``variable='water_table_depth'``:

.. list-table:: Groundwater Well Metadata
    :widths: 25 100
    :header-rows: 1
    
    * - Attribute
      - Description
    * - usgs_nat_aqfr_cd
      -  National aquifer code, as provided by the USGS.
    * - usgs_aqfr_cd
      - Local aquifer code, as provided by the USGS.
    * - usgs_aqfr_type_cd
      - Local aquifer type code, as provided by the USGS.
    * - usgs_well_depth
      - Well depth (feet), as provided by the USGS.
    * - usgs_hole_depth
      - Hole depth (feet), as provided by the USGS.
    * - usgs_hole_depth_src_cd
      - Source of hole depth data, as provided by the USGS.

SNOTEL and SCAN Station Metadata
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
There are several site attributes that are specific to sites with ``dataset='snotel'`` or 
``dataset='scan'``:

.. list-table:: SNOTEL/SCAN Station Metadata
    :widths: 25 100
    :header-rows: 1
    
    * - Attribute
      - Description
    * - usda_elevation
      -  Site elevation (feet), queried from the USDA. 

AmeriFlux Tower Metadata
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
There are several site attributes that are specific to sites with ``dataset='ameriflux'``:

.. list-table:: AmeriFlux Station Metadata
    :widths: 25 100
    :header-rows: 1
    
    * - Attribute
      - Description
    * - ameriflux_site_description
      -  Site description, as queried from AmeriFlux.
    * - ameriflux_elevation
      - Site elevation (meters), as queried from AmeriFlux.
    * - ameriflux_tower_type
      - Type of tower structure, as queried from AmeriFlux.
    * - ameriflux_igbp
      - International Geosphere-Biosphere Programme vegetation type, as queried from AmeriFlux.
    * - ameriflux_terrain
      - Categorical describing the site terrain, as queried from AmeriFlux.
    * - ameriflux_site_snow_cover_days
      - Number of days per year that the site is covered by snow, as queried from AmeriFlux.
    * - ameriflux_climate_koeppen
      - Koppen climate classification, as queried from AmeriFlux.
    * - ameriflux_mean_annual_temp
      - Long-term mean annual average air temperature (degrees C), as queried from AmeriFlux.
    * - ameriflux_mean_annual_precip
      - Long-term mean annual average precipitation (millimeters), as queried from AmeriFlux.
    * - ameriflux_team_member_name
      - Site team member name, as queried from AmeriFlux.
    * - ameriflux_team_member_role
      - Site team member role, as queried from AmeriFlux.
    * - ameriflux_team_member_email
      - Site team member email, as queried from AmeriFlux.
    * - ameriflux_team_member_institution
      - Site team member institution, as queried from AmeriFlux.
    * - ameriflux_site_funding
      - Agencies and institutions providing funding for the site, as queried from AmeriFlux.
    * - ameriflux_acknowledgement
      - Acknowledgement, as queried from AmeriFlux.
    * - ameriflux_acknowledgement_comment
      - Acknowledgement additional comments, as queried from AmeriFlux.
    * - ameriflux_doi_citation
      - DOI citation text for the site, as queried from AmeriFlux.
    * - ameriflux_alternate_url
      - URL to site information on AmeriFlux website, as queried from AmeriFlux.
