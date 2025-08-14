"""Module to retrieve point observations."""

# pylint: disable=C0301,W0707,W0719,C0121,C0302,C0209,C0325,W0702
import datetime
from typing import Tuple
import io
import ast
import os
import json
import sqlite3
import warnings
import pandas as pd
import xarray as xr
import numpy as np
import requests
import shapefile
import pyproj
from shapely import contains_xy
from shapely.geometry import Point, shape
from shapely.ops import transform
from hf_hydrodata.gridded import (
    get_huc_bbox,
    get_gridded_data,
)
from hf_hydrodata.data_catalog import get_catalog_entry, _maintenance_guard


HYDRODATA = "/hydrodata"
DB_PATH = f"{HYDRODATA}/national_obs/point_obs.sqlite"
HYDRODATA_URL = os.getenv("HYDRODATA_URL", "https://hydrogen.princeton.edu")
NETWORK_LISTS_PATH = f"{HYDRODATA}/national_obs/tools/network_lists"

# Use this to check that user-supplied parameters are being used
SUPPORTED_FILTERS = [
    "dataset",
    "variable",
    "temporal_resolution",
    "aggregation",
    "depth_level",
    "date_start",
    "date_end",
    "latitude_range",
    "longitude_range",
    "site_ids",
    "state",
    "polygon",
    "polygon_crs",
    "site_networks",
    "min_num_obs",
    "grid",
    "grid_bounds",
    "huc_id",
]

# List of SQL tables in the database corresponding to site-type-specific attributes
SITE_ATTRIBUTE_TABLES = [
    "streamgauge_attributes",
    "well_attributes",
    "snotel_station_attributes",
    "flux_tower_attributes",
    "jasechko_attributes",
]

DEPTH_LEVELS = [2, 4, 8, 20, 40]


@_maintenance_guard
def get_point_data(*args, **kwargs):
    """
    Collect point observations data into a Pandas DataFrame.

    Observations collected from HydroData for the specified data source, variable, temporal
    resolution, and aggregation. Optional arguments can be supplied for filters such as
    date bounds, geography bounds, and/or the minimum number of per-site observations allowed.
    Please see the package documentation for the full set of supported combinations.

    Parameters
    ----------
    dataset : str, required
        Source from which requested data originated. Currently supported: 'usgs_nwis', 'snotel',
        'scan', 'ameriflux', 'jasechko_2024', 'fan_2013'.
    variable : str, required
        Description of type of data requested. Currently supported: 'streamflow', 'water_table_depth', 'swe',
        'precipitation', 'air_temp', 'soil_moisture', 'latent_heat', 'sensible_heat',
        'downward_shortwave', 'downward_longwave', 'vapor_pressure_deficit', 'wind_speed'.
    temporal_resolution : str, required
        Collection frequency of data requested. Currently supported: 'daily', 'hourly', 'instantaneous',
        'yearly', and 'long_term'. Please see the documentation for allowable combinations with `variable`.
    aggregation : str, required
        Additional information specifying the aggregation method for the variable to be returned.
        Options include descriptors such as 'mean' and 'sum'. Please see the documentation
        for allowable combinations with `variable`.
    depth_level : int, optional
        Depth level in inches at which the measurement is taken. Necessary for `variable` = 'soil_moisture'.
    date_start : str, optional
        A date provided as a string in 'YYYY-MM-DD' format. If provided, data will be returned from this
        date forward (inclusive).
    date_end : str, optional
        A date provided as a string in 'YYYY-MM-DD' format. If provided, data will be returned up
        through this date (inclusive).
    latitude_range : tuple, optional
        Latitude range bounds for the geographic domain; lesser value is provided first.
    longitude_range : tuple, optional
        Longitude range bounds for the geographic domain; lesser value is provided first.
    grid : str, optional
        Value of either 'conus1' or 'conus2'. Used in combination with parameter `grid_bounds`
        to extract site locations for a specific region of conus coordinates.
    grid_bounds : list of integers, optional
        A list of points [left, bottom, right, top] in ij grid coordinates of the grid supplied
        by the `grid` parameter.
    site_ids : str or list of strings, optional
        Single site ID string or list of desired (string) site identifiers.
    state : str, optional
        Two-letter postal code state abbreviation (example: state='NJ').
    huc_id : str or list of strings, optional
        Single HUC ID string or list of adjacent HUC ID strings.
    polygon : str, optional
        Path to location of shapefile. Must be readable by PyShp's `shapefile.Reader()`.
    polygon_crs : str, optional
        CRS definition accepted by `pyproj.CRS.from_user_input()`.
    site_networks: str or list of strings, optional
        Name(s) of site networks. Can be a string with a single network name, or a list of strings
        containing strings for multiple available networks. There are currently supported networks for
        stream gages (dataset=='usgs_nwis', variable='streamflow') and groundwater wells (dataset=='usgs_nwis',
        variable='water_table_depth'). For streamflow, options include: 'gagesii', 'gagesii_reference', 'hcdn2009',
        'camels', and 'nwm'. For water table depth, options include: 'climate_response_network'.
    min_num_obs : int, optional
        Value for the minimum number of observations desired for a site to have. If provided, data will
        be returned only for sites that have at least this number of non-NaN observation records within
        the requested date range (if supplied).

    Returns
    -------
    data_df : DataFrame
        DataFrame with columns for each site_id satisfying input filters. Rows represent
        the date range requested from date_start and/or date_end, or the broadest range of
        data available for returned sites if no date range is explicitly requested.

    If the environment variable HUC_VERSION is set this will cause the function to use the HUC boundaries for
    that dataset_version when HUC is passed as a option.
    The versions 2025_06, 2025_01, 2024_11 are supported as well as blank to use the latest HUC boundaries.
    """
    if len(args) > 0 and isinstance(args[0], dict):
        options = args[0]
    else:
        options = kwargs

    # Confirm that the "mandatory" inputs are all provided
    if (
        "dataset" not in options
        or "variable" not in options
        or "temporal_resolution" not in options
        or "aggregation" not in options
    ):
        raise ValueError(
            "You must specify a dataset, variable, temporal_resolution, and aggregation.  One or more of these query parameters is missing."
        )

    # Raise error if unrecognized parameter input
    for k in options.keys():
        if k not in SUPPORTED_FILTERS:
            raise ValueError(
                f"Supplied parameter {k} is not recognized. Please visit the package API documentation to see a description of supported parameters."
            )

    run_remote = not os.path.exists(HYDRODATA)

    if run_remote:
        # Cannot pass local shapefile to API; pass bounding box instead
        if "polygon" in options and options["polygon"] is not None:
            try:
                assert "polygon_crs" in options and options["polygon_crs"] is not None
            except:
                raise Exception(
                    """Please provide 'polygon_crs' with a CRS definition accepted by pyproj.CRS.from_user_input()
                   to specify this shape's CRS."""
                )
            latitude_range, longitude_range = _get_bbox_from_shape(
                options["polygon"], options["polygon_crs"]
            )

            # Send bounding box to API; remove polygon filters from API options
            polygon = options["polygon"]
            polygon_crs = options["polygon_crs"]
            polygon_filter = True
            del options["polygon"]
            del options["polygon_crs"]
            options["latitude_range"] = latitude_range
            options["longitude_range"] = longitude_range
        else:
            polygon_filter = False

        data_df = _get_data_from_api(
            "data_only",
            options,
        )

        # Re-filter on shapefile to trim bounding box
        if polygon_filter == True:
            # Use metadata call to get latitude/longitude for the sites
            metadata_df = _get_data_from_api(
                "metadata_only",
                options,
            )

            # Clip metadata to polygon. Use this new list of sites to filter data_df.
            clipped_metadata_df = _filter_on_polygon(metadata_df, polygon, polygon_crs)

            metadata_site_ids = list(clipped_metadata_df["site_id"])
            data_site_ids = list(data_df.columns)[1:]
            site_ids_to_drop = [s for s in data_site_ids if s not in metadata_site_ids]

            clipped_df = data_df.drop(columns=site_ids_to_drop)
            return clipped_df

        return data_df

    options = _convert_strings_to_type(options)

    # Create database connection
    conn = sqlite3.connect(DB_PATH)

    # Validation checks on inputs
    _check_inputs(
        options["dataset"],
        options["variable"],
        options["temporal_resolution"],
        options["aggregation"],
        options,
    )

    # Get associated variable IDs for requested data types and time periods
    var_id = _get_var_id(
        conn,
        options["dataset"],
        options["variable"],
        options["temporal_resolution"],
        options["aggregation"],
        options,
    )

    # Get site list
    sites_df = _get_sites(
        conn,
        options["dataset"],
        options["variable"],
        options["temporal_resolution"],
        options["aggregation"],
        options,
    )

    if len(sites_df) == 0:
        raise ValueError("There are zero sites that satisfy the given parameters.")

    # Get data
    site_list = list(sites_df["site_id"])

    if (var_id in (1, 2, 3, 4)) | (var_id in range(6, 25)):
        data_df = _get_data_nc(
            site_list,
            options["dataset"],
            options["variable"],
            options["temporal_resolution"],
            options["aggregation"],
            options,
        )

    elif var_id in (5, 25, 26):
        data_df = _get_data_sql(conn, site_list, var_id, *args, **kwargs)

    conn.close()

    return data_df.reset_index().drop("index", axis=1)


@_maintenance_guard
def get_point_metadata(*args, **kwargs):
    """
    Return DataFrame with site metadata for the filtered sites.

    Parameters
    ----------
    dataset : str, required
        Source from which requested data originated. Currently supported: 'usgs_nwis', 'snotel',
        'scan', 'ameriflux', 'jasechko_2024', 'fan_2013'.
    variable : str, required
        Description of type of data requested. Currently supported: 'streamflow', 'water_table_depth', 'swe',
        'precipitation', 'air_temp', 'soil_moisture', 'latent_heat', 'sensible_heat',
        'downward_shortwave', 'downward_longwave', 'vapor_pressure_deficit', 'wind_speed'.
    temporal_resolution : str, required
        Collection frequency of data requested. Currently supported: 'daily', 'hourly', 'instantaneous',
        'yearly', and 'multiyear'.
        Please see the documentation for allowable combinations with `variable`.
    aggregation : str, required
        Additional information specifying the aggregation method for the variable to be returned.
        Options include descriptors such as 'mean' and 'sum'. Please see the documentation
        for allowable combinations with `variable`.
    depth_level : int, optional
        Depth level in inches at which the measurement is taken. Necessary for `variable` = 'soil_moisture'.
    date_start : str, optional
        A date provided as a string in 'YYYY-MM-DD' format. If provided, data will be returned from this
        date forward (inclusive).
    date_end : str, optional
        A date provided as a string in 'YYYY-MM-DD' format. If provided, data will be returned up
        through this date (inclusive).
    latitude_range : tuple, optional
        Latitude range bounds for the geographic domain; lesser value is provided first.
    longitude_range : tuple, optional
        Longitude range bounds for the geographic domain; lesser value is provided first.
    grid : str, optional
        Value of either 'conus1' or 'conus2'. Used in combination with parameter `grid_bounds`
        to extract site locations for a specific region of conus coordinates.
    grid_bounds : list of integers, optional
        A list of points [left, bottom, right, top] in ij grid coordinates of the grid supplied
        by the `grid` parameter.
    site_ids : str or list of strings, optional
        Single site ID string or list of desired (string) site identifiers.
    state : str, optional
        Two-letter postal code state abbreviation (example: state='NJ').
    huc_id : str or list of strings, optional
        Single HUC ID string or list of adjacent HUC ID strings.
    polygon : str, optional
        Path to location of shapefile. Must be readable by PyShp's `shapefile.Reader()`.
    polygon_crs : str, optional
        CRS definition accepted by `pyproj.CRS.from_user_input()`.
    site_networks: str or list of strings, optional
        Name(s) of site networks. Can be a string with a single network name, or a list of strings
        containing strings for multiple available networks. There are currently supported networks for
        stream gages (dataset=='usgs_nwis', variable='streamflow') and groundwater wells (dataset=='usgs_nwis',
        variable='water_table_depth'). For streamflow, options include: 'gagesii', 'gagesii_reference', 'hcdn2009',
        'camels', and 'nwm'. For water table depth, options include: 'climate_response_network'.

    Returns
    -------
    DataFrame
        Site-level DataFrame of site-level metadata.
    """
    if len(args) > 0 and isinstance(args[0], dict):
        options = args[0]
    else:
        options = kwargs

    # Confirm that the "mandatory" inputs are all provided
    if (
        "dataset" not in options
        or "variable" not in options
        or "temporal_resolution" not in options
        or "aggregation" not in options
    ):
        raise ValueError(
            "You must specify a dataset, variable, temporal_resolution, and aggregation.  One or more of these query parameters is missing."
        )

    # Raise error if unrecognized parameter input
    for k in options.keys():
        if k not in SUPPORTED_FILTERS:
            raise ValueError(
                f"Supplied parameter {k} is not recognized. Please visit the package API documentation to see a description of supported parameters."
            )

    run_remote = not os.path.exists(HYDRODATA)

    if run_remote:
        # Cannot pass local shapefile to API; pass bounding box instead
        if "polygon" in options and options["polygon"] is not None:
            try:
                assert "polygon_crs" in options and options["polygon_crs"] is not None
            except:
                raise Exception(
                    """Please provide 'polygon_crs' with a CRS definition accepted by pyproj.CRS.from_user_input()
                   to specify this shape's CRS."""
                )
            latitude_range, longitude_range = _get_bbox_from_shape(
                options["polygon"], options["polygon_crs"]
            )

            # Send bounding box to API; remove polygon filters from API options
            polygon = options["polygon"]
            polygon_crs = options["polygon_crs"]
            polygon_filter = True
            del options["polygon"]
            del options["polygon_crs"]
            options["latitude_range"] = latitude_range
            options["longitude_range"] = longitude_range
        else:
            polygon_filter = False

        data_df = _get_data_from_api(
            "metadata_only",
            options,
        )

        # Re-filter on shapefile to trim bounding box
        if polygon_filter == True:
            clipped_df = _filter_on_polygon(data_df, polygon, polygon_crs)
            return clipped_df

        return data_df

    options = _convert_strings_to_type(options)

    # Create database connection
    conn = sqlite3.connect(DB_PATH)

    metadata_df = _get_sites(
        conn,
        options["dataset"],
        options["variable"],
        options["temporal_resolution"],
        options["aggregation"],
        options,
    )

    # Clean up HUC to string of appropriate length
    metadata_df["huc8"] = metadata_df["huc"].apply(_clean_huc)
    metadata_df.drop(columns=["huc"], inplace=True)

    # Merge on additional metadata attribute tables as needed
    site_ids = list(metadata_df["site_id"])

    if "stream gauge" in metadata_df["site_type"].unique():
        attributes_df = pd.read_sql_query(
            """SELECT site_id, conus1_i, conus1_j, conus2_i, conus2_j,
                      conus2_i_nwm, conus2_j_nwm,
                      gages_drainage_sqkm AS gagesii_drainage_area,
                      class AS gagesii_class,
                      site_elevation_meters AS gagesii_site_elevation,
                      drain_area_va AS usgs_drainage_area
               FROM streamgauge_attributes WHERE site_id IN (%s)"""
            % ",".join("?" * len(site_ids)),
            conn,
            params=site_ids,
        )
        metadata_df = pd.merge(metadata_df, attributes_df, how="left", on="site_id")

    if ("groundwater well" in metadata_df["site_type"].unique()) and (
        options["dataset"] == "usgs_nwis"
    ):
        attributes_df = pd.read_sql_query(
            """SELECT site_id, conus1_i, conus1_j, conus2_i, conus2_j,
                      nat_aqfr_cd AS usgs_nat_aqfr_cd,
                      aqfr_cd AS usgs_aqfr_cd,
                      aqfr_type_cd AS usgs_aqfr_type_cd,
                      well_depth_va AS usgs_well_depth,
                      hole_depth_va AS usgs_hole_depth,
                      depth_src_cd AS usgs_hole_depth_src_cd
               FROM well_attributes WHERE site_id IN (%s)"""
            % ",".join("?" * len(site_ids)),
            conn,
            params=site_ids,
        )
        metadata_df = pd.merge(metadata_df, attributes_df, how="left", on="site_id")

        # For "instantaneous" groundwater data only, also filter on the sites that actually
        # have observations. This involves actually reading in and filtering the observations
        # data. Otherwise, get_point_metadata only reads in site-level metadata to keep it fast.
        # Because this "instantaneous" data is stored sparsely, the user does not get back
        # NaN series for sites that are located within the filter but that don't have observations
        # for the requested time period. Therefore, it is not obvious to users why there is
        # a discrepancy in unique site IDs between the data DataFrame returned by get_point_data and
        # the metadata DataFrame returned by get_point_metadata when querying for "instantaneous" wtd.
        # For this data source only, this additional filtering will sacrifice some speed (to actually
        # query the data) with better interpretability.
        if options["temporal_resolution"] == "instantaneous":
            wtd_data_df = _get_data_sql(conn, site_ids, 5, options)
            wtd_sites_with_data = list(wtd_data_df["site_id"].unique())
            metadata_df = pd.merge(
                metadata_df,
                pd.DataFrame(data=wtd_sites_with_data, columns=["site_id"]),
                on="site_id",
                how="inner",
            )

    if ("groundwater well" in metadata_df["site_type"].unique()) and (
        options["dataset"] == "jasechko_2024"
    ):
        attributes_df = pd.read_sql_query(
            """SELECT site_id, conus1_i, conus1_j, conus2_i, conus2_j, usgs_site
               FROM jasechko_attributes WHERE site_id IN (%s)"""
            % ",".join("?" * len(site_ids)),
            conn,
            params=site_ids,
        )
        metadata_df = pd.merge(metadata_df, attributes_df, how="left", on="site_id")

    if ("groundwater well" in metadata_df["site_type"].unique()) and (
        options["dataset"] == "fan_2013"
    ):
        attributes_df = pd.read_sql_query(
            """SELECT site_id, conus1_i, conus1_j, conus2_i, conus2_j
               FROM well_attributes WHERE site_id IN (%s)"""
            % ",".join("?" * len(site_ids)),
            conn,
            params=site_ids,
        )
        metadata_df = pd.merge(metadata_df, attributes_df, how="left", on="site_id")
        metadata_df["doi"] = "10.1126/science.1229881"

    if ("SNOTEL station" in metadata_df["site_type"].unique()) or (
        "SCAN station" in metadata_df["site_type"].unique()
    ):
        attributes_df = pd.read_sql_query(
            """SELECT site_id, conus1_i, conus1_j, conus2_i, conus2_j,
                      elevation AS usda_elevation
               FROM snotel_station_attributes WHERE site_id IN (%s)"""
            % ",".join("?" * len(site_ids)),
            conn,
            params=site_ids,
        )
        metadata_df = pd.merge(metadata_df, attributes_df, how="left", on="site_id")

    if "flux tower" in metadata_df["site_type"].unique():
        attributes_df = pd.read_sql_query(
            """SELECT site_id, conus1_i, conus1_j, conus2_i, conus2_j,
                      site_description AS ameriflux_site_description,
                      elevation AS ameriflux_elevation,
                      tower_type AS ameriflux_tower_type,
                      igbp AS ameriflux_igbp,
                      terrain AS ameriflux_terrain,
                      site_snow_cover_days AS ameriflux_site_snow_cover_days,
                      climate_koeppen AS ameriflux_climate_koeppen,
                      mean_annual_temp AS ameriflux_mean_annual_temp,
                      mean_annual_precip AS ameriflux_mean_annual_precip,
                      team_member_name AS ameriflux_team_member_name,
                      team_member_role AS ameriflux_team_member_role,
                      team_member_email AS ameriflux_team_member_email,
                      team_member_institution AS ameriflux_team_member_institution,
                      site_funding AS ameriflux_site_funding,
                      acknowledgement AS ameriflux_acknowledgement,
                      acknowledgement_comment AS ameriflux_acknowledgement_comment,
                      doi_citation AS ameriflux_doi_citation,
                      alternate_url AS ameriflux_alternate_url
               FROM flux_tower_attributes WHERE site_id IN (%s)"""
            % ",".join("?" * len(site_ids)),
            conn,
            params=site_ids,
        )
        metadata_df = pd.merge(metadata_df, attributes_df, how="left", on="site_id")

    conn.close()
    return metadata_df


@_maintenance_guard
def get_site_variables(*args, **kwargs):
    """
    Return DataFrame with available sites, variables, and the period of record.

    Parameters
    ----------
    dataset : str, optional
        Source from which requested data originated. Currently supported: 'usgs_nwis', 'snotel',
        'scan', 'ameriflux', 'jasechko_2024', and 'fan_2013'.
    variable : str, optional
        Description of type of data requested. Currently supported: 'streamflow', 'water_table_depth', 'swe',
        'precipitation', 'air_temp', 'soil_moisture', 'latent_heat', 'sensible_heat',
        'downward_shortwave', 'downward_longwave', 'vapor_pressure_deficit', 'wind_speed'.
    temporal_resolution : str, optional
        Collection frequency of data requested. Currently supported: 'daily', 'hourly', 'instantaneous',
        'yearly', and 'long_term'.
        Please see the documentation for allowable combinations with `variable`.
    aggregation : str, optional
        Additional information specifying the aggregation method for the variable to be returned.
        Options include descriptors such as 'mean' and 'sum'. Please see the documentation
        for allowable combinations with `variable`.
    date_start : str, optional
        A date provided as a string in 'YYYY-MM-DD' format. If provided, data will be returned from this
        date forward (inclusive).
    date_end : str, optional
        A date provided as a string in 'YYYY-MM-DD' format. If provided, data will be returned up
        through this date (inclusive).
    latitude_range : tuple, optional
        Latitude range bounds for the geographic domain; lesser value is provided first.
    longitude_range : tuple, optional
        Longitude range bounds for the geographic domain; lesser value is provided first.
    grid : str, optional
        Value of either 'conus1' or 'conus2'. Used in combination with parameter `grid_bounds`
        to extract site locations for a specific region of conus coordinates.
    grid_bounds : list of integers, optional
        A list of points [left, bottom, right, top] in ij grid coordinates of the grid supplied
        by the `grid` parameter.
    site_ids : str or list of strings, optional
        Single site ID string or list of desired (string) site identifiers.
    state : str, optional
        Two-letter postal code state abbreviation (example: state='NJ').
    huc_id : str or list of strings, optional
        Single HUC ID string or list of adjacent HUC ID strings.
    polygon : str, optional
        Path to location of shapefile. Must be readable by PyShp's `shapefile.Reader()`.
    polygon_crs : str, optional
        CRS definition accepted by `pyproj.CRS.from_user_input()`.
    site_networks: str or list of strings, optional
        Name(s) of site networks. Can be a string with a single network name, or a list of strings
        containing strings for multiple available networks. There are currently supported networks for
        stream gages (dataset=='usgs_nwis', variable='streamflow') and groundwater wells (dataset=='usgs_nwis',
        variable='water_table_depth'). For streamflow, options include: 'gagesii', 'gagesii_reference', 'hcdn2009',
        'camels', and 'nwm'. For water table depth, options include: 'climate_response_network'.

    Returns
    -------
    DataFrame
        DataFrame unique by site_id and variable_name containing site- and variable-level metadata.
    """
    if len(args) > 0 and isinstance(args[0], dict):
        options = args[0]
    else:
        options = kwargs

    run_remote = not os.path.exists(HYDRODATA)

    if run_remote:
        # Cannot pass local shapefile to API; pass bounding box instead
        if "polygon" in options and options["polygon"] is not None:
            try:
                assert "polygon_crs" in options and options["polygon_crs"] is not None
            except:
                raise Exception(
                    """Please provide 'polygon_crs' with a CRS definition accepted by pyproj.CRS.from_user_input()
                   to specify this shape's CRS."""
                )
            latitude_range, longitude_range = _get_bbox_from_shape(
                options["polygon"], options["polygon_crs"]
            )

            # Send bounding box to API; remove polygon filters from API options
            polygon = options["polygon"]
            polygon_crs = options["polygon_crs"]
            polygon_filter = True
            del options["polygon"]
            del options["polygon_crs"]
            options["latitude_range"] = latitude_range
            options["longitude_range"] = longitude_range
        else:
            polygon_filter = False

        data_df = _get_siteid_data_from_api(
            options,
        )

        # Re-filter on shapefile to trim bounding box
        if polygon_filter == True:
            # Clip metadata to polygon. Use this new list of sites to filter data_df.
            clipped_df = _filter_on_polygon(data_df, polygon, polygon_crs)
            return clipped_df

        return data_df

    kwargs = _convert_strings_to_type(options)

    # Raise error immediately if no filtering parameters supplied (return DataFrame would be too large).
    if len(options.keys()) == 0:
        raise ValueError(
            "You did not provide any filtering parameters. Please provide some filtering parameters to narrow down your search."
        )

    # Create database connection
    conn = sqlite3.connect(DB_PATH)

    # Initialize parameter list to SQL queries
    param_list = []

    # Data source
    if "dataset" in options and options["dataset"] is not None:
        try:
            assert options["dataset"] in [
                "usgs_nwis",
                "snotel",
                "scan",
                "ameriflux",
                "jasechko_2024",
                "fan_2013",
            ]
        except:
            raise ValueError(
                f"dataset must be one of 'usgs_nwis', 'snotel', 'scan', 'ameriflux', 'jasechko_2024', 'fan_2013'. You provided {options['dataset']}"
            )

        if options["dataset"] == "usgs_nwis":
            dataset_query = " AND agency == ?"
            param_list.append("USGS")
        elif options["dataset"] == "ameriflux":
            dataset_query = " AND agency == ?"
            param_list.append("AmeriFlux")
        elif options["dataset"] == "jasechko_2024":
            dataset_query = " AND agency == ?"
            param_list.append("Jasechko_et_al_2024")
        elif options["dataset"] == "snotel":
            dataset_query = " AND site_type == ?"
            param_list.append("SNOTEL station")
        elif options["dataset"] == "scan":
            dataset_query = " AND site_type == ?"
            param_list.append("SCAN station")
        elif options["dataset"] == "fan_2013":
            dataset_query = " AND fan_2013 == 1 AND var_id == 26"
    else:
        dataset_query = ""

    # Date start
    if "date_start" in options and options["date_start"] is not None:
        date_start_query = " AND last_date_data_available >= ?"
        param_list.append(options["date_start"])
    else:
        date_start_query = ""

    # Date end
    if "date_end" in options and options["date_end"] is not None:
        date_end_query = " AND first_date_data_available <= ?"
        param_list.append(options["date_end"])
    else:
        date_end_query = ""

    # Latitude
    if "latitude_range" in options and options["latitude_range"] is not None:
        lat_query = " AND latitude BETWEEN ? AND ?"
        param_list.append(options["latitude_range"][0])
        param_list.append(options["latitude_range"][1])
    else:
        lat_query = ""

    # Longitude
    if "longitude_range" in options and options["longitude_range"] is not None:
        lon_query = " AND longitude BETWEEN ? AND ?"
        param_list.append(options["longitude_range"][0])
        param_list.append(options["longitude_range"][1])
    else:
        lon_query = ""

    # CONUS grid bounds
    if "grid_bounds" in options and options["grid_bounds"] is not None:
        # Make sure that the option "grid" is defined
        try:
            assert "grid" in options and options["grid"] in ("conus1", "conus2")
        except:
            raise ValueError(
                "When providing the parameter `grid_bounds`, please also provide the parameter `grid` as either 'conus1' or 'conus2'."
            )

        grid = options["grid"]
        grid_bounds = options["grid_bounds"]

        grid_bounds_sites = []

        for tbl in SITE_ATTRIBUTE_TABLES:
            grid_bounds_query = f"""SELECT site_id, {grid}_i, {grid}_j
                            FROM {tbl}
                            WHERE {grid}_i >= {grid_bounds[0]}
                              AND {grid}_j >= {grid_bounds[1]}
                              AND {grid}_i <= {grid_bounds[2]}
                              AND {grid}_j <= {grid_bounds[3]}
                         """
            grid_bounds_df = pd.read_sql_query(grid_bounds_query, conn)
            grid_bounds_sites.extend(list(grid_bounds_df["site_id"]))

        if len(grid_bounds_sites) > 0:
            grid_bounds_query = " AND s.site_id IN (%s)" % ",".join(
                "?" * len(grid_bounds_sites)
            )
            for s in grid_bounds_sites:
                param_list.append(s)
        else:
            raise Exception("There are no sites within the provided grid_bounds.")
    else:
        grid_bounds_query = ""

    # HUC ID filter
    if "huc_id" in options and options["huc_id"] is not None:
        huc_query, param_list = _get_huc_query(options, param_list, conn)
    else:
        huc_query = ""

    # Site ID
    if "site_ids" in options and options["site_ids"] is not None:
        if isinstance(options["site_ids"], list):
            site_query = " AND s.site_id IN (%s)" % ",".join(
                "?" * len(options["site_ids"])
            )
            for s in options["site_ids"]:
                param_list.append(s)
        elif isinstance(options["site_ids"], str):
            site_query = " AND s.site_id == ?"
            param_list.append(options["site_ids"])
        else:
            raise ValueError(
                "Parameter site_ids must be either a single site ID string, or a list of site ID strings"
            )
    else:
        site_query = ""

    # State
    if "state" in options and options["state"] is not None:
        state_query = " AND state == ?"
        param_list.append(options["state"])
    else:
        state_query = ""

    # Site Networks
    if "site_networks" in options and options["site_networks"] is not None:
        try:
            assert "dataset" in options and options["dataset"] is not None
            assert "variable" in options and options["variable"] is not None
        except:
            raise ValueError(
                "Please provide parameter values for dataset and variable if specifying site_networks"
            )
        network_site_list = _get_network_site_list(
            options["dataset"], options["variable"], options["site_networks"]
        )
        network_query = """ AND s.site_id IN (%s)""" % ",".join(
            "?" * len(network_site_list)
        )
        for s in network_site_list:
            param_list.append(s)
    else:
        network_query = ""

    query = (
        """
            SELECT s.site_id, s.site_name, s.site_type, s.agency, s.state,
                   o.var_id, o.first_date_data_available,
                   o.last_date_data_available, o.record_count,
                   s.latitude, s.longitude,  s.site_query_url,
                   s.date_metadata_last_updated, s.tz_cd, s.doi
            FROM sites s
            INNER JOIN observations o
            ON s.site_id = o.site_id
            WHERE first_date_data_available <> 'None'
            """
        + dataset_query
        + date_start_query
        + date_end_query
        + lat_query
        + lon_query
        + grid_bounds_query
        + huc_query
        + site_query
        + state_query
        + network_query
    )

    df = pd.read_sql_query(query, conn, params=param_list)

    # Polygon shapefile provided
    if "polygon" in options and options["polygon"] is not None:
        # Make sure CRS aligns between polygon and lat/lon points
        try:
            assert "polygon_crs" in options and options["polygon_crs"] is not None
        except:
            raise Exception(
                """Please provide 'polygon_crs' with a CRS definition accepted by pyproj.CRS.from_user_input()
                   to specify this shape's CRS."""
            )

        clipped_df = _filter_on_polygon(df, options["polygon"], options["polygon_crs"])
        df = clipped_df.copy()

    # Map var_id into variable, temporal_resolution, aggregation
    variables = _get_variables(conn)
    merged = pd.merge(df, variables, on="var_id", how="left")

    # Add filter based on variable name
    if "variable" in options and options["variable"] is not None:
        merged = merged.loc[merged["variable"] == options["variable"]]

    # Add filter based on temporal_resolution
    if "temporal_resolution" in options and options["temporal_resolution"] is not None:
        merged = merged.loc[
            merged["temporal_resolution"] == options["temporal_resolution"]
        ]

    if "aggregation" in options and options["aggregation"] is not None:
        merged = merged.loc[merged["aggregation"] == options["aggregation"]]

    # Drop extra columns
    final_df = merged.drop(
        columns=[
            "var_id",
            "variable_type",
            "depth_level",
        ]
    )

    # Rename "data_source" to "dataset"
    final_df = final_df.rename(columns={"data_source": "dataset"})

    # Re-order final columns
    ordered_cols = [
        "site_id",
        "site_name",
        "site_type",
        "agency",
        "state",
        "variable_name",
        "units",
        "dataset",
        "variable",
        "temporal_resolution",
        "aggregation",
        "first_date_data_available",
        "last_date_data_available",
        "record_count",
        "latitude",
        "longitude",
        "site_query_url",
        "date_metadata_last_updated",
        "tz_cd",
        "doi",
    ]

    final_df = final_df[ordered_cols]

    # Merge on conus grid mappings
    final_site_list = list(final_df["site_id"].unique())
    conus_map_df = pd.DataFrame(
        columns=(["site_id", "conus1_i", "conus1_j", "conus2_i", "conus2_j"])
    )

    for tbl in SITE_ATTRIBUTE_TABLES:
        conus_map_query = f"""SELECT site_id, conus1_i, conus1_j, conus2_i, conus2_j
                              FROM {tbl}
                              WHERE site_id IN (%s)""" % ",".join(
            "?" * len(final_site_list)
        )
        tbl_df = pd.read_sql_query(conus_map_query, conn, params=final_site_list)
        if len(tbl_df) > 0:
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=FutureWarning)
                conus_map_df = pd.concat(
                    [conus_map_df, tbl_df], axis=0, ignore_index=True
                )

    final_df = pd.merge(final_df, conus_map_df, on="site_id", how="left")
    conn.close()
    return final_df


def _get_siteid_data_from_api(options):
    options = _convert_params_to_string_dict(options)

    q_params = _construct_siteids_string_from_qparams(options)

    point_data_url = f"{HYDRODATA_URL}/api/site-variables-dataframe?{q_params}"

    try:
        headers = _validate_user()
        response = requests.get(point_data_url, headers=headers, timeout=180)
        if response.status_code != 200:
            raise ValueError(f"{response.content}.")

    except requests.exceptions.Timeout as e:
        raise ValueError(f"The point_data_url {point_data_url} has timed out.") from e

    data_df = pd.read_pickle(io.BytesIO(response.content))
    return data_df


def _get_data_from_api(data_type, options):
    options = _convert_params_to_string_dict(options)

    q_params = _construct_string_from_qparams(data_type, options)

    point_data_url = f"{HYDRODATA_URL}/api/point-data-dataframe?{q_params}"

    try:
        headers = _validate_user()
        response = requests.get(point_data_url, headers=headers, timeout=180)
        if response.status_code != 200:
            if response.status_code == 400:
                content = response.content.decode()
                response_json = json.loads(content)
                message = response_json.get("message")
                raise ValueError(message)
            if response.status_code == 502:
                raise ValueError(
                    "Timeout error from server. Try again later or try to reduce the size of data in the API request using time or space filters."
                )
            raise ValueError(
                f"The  {point_data_url} returned error code {response.status_code}."
            )
    except requests.exceptions.ChunkedEncodingError as ce:
        raise ValueError(
            "Timeout error from server. Try again later or try to reduce the size of data in the API request using time or space filters."
        ) from ce
    except requests.exceptions.Timeout as te:
        raise ValueError(
            "Timeout error from server. Try again later or try to reduce the size of data in the API request using time or space filters."
        ) from te

    data_df = pd.read_pickle(io.BytesIO(response.content))
    return data_df


def _get_registered_api_pin() -> Tuple[str, str]:
    """
    Get the email and pin registered by the current user.

    Returns:
        A tuple (email, pin)
    Raises:
        ValueError if no email/pin was registered
    """

    pin_dir = os.path.expanduser("~/.hydrodata")
    pin_path = f"{pin_dir}/pin.json"
    if not os.path.exists(pin_path):
        raise ValueError(
            "No email/pin was registered. Use the register_api() method to register the pin you created at the website."
        )
    try:
        with open(pin_path, "r") as stream:
            contents = stream.read()
            parsed_contents = json.loads(contents)
            email = parsed_contents.get("email")
            pin = parsed_contents.get("pin")
            return (email, pin)
    except Exception as e:
        raise ValueError(
            "No email/pin was registered. Use the register_api() method to register the pin you created at the website."
        ) from e


def _convert_params_to_string_dict(options):
    """
    Converts types other than strings to strings.

    Parameters
    ----------
    options : dictionary
        request options.
    """

    for key, value in options.items():
        if key == "depth_level":
            if not isinstance(value, str):
                options[key] = str(value)
        if key == "latitude_range":
            if not isinstance(value, str):
                options[key] = str(value)
        if key == "longitude_range":
            if not isinstance(value, str):
                options[key] = str(value)
        if key == "grid_bounds":
            if not isinstance(value, str):
                options[key] = str(value)
        if key == "site_ids":
            if not isinstance(value, str):
                options[key] = str(value)
        if key == "huc_id":
            if not isinstance(value, str):
                options[key] = str(value)
        if key == "min_num_obs":
            if not isinstance(value, str):
                options[key] = str(value)
        if key == "site_networks":
            if not isinstance(value, str):
                options[key] = str(value)
    return options


def _convert_strings_to_type(options):
    """
    Converts strings to relevant types.

    Parameters
    ----------
    options : dictionary
        request options.
    """

    for key, value in options.items():
        if key == "depth_level":
            if isinstance(value, str):
                options[key] = int(value)
        if key == "latitude_range":
            if isinstance(value, str):
                options[key] = ast.literal_eval(value)
        if key == "longitude_range":
            if isinstance(value, str):
                options[key] = ast.literal_eval(value)
        if key == "grid_bounds":
            if isinstance(value, str):
                options[key] = ast.literal_eval(value)
        if key == "site_ids":
            if isinstance(value, str):
                try:
                    options[key] = ast.literal_eval(value)
                    if isinstance(options[key], int):
                        options[key] = value
                except:
                    options[key] = value  # when site_id is a single str
        if key == "huc_id":
            if isinstance(value, str):
                try:
                    options[key] = ast.literal_eval(value)
                    if isinstance(options[key], int):
                        options[key] = value
                except:
                    options[key] = value  # when huc_id is a single str
        if key == "site_networks":
            if isinstance(value, str):
                try:
                    options[key] = ast.literal_eval(value)
                except:
                    options[key] = value  # when site_networks is a single str
        if key == "min_num_obs":
            if isinstance(value, str):
                options[key] = int(value)
    return options


def _construct_siteids_string_from_qparams(options):
    """
    Constructs the query parameters from the entry and options provided.

    Parameters
    ----------
    entry : hydroframe.data_catalog.data_model_access.ModelTableRow
        variable to be downloaded.
    options : dictionary
        datast to which the variable belongs.

    Returns
    -------
    data : numpy array
        the requested data.
    """
    string_parts = [
        f"{name}={value}" for name, value in options.items() if value is not None
    ]
    result_string = "&".join(string_parts)
    return result_string


def _construct_string_from_qparams(data_type, options):
    """
    Constructs the query parameters from the entry and options provided.

    Parameters
    ----------
    entry : hydroframe.data_catalog.data_model_access.ModelTableRow
        variable to be downloaded.
    options : dictionary
        datast to which the variable belongs.

    Returns
    -------
    data : numpy array
        the requested data.
    """

    string_parts = [
        f"{name}={value}" for name, value in options.items() if value is not None
    ]
    string_parts.append(f"data_type={data_type}")
    result_string = "&".join(string_parts)

    return result_string


def _validate_user():
    email, pin = _get_registered_api_pin()
    url_security = f"{HYDRODATA_URL}/api/api_pins?pin={pin}&email={email}"
    response = requests.get(url_security, headers=None, timeout=15)
    if not response.status_code == 200:
        raise ValueError(
            f"PIN has expired. Re-register a pin for '{email}' with https://hydrogen.princeton.edu/pin ."
        )
    json_string = response.content.decode("utf-8")
    jwt_json = json.loads(json_string)
    expires_string = jwt_json.get("expires")
    if expires_string:
        expires = datetime.datetime.strptime(
            expires_string, "%Y/%m/%d %H:%M:%S GMT-0000"
        )
        now = datetime.datetime.now()
        if now > expires:
            raise ValueError(
                "PIN has expired. Please re-register it from https://hydrogen.princeton.edu/pin"
            )
    jwt_token = jwt_json["jwt_token"]
    headers = {}
    headers["Authorization"] = f"Bearer {jwt_token}"
    return headers


def _get_variables(conn):
    """
    Get list of stored variables.
    Parameters
    ----------
    conn : Connection object
        The Connection object associated with the SQLite database to query from.
    Returns
    -------
    DataFrame
        DataFrame containing the entries from the variables SQLite table.
    """
    query = """
            SELECT *
            FROM variables
            """
    variables = pd.read_sql_query(query, conn)
    return variables


def _check_inputs(dataset, variable, temporal_resolution, aggregation, *args, **kwargs):
    """
    Checks on inputs to get_observations function.

    Parameters
    ----------
    dataset : str
        Source from which requested data originated. Currently supported: 'usgs_nwis', 'snotel',
        'scan', 'ameriflux', 'jasechko_2024', 'fan_2013'.
    variable : str, required
        Description of type of data requested. Currently supported: 'streamflow', 'water_table_depth', 'swe',
        'precipitation', 'air_temp', 'soil_moisture', 'latent_heat', 'sensible_heat',
        'downward_shortwave', 'downward_longwave', 'vapor_pressure_deficit', 'wind_speed'.
    temporal_resolution : str
        Collection frequency of data requested. Currently supported: 'daily', 'hourly', 'instantaneous',
        'yearly', 'long_term'.
    aggregation : str
        Additional information specifying the aggregation method for the variable to be returned.
        Options include descriptors such as 'mean' and 'sum'. Please see the documentation
        for allowable combinations with `variable`.
    args :
        Optional positional parameters that must be a dict with filter options.
    kwargs :
        Supports multiple named parameters with filter option values.

    Keyword Arguments
    --------------------
    depth_level : int
        Depth level in inches at which the measurement is taken. Necessary for `variable` = 'soil_moisture'.

    Returns
    -------
    None
    """
    if len(args) > 0 and isinstance(args[0], dict):
        options = args[0]
    else:
        options = kwargs

    try:
        assert temporal_resolution in [
            "daily",
            "hourly",
            "instantaneous",
            "yearly",
            "long_term",
        ]
    except:
        raise ValueError(
            f"Unexpected value for temporal_resolution, {temporal_resolution}. Please see the documentation for allowed values."
        )

    try:
        assert variable in [
            "streamflow",
            "water_table_depth",
            "swe",
            "precipitation",
            "air_temp",
            "soil_moisture",
            "latent_heat",
            "sensible_heat",
            "downward_shortwave",
            "downward_longwave",
            "vapor_pressure_deficit",
            "wind_speed",
        ]
    except:
        raise ValueError(
            f"Unexpected value for variable, {variable}. Please see the documentation for allowed values."
        )

    try:
        assert aggregation in [
            "mean",
            "median",
            "instantaneous",
            "-",
            "sum",
            "sum_snow_adjusted",
            "sod",
            "accumulated",
            "min",
            "max",
        ]
    except:
        raise ValueError(
            f"Unexpected value for aggregation, {aggregation}. Please see the documentation for allowed values."
        )

    try:
        assert dataset in [
            "usgs_nwis",
            "snotel",
            "scan",
            "ameriflux",
            "jasechko_2024",
            "fan_2013",
        ]
    except:
        raise ValueError(
            f"Unexpected value for dataset, {dataset} Please see the documentation for allowed values."
        )

    if variable == "soil_moisture":
        try:
            assert "depth_level" in options
            assert options["depth_level"] in DEPTH_LEVELS
        except:
            raise ValueError(
                "Please provide depth_level with one of the supported values. Please see the documentation for allowed values."
            )
    else:
        try:
            assert "depth_level" not in options or options["depth_level"] is None
        except:
            raise ValueError(
                "Parameter depth_level is only supported when variable=='soil_moisture'."
            )


def _get_var_id(
    conn, dataset, variable, temporal_resolution, aggregation, *args, **kwargs
):
    """
    Return mapped var_id.

    Parameters
    ----------
    conn : Connection object
        The Connection object associated with the SQLite database to query from.
    dataset : str
        Source from which requested data originated. Currently supported: 'usgs_nwis', 'snotel',
        'scan', 'ameriflux', 'jasechko_2024'.
    variable : str, required
        Description of type of data requested. Currently supported: 'streamflow', 'water_table_depth', 'swe',
        'precipitation', 'air_temp', 'soil_moisture', 'latent_heat', 'sensible_heat',
        'downward_shortwave', 'downward_longwave', 'vapor_pressure_deficit', 'wind_speed'.
    temporal_resolution : str
        Collection frequency of data requested. Currently supported: 'daily', 'hourly', 'instantaneous', and
        'yearly'.
    aggregation : str
        Additional information specifying the aggregation method for the variable to be returned.
        Options include descriptors such as 'mean' and 'sum'. Please see the documentation
        for allowable combinations with `variable`.
    args :
        Optional positional parameters that must be a dict with filter options.
    kwargs :
        Supports multiple named parameters with filter option values.

    Keyword Arguments
    --------------------
    depth_level : int
        Depth level in inches at which the measurement is taken. Necessary for `variable` = 'soil_moisture'.

    Returns
    -------
    var_id : int
        Integer variable ID associated with combination of `dataset`, `variable`, `temporal_resolution`,
        and `aggregation`.
    """
    if len(args) > 0 and isinstance(args[0], dict):
        options = args[0]
    else:
        options = kwargs

    if dataset in ["snotel", "scan"]:
        data_source = "usda_nrcs"
    else:
        data_source = dataset

    # Accept "-" in new versions of code as aggregation level
    # Maintain compatibility with older versions using "instantaneous"
    if aggregation == "-":
        aggregation = "instantaneous"

    if variable == "soil_moisture":
        query = """
                SELECT var_id
                FROM variables
                WHERE data_source = ?
                    AND variable = ?
                    AND temporal_resolution = ?
                    AND aggregation = ?
                    AND depth_level = ?
                """
        param_list = [
            data_source,
            variable,
            temporal_resolution,
            aggregation,
            options["depth_level"],
        ]

    else:
        query = """
                SELECT var_id
                FROM variables
                WHERE data_source = ?
                    AND variable = ?
                    AND temporal_resolution = ?
                    AND aggregation = ?
                """
        param_list = [data_source, variable, temporal_resolution, aggregation]

    try:
        result = pd.read_sql_query(query, conn, params=param_list)
        return int(result["var_id"][0])
    except:
        raise ValueError(
            "The provided combination of dataset, variable, temporal_resolution, and aggregation is not currently supported."
        )


def _get_sites(
    conn, dataset, variable, temporal_resolution, aggregation, *args, **kwargs
):
    """
    Build DataFrame with site attribute metadata information.

    Parameters
    ----------
    conn : Connection object
        The Connection object associated with the SQLite database to
        query from.
    dataset : str
        Source from which requested data originated. Currently supported: 'usgs_nwis', 'snotel',
        'scan', 'ameriflux', 'jasechko_2024', 'fan_2013'.
    variable : str, required
        Description of type of data requested. Currently supported: 'streamflow', 'water_table_depth', 'swe',
        'precipitation', 'air_temp', 'soil_moisture', 'latent_heat', 'sensible_heat',
        'downward_shortwave', 'downward_longwave', 'vapor_pressure_deficit', 'wind_speed'.
    temporal_resolution : str
        Collection frequency of data requested. Currently supported: 'daily', 'hourly', 'instantaneous',
        'yearly', 'long_term'.
        Please see the documentation for allowable combinations with `variable`.
    aggregation : str
        Additional information specifying the aggregation method for the variable to be returned.
        Options include descriptors such as 'mean' and 'sum'. Please see the documentation
        for allowable combinations with `variable`.
    depth_level : int, optional
        Depth level in inches at which the measurement is taken. Necessary for `variable` = 'soil_moisture'.
    date_start : str, optional
        'YYYY-MM-DD' date indicating beginning of time range.
    date_end : str, optional
        'YYYY-MM-DD' date indicating end of time range.
    latitude_range : tuple, optional
        Latitude range bounds for the geographic domain; lesser value is provided first.
    longitude_range : tuple, optional
        Longitude range bounds for the geographic domain; lesser value is provided first.
    grid : str, optional
        Value of either 'conus1' or 'conus2'. Used in combination with parameter `grid_bounds`
        to extract site locations for a specific region of conus coordinates.
    grid_bounds : list of integers, optional
        A list of points [left, bottom, right, top] in ij grid coordinates of the grid supplied
        by the `grid` parameter.
    site_ids : str or list of strings, optional
        Single site ID string or list of desired (string) site identifiers.
    state : str, optional
        Two-letter postal code state abbreviation.
    huc_id : str or list of strings, optional
        Single HUC ID string or list of adjacent HUC ID strings.
    polygon : str, optional
        Path to location of shapefile. Must be readable by PyShp's `shapefile.Reader()`.
    polygon_crs : str, optional
        CRS definition accepted by `pyproj.CRS.from_user_input()`.
    site_networks: str or list of strings, optional
        Name(s) of site networks. Can be a string with a single network name, or a list of strings
        containing strings for multiple available networks. There are currently supported networks for
        stream gages (dataset=='usgs_nwis', variable='streamflow') and groundwater wells (dataset=='usgs_nwis',
        variable='water_table_depth'). For streamflow, options include: 'gagesii', 'gagesii_reference', 'hcdn2009',
        'camels', and 'nwm'. For water table depth, options include: 'climate_response_network'.


    Returns
    -------
    DataFrame
        Site-level DataFrame of attribute metadata information.

    Notes
    -----
    The returned field 'record_count' is OVERALL record count. Filtering of metadata
    only applies at the site level, so only sites within the provided bounds
    (space and time) are included. The record count does not reflect any filtering
    at the data/observation level.
    """
    if len(args) > 0 and isinstance(args[0], dict):
        options = args[0]
    else:
        options = kwargs

    # Get associated variable IDs for requested data types and time periods
    var_id = _get_var_id(
        conn, dataset, variable, temporal_resolution, aggregation, options
    )

    param_list = [var_id]

    # Split site type by SNOTEL/SCAN station based on dataset
    if dataset in ["snotel", "scan"]:
        site_type_query = " AND s.site_type == ?"
        if dataset == "snotel":
            param_list.append("SNOTEL station")
        elif dataset == "scan":
            param_list.append("SCAN station")
    else:
        site_type_query = ""

    # Date start
    if "date_start" in options and options["date_start"] is not None:
        date_start_query = " AND last_date_data_available >= ?"
        param_list.append(options["date_start"])
    else:
        date_start_query = ""

    # Date end
    if "date_end" in options and options["date_end"] is not None:
        date_end_query = " AND first_date_data_available <= ?"
        param_list.append(options["date_end"])
    else:
        date_end_query = ""

    # Latitude
    if "latitude_range" in options and options["latitude_range"] is not None:
        lat_query = " AND latitude BETWEEN ? AND ?"
        param_list.append(options["latitude_range"][0])
        param_list.append(options["latitude_range"][1])
    else:
        lat_query = ""

    # Longitude
    if "longitude_range" in options and options["longitude_range"] is not None:
        lon_query = " AND longitude BETWEEN ? AND ?"
        param_list.append(options["longitude_range"][0])
        param_list.append(options["longitude_range"][1])
    else:
        lon_query = ""

    # CONUS grid bounds
    if "grid_bounds" in options and options["grid_bounds"] is not None:
        # Make sure that the option "grid" is defined
        try:
            assert "grid" in options and options["grid"] in ("conus1", "conus2")
        except:
            raise ValueError(
                "When providing the parameter `grid_bounds`, please also provide the parameter `grid` as either 'conus1' or 'conus2'."
            )

        # Determine which database table to get conus coordinates from
        if dataset == "usgs_nwis":
            if variable == "streamflow":
                tbl = "streamgauge_attributes"
            elif variable == "water_table_depth":
                tbl = "well_attributes"
        elif dataset in ("snotel", "scan"):
            tbl = "snotel_station_attributes"
        elif dataset == "ameriflux":
            tbl = "flux_tower_attributes"
        elif dataset == "jasechko_2024":
            tbl = "jasechko_attributes"
        elif dataset == "fan_2013":
            tbl = "well_attributes"

        grid = options["grid"]
        grid_bounds = options["grid_bounds"]

        grid_bounds_query = f"""SELECT site_id, {grid}_i, {grid}_j
                            FROM {tbl}
                            WHERE {grid}_i >= {grid_bounds[0]}
                              AND {grid}_j >= {grid_bounds[1]}
                              AND {grid}_i <= {grid_bounds[2]}
                              AND {grid}_j <= {grid_bounds[3]}
                        """
        grid_bounds_df = pd.read_sql_query(grid_bounds_query, conn)
        grid_bounds_sites = list(grid_bounds_df["site_id"])

        if len(grid_bounds_sites) > 0:
            grid_bounds_query = " AND s.site_id IN (%s)" % ",".join(
                "?" * len(grid_bounds_sites)
            )
            for s in grid_bounds_sites:
                param_list.append(s)
        else:
            raise Exception("There are no sites within the provided grid_bounds.")
    else:
        grid_bounds_query = ""

    # HUC ID filter
    if "huc_id" in options and options["huc_id"] is not None:
        huc_query, param_list = _get_huc_query(
            options, param_list, conn, dataset=dataset, variable=variable
        )
    else:
        huc_query = ""

    # Site ID
    if "site_ids" in options and options["site_ids"] is not None:
        if isinstance(options["site_ids"], list):
            site_query = " AND s.site_id IN (%s)" % ",".join(
                "?" * len(options["site_ids"])
            )
            for s in options["site_ids"]:
                param_list.append(s)
        elif isinstance(options["site_ids"], str):
            site_query = " AND s.site_id == ?"
            param_list.append(options["site_ids"])
        else:
            raise ValueError(
                "Parameter site_ids must be either a single site ID string, or a list of site ID strings"
            )
    else:
        site_query = ""

    # State
    if "state" in options and options["state"] is not None:
        state_query = " AND state == ?"
        param_list.append(options["state"])
    else:
        state_query = ""

    # Site Networks
    if "site_networks" in options and options["site_networks"] is not None:
        network_site_list = _get_network_site_list(
            dataset, variable, options["site_networks"]
        )
        network_query = " AND s.site_id IN (%s)" % ",".join(
            "?" * len(network_site_list)
        )
        for s in network_site_list:
            param_list.append(s)
    else:
        network_query = ""

    query = (
        """
            SELECT s.site_id, s.site_name, s.site_type, s.agency, s.state,
                   s.latitude, s.longitude, s.huc, o.first_date_data_available,
                   o.last_date_data_available, o.record_count, s.site_query_url,
                   s.date_metadata_last_updated, s.tz_cd, s.doi
            FROM sites s
            INNER JOIN observations o
            ON s.site_id = o.site_id AND o.var_id == ?
            WHERE first_date_data_available <> 'None'
            """
        + site_type_query
        + date_start_query
        + date_end_query
        + lat_query
        + lon_query
        + grid_bounds_query
        + huc_query
        + site_query
        + state_query
        + network_query
    )

    df = pd.read_sql_query(query, conn, params=param_list)

    # Polygon shapefile provided
    if "polygon" in options and options["polygon"] is not None:
        # Make sure CRS aligns between polygon and lat/lon points
        try:
            assert "polygon_crs" in options and options["polygon_crs"] is not None
        except:
            raise Exception(
                """Please provide 'polygon_crs' with a CRS definition accepted by pyproj.CRS.from_user_input()
                    to specify this shape's CRS."""
            )

        clipped_df = _filter_on_polygon(df, options["polygon"], options["polygon_crs"])
        return clipped_df

    else:
        return df


def _get_bbox_from_shape(polygon, polygon_crs):
    """
    Construct transformed latitude and longitude ranges representing a shape's bounding box.

    Parameters
    ----------
    polygon : str
        Path to location of shapefile. Must be readable by PyShp's `shapefile.Reader()`.
    polygon_crs : str
        CRS definition accepted by `pyproj.CRS.from_user_input()`.

    Returns
    -------
    Tuple : (latitude_range, longitude_range)
    """

    # Read in shapefile, obtain bounding box
    shp = shapefile.Reader(polygon)
    bbox = shp.bbox

    # Create series of corner points (lat/lon values) based on the bounding box
    # bbox = [lon, lat, lon, lat]
    bbox_df = pd.DataFrame(
        data={
            "lon": [bbox[0], bbox[0], bbox[2], bbox[2]],
            "lat": [bbox[1], bbox[3], bbox[1], bbox[3]],
        }
    )
    bbox_df["geometry"] = bbox_df.apply(
        lambda row: Point(row["lon"], row["lat"]), axis=1
    )

    # Transform the corner points into the lat/lon projection
    shp_crs = pyproj.CRS.from_user_input(polygon_crs)

    project = pyproj.Transformer.from_crs(
        shp_crs, pyproj.CRS("EPSG:4326"), always_xy=True
    ).transform
    bbox_df["transform"] = bbox_df.apply(
        lambda x: transform(project, x["geometry"]), axis=1
    )
    bbox_df["transform_x"] = bbox_df["transform"].apply(lambda x: x.x)
    bbox_df["transform_y"] = bbox_df["transform"].apply(lambda x: x.y)

    # Save transformed bounding box as latitude_range, longitude_range
    latitude_range = (
        float(bbox_df["transform_y"].min()),
        float(bbox_df["transform_y"].max()),
    )
    longitude_range = (
        float(bbox_df["transform_x"].min()),
        float(bbox_df["transform_x"].max()),
    )

    return (latitude_range, longitude_range)


def _filter_on_polygon(data_df, polygon, polygon_crs):
    """
    Filter site-level DataFrame on being within Polygon bounds.

    Parameters
    ----------
    data_df : DataFrame
        DataFrame containing site-level information including 'latitude' and 'longitude'.
    polygon : str
        Path to location of shapefile. Must be readable by PyShp's `shapefile.Reader()`.
    polygon_crs : str
        CRS definition accepted by `pyproj.CRS.from_user_input()`.

    Returns
    -------
    clipped_df : DataFrame
        DataFrame subset to only sites within the Polygon bounds.
    """

    # Read in shapefile
    shp = shapefile.Reader(polygon)

    # Convert features to shapely geometries
    try:
        assert len(shp.shapeRecords()) == 1
    except:
        raise Exception(
            "Please make sure your input shapefile contains only a single shape feature."
        )

    feature = shp.shapeRecords()[0].shape.__geo_interface__
    shp_geom = shape(feature)

    shp_crs = pyproj.CRS.from_user_input(polygon_crs)

    project = pyproj.Transformer.from_crs(
        shp_crs, pyproj.CRS("EPSG:4326"), always_xy=True
    ).transform
    shp_geom_crs = transform(project, shp_geom)

    # Clip points to only those within the polygon
    data_df["clip"] = data_df.apply(
        lambda x: contains_xy(
            shp_geom_crs, float(x["longitude"]), float(x["latitude"])
        ),
        axis=1,
    )
    clipped_df = (
        data_df[data_df["clip"] == True].reset_index().drop(columns=["index", "clip"])
    )

    return clipped_df


def _get_network_site_list(dataset, variable, site_networks):
    """
    Return list of site IDs for desired network of observation sites.

    Parameters
    ----------
    dataset : str
        Source from which requested data originated. Currently supported: 'usgs_nwis', 'snotel',
        'scan', 'ameriflux'.
    variable : str, required
        Description of type of data requested. Currently supported: 'streamflow', 'water_table_depth', 'swe',
        'precipitation', 'air_temp', 'soil_moisture', 'latent_heat', 'sensible_heat',
        'downward_shortwave', 'downward_longwave', 'vapor_pressure_deficit', 'wind_speed'.
    site_networks: str or list or strings
        List of names of site networks. Can be a list with a single network name.
        Each network must have matching .csv file with a list of site ID values that comprise
        the network. This .csv file must be located under network_lists/{dataset}/{variable}
        in the package directory and named as 'network_name'.csv.

    Returns
    -------
    site_list: list
        List of site ID strings for sites belonging to named network.
    """
    network_options = {
        "usgs_nwis": {
            "streamflow": ["camels", "gagesii_reference", "gagesii", "hcdn2009", "nwm"],
            "water_table_depth": ["climate_response_network"],
        }
    }

    # Initialize final site list
    site_list = []

    # Append sites from desired network(s)
    if isinstance(site_networks, str):
        site_networks = [site_networks]

    for network in site_networks:
        try:
            assert network in network_options[dataset][variable]
        except:
            raise ValueError(f"Network option {network} is not recognized.")
        try:
            df = pd.read_csv(
                f"{NETWORK_LISTS_PATH}/{dataset}/{variable}/{network}.csv",
                dtype=str,
                header=None,
                names=["site_id"],
            )
            site_list += list(df["site_id"])
        except:
            raise ValueError(
                f"Network list for {network} is not found. Please make sure the .csv network_lists/{dataset}/{variable}/{network}.csv exists."
            )

    # Make sure only list of unique site IDs is returned (in case multiple, overlapping networks provided)
    # Note: calling 'set' can change the order of the IDs, but for this workflow that does not matter
    return list(set(site_list))


def _clean_huc(huc):
    """
    Clean up and standardize HUC8 values.

    Parameters
    ----------
    huc : str
        Single string value representing a HUC code.

    Returns
    -------
    cleaned_huc : str
        HUC8 code or '' if not enough information available.
    """
    # Clean out HUC values that are fewer than 7 digits
    huc_length = len(huc)
    if huc_length < 7:
        cleaned_huc = ""

    # If 7 or 11 digits, add a leading 0
    elif len(huc) in (7, 11):
        huc = "0" + huc

    # Truncate to HUC8 for 'least common denominator' level
    if len(huc) >= 8:
        cleaned_huc = huc[0:8]

    return cleaned_huc


def _convert_to_pandas(ds):
    """
    Convert xarray DataSet to pandas DataFrame.

    Parameters
    ----------
    ds : DataSet
        xarray DataSet containing stacked observations data for a
        single variable.
    var_id : int
        Integer variable ID associated with combination of `variable`, `temporal_resolution`,
        and `aggregation`.

    Returns
    -------
    DataFrame
        Stacked observations data for a single variable.
    """
    sites = pd.Series(ds["site"].to_numpy())
    dates = pd.Series(ds["date"].to_numpy()).astype(str)
    data = ds.to_numpy()

    df = pd.DataFrame(data.T, columns=sites)
    df["date"] = dates

    # Reorder columns to put site_id first
    cols = df.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    df = df[cols]

    return df


def _filter_min_num_obs(df, min_num_obs):
    """
    Filter to only sites which have a minimum number of observations.

    This filtering is done after the observations are subset by time, so these
    observation counts will only filter out sites if the number of observations *within
    that time range* is not satisfied.

    Parameters
    ----------
    df : DataFrame
        Stacked observations data for a single variable.
    min_num_obs : int
        Value for the minimum number of observations desired for a site to have.

    Returns
    -------
    DataFrame
        Stacked observations data for a single variable, filtered to only sites that
        have the minimum number of observations specified.
    """
    dfc = df.copy()

    # drop columns with too many NaN values
    df_filtered = dfc.dropna(thresh=min_num_obs, axis=1)

    return df_filtered


def _get_data_nc(
    site_list, dataset, variable, temporal_resolution, aggregation, *args, **kwargs
):
    """
    Get observations data for data that is stored in NetCDF files.

    Parameters
    ----------
    site_list : list
        List of site IDs to query observations data for.
    dataset : str
        Source from which requested data originated. Currently supported: 'usgs_nwis', 'snotel',
        'scan', 'ameriflux', 'jasechko_2024', 'fan_2013'.
    variable : str, required
        Description of type of data requested. Currently supported: 'streamflow', 'water_table_depth', 'swe',
        'precipitation', 'air_temp', 'soil_moisture', 'latent_heat', 'sensible_heat',
        'downward_shortwave', 'downward_longwave', 'vapor_pressure_deficit', 'wind_speed'.
    temporal_resolution : str
        Collection frequency of data requested. Currently supported: 'daily', 'hourly', 'instantaneous',
        'yearly', 'long_term'.
        Please see the documentation for allowable combinations with `variable`.
    aggregation : str
        Additional information specifying the aggregation method for the variable to be returned.
        Options include descriptors such as 'mean' and 'sum'. Please see the documentation
        for allowable combinations with `variable`.
    args :
        Optional positional parameters that must be a dict with filter options.
    kwargs :
        Supports multiple named parameters with filter option values.

    Optional Parameters
    --------------------
    date_start : str; default=None
        'YYYY-MM-DD' date indicating beginning of time range.
    date_end : str; default=None
        'YYYY-MM-DD' date indicating end of time range.
    min_num_obs : int; default=1
        Value for the minimum number of observations desired for a site to have.

    Returns
    -------
    DataFrame
        Stacked observations data for a single variable, filtered to only sites that
        have the minimum number of observations specified.
    """
    if len(args) > 0 and isinstance(args[0], dict):
        options = args[0]
    else:
        options = kwargs

    # For soil moisture queries, we need the additional dataset_var filter to make the
    # request un-ambiguous.
    if "depth_level" in options and options["depth_level"] is not None:
        # Users input a `depth_level` parameter to filter different soil moisture variables
        # Values of depth_level are checked within the function _check_inputs.
        # This defines the dataset_var associated with that depth level for the request.
        sm_var = f"sms_{options['depth_level']}in"

        dc_entry = get_catalog_entry(
            dataset=dataset,
            variable=variable,
            temporal_resolution=temporal_resolution,
            aggregation=aggregation,
            dataset_var=sm_var,
            file_grouping="site_id",
        )
    else:
        dc_entry = get_catalog_entry(
            dataset=dataset,
            variable=variable,
            temporal_resolution=temporal_resolution,
            aggregation=aggregation,
            file_grouping="site_id",
        )

    # Parse out the directory path rather than a specific file path
    # Because the point data is one file per site ID, the code uses the
    # generic directory path and fills in the necessary site ID file information
    # when the data is requested.
    dirpath = ("/").join(dc_entry["path"].split("/")[:-1])
    file_list = [f"{dirpath}/{site}.nc" for site in site_list]
    varname = dc_entry["dataset_var"]

    if "date_start" in options:
        date_start_dt = np.datetime64(options["date_start"])
    if "date_end" in options:
        date_end_dt = np.datetime64(options["date_end"])

    for i in range(len(site_list)):
        # open single site file
        temp = xr.open_dataset(file_list[i])[varname]

        # make date variable name consistent
        date_var = list(temp.coords)[0]
        temp = temp.rename({date_var: "datetime"})

        # convert date string to datetime values
        temp["datetime"] = pd.DatetimeIndex(temp["datetime"].values)

        # subset to only observations within desired time range
        if ("date_start" not in options) and ("date_end" not in options):
            temp_wy = temp
        elif ("date_start" not in options) and ("date_end" in options):
            temp_wy = temp.sel(datetime=(temp.datetime <= date_end_dt))
        elif ("date_start" in options) and ("date_end" not in options):
            temp_wy = temp.sel(datetime=(temp.datetime >= date_start_dt))
        elif ("date_start" in options) and ("date_end" in options):
            temp_wy = temp.sel(
                datetime=(temp.datetime >= date_start_dt)
                & (temp.datetime <= date_end_dt)
            )

        if i == 0:
            ds = temp_wy
        else:
            ds = xr.concat([ds, temp_wy], dim="site")

    if len(site_list) == 1:
        ds = ds.expand_dims(dim="site")

    ds = ds.assign_coords({"site": (site_list)})
    ds = ds.rename({"datetime": "date"})

    data_df = _convert_to_pandas(ds)
    if "min_num_obs" in options and options["min_num_obs"] is not None:
        return _filter_min_num_obs(data_df, options["min_num_obs"])
    else:
        return data_df


def _get_data_sql(conn, site_list, var_id, *args, **kwargs):
    """
    Get observations data for data that is stored in a SQL table.

    Parameters
    ----------
    conn : Connection object
        The Connection object associated with the SQLite database to
        query from.
    site_list : list
        List of site IDs to query observations data for.
    var_id : int
        Integer variable ID associated with combination of `dataset`,
        `variable`, `temporal_resolution`, and `aggregation`.
    args :
        Optional positional parameters that must be a dict with filter options.
    kwargs :
        Supports multiple named parameters with filter option values.

    Optional Parameters
    --------------------
    date_start : str; default=None
        'YYYY-MM-DD' date indicating beginning of time range.
    date_end : str; default=None
        'YYYY-MM-DD' date indicating end of time range.
    min_num_obs : int; default=1
        Value for the minimum number of observations desired for a site to have.

    Returns
    -------
    DataFrame
        Stacked observations data for a single variable, filtered to only sites that
        have the minimum number of observations specified.
    """
    assert var_id in (5, 25, 26)
    if var_id == 5:
        tbl_name = "wtd_discrete_data"
        var_names = "w.site_id, w.date, w.wtd, w.pumping_status"

    elif var_id == 25:
        tbl_name = "jasechko_wtd_data"
        var_names = "w.site_id, w.date, w.wtd"

    elif var_id == 26:
        tbl_name = "fan_wtd_data"
        var_names = "w.site_id, w.wtd, record_count"

    if len(args) > 0 and isinstance(args[0], dict):
        options = args[0]
    else:
        options = kwargs

    # Note:
    #   pumping_status == '1' --> Static (not pumping)
    #   pumping_status == 'P' --> Pumping
    #   pumping_status == '' --> unknown (not reported)
    if "min_num_obs" not in options or options["min_num_obs"] is None:
        min_num_obs = 1
    else:
        min_num_obs = options["min_num_obs"]

    # This is a yearly variable. For date filtering to work properly, only consider the
    # year provided in the input arguments.
    if var_id == 25:
        if "date_start" in options and options["date_start"] is not None:
            options["date_start"] = datetime.datetime.strptime(
                options["date_start"], "%Y-%m-%d"
            ).year
        if "date_end" in options and options["date_end"] is not None:
            options["date_end"] = datetime.datetime.strptime(
                options["date_end"], "%Y-%m-%d"
            ).year

    # These variables have w.date in the database to be able to filter on
    if var_id in (5, 25):
        if ("date_start" not in options) and ("date_end" not in options):
            date_query = ""
            param_list = [min_num_obs]
        elif ("date_start" not in options) and ("date_end" in options):
            date_query = " WHERE w.date <= ?"
            param_list = [options["date_end"], min_num_obs, options["date_end"]]
        elif ("date_start" in options) and ("date_end" not in options):
            date_query = " WHERE w.date >= ?"
            param_list = [options["date_start"], min_num_obs, options["date_start"]]
        elif ("date_start" in options) and ("date_end" in options):
            date_query = " WHERE w.date >= ? AND w.date <= ?"
            param_list = [
                options["date_start"],
                options["date_end"],
                min_num_obs,
                options["date_start"],
                options["date_end"],
            ]

        # Add filter for only the site IDs in site_list
        site_query = " AND w.site_id IN (%s)" % ",".join("?" * len(site_list))
        for s in site_list:
            param_list.append(s)

        query = (
            f"""
            SELECT {var_names}
            FROM {tbl_name} AS w
            INNER JOIN (SELECT w.site_id, COUNT(*) AS num_obs
                FROM {tbl_name} AS w
                """
            + date_query
            + """
                GROUP BY site_id
                HAVING num_obs >= ?) AS c
            ON w.site_id = c.site_id
            """
            + date_query
            + site_query
        )

    # Check start and end dates overlap with overall dataset date range of 1927-2009
    elif var_id == 26:
        param_list = [min_num_obs]
        for s in site_list:
            param_list.append(s)

        query = f"""
                    SELECT {var_names}
                    FROM {tbl_name} AS w
                    INNER JOIN (SELECT site_id, record_count
                                FROM observations
                                WHERE record_count >= ? AND site_id IN (%s)
                                AND var_id = 26) AS o
                    ON w.site_id = o.site_id
                """ % ",".join(
            "?" * len(site_list)
        )

    df = pd.read_sql_query(query, conn, params=param_list)

    return df


def _get_huc_query(options, param_list, conn, dataset=None, variable=None):
    """Get sql query for filtering on list of HUC IDs"""

    # Make sure that the option "grid" is defined
    try:
        assert "grid" in options and options["grid"] in ("conus1", "conus2")
    except:
        raise ValueError(
            "When providing the parameter `huc_id`, please also provide the parameter `grid` as either 'conus1' or 'conus2'."
        )

    grid = options["grid"]
    huc_id = options["huc_id"]

    # Define bbox and mask
    bbox = get_huc_bbox(grid, huc_id)
    hucs = [int(huc) for huc in huc_id]
    level = len(huc_id[0])
    conus_hucs = get_gridded_data(
        {
            "dataset": "huc_mapping",
            "grid": grid,
            "file_type": "tiff",
            "level": level,
            "dataset_version": os.getenv("HUC_VERSION", None),
        }
    )
    conus_huc_mask = np.isin(conus_hucs, hucs).squeeze()
    imin, jmin, imax, jmax = bbox
    mask = conus_huc_mask[jmin:jmax, imin:imax].astype(int)

    # Determine which database table(s) to get conus coordinates from
    if dataset is not None:
        if dataset == "usgs_nwis":
            if variable == "streamflow":
                tbl_list = ["streamgauge_attributes"]
            elif variable == "water_table_depth":
                tbl_list = ["well_attributes"]
        elif dataset in ("snotel", "scan"):
            tbl_list = ["snotel_station_attributes"]
        elif dataset == "ameriflux":
            tbl_list = ["flux_tower_attributes"]
        elif dataset == "jasechko_2024":
            tbl_list = ["jasechko_attributes"]
        elif dataset == "fan_2013":
            tbl_list = ["well_attributes"]
    else:
        tbl_list = SITE_ATTRIBUTE_TABLES

    huc_sites = []
    for tbl in tbl_list:
        # First filter on HUC bounding box to get subset list of sites
        bbox_query = f"""SELECT site_id, {grid}_i, {grid}_j
                        FROM {tbl}
                        WHERE {grid}_i >= {bbox[0]}
                          AND {grid}_j >= {bbox[1]}
                          AND {grid}_i < {bbox[2]}
                          AND {grid}_j < {bbox[3]}
                        """
        bbox_df = pd.read_sql_query(bbox_query, conn)

        # Second filter on HUC mask to remove sites within bbox but not within HUC
        if len(bbox_df) > 0:
            # Shift i/j coordinates so that they index starting from the regional
            # bounding box origin instead of the overall CONUS grid origin
            bbox_df["domain_i"] = bbox_df.apply(
                lambda x: int(x[f"{grid}_i"]) - bbox[0], axis=1
            )
            bbox_df["domain_j"] = bbox_df.apply(
                lambda x: int(x[f"{grid}_j"]) - bbox[1], axis=1
            )

            # Filter sites to only those within HUC mask
            bbox_df["mask"] = mask[bbox_df["domain_j"], bbox_df["domain_i"]]
            bbox_df = bbox_df[bbox_df["mask"] == 1]

            huc_sites.extend(list(bbox_df["site_id"]))

    if len(huc_sites) > 0:
        huc_query = " AND s.site_id IN (%s)" % ",".join("?" * len(huc_sites))
        for s in huc_sites:
            param_list.append(s)
    else:
        raise Exception("There are no sites within the provided huc_id.")

    return huc_query, param_list
