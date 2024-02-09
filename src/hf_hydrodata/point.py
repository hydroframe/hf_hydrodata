"""Module to retrieve point observations."""

# pylint: disable=C0301,W0707,W0719,C0121,C0302,C0209,C0325,W0702
import datetime
from typing import Tuple
import io
import ast
import os
import json
import sqlite3
import pandas as pd
import xarray as xr
import numpy as np
import requests
import shapefile
import pyproj
from shapely import contains_xy
from shapely.geometry import Point, shape
from shapely.ops import transform


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
]


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
        'scan', 'ameriflux'.
    variable : str, required
        Description of type of data requested. Currently supported: 'streamflow', 'water_table_depth', 'swe',
        'precipitation', 'air_temp', 'soil_moisture', 'latent_heat', 'sensible_heat',
        'downward_shortwave', 'downward_longwave', 'vapor_pressure_deficit', 'wind_speed'.
    temporal_resolution : str, required
        Collection frequency of data requested. Currently supported: 'daily', 'hourly', and 'instantaneous'.
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
    site_ids : str or list of strings, optional
        Single site ID string or list of desired (string) site identifiers.
    state : str, optional
        Two-letter postal code state abbreviation (example: state='NJ').
    polygon : str, optional
        Path to location of shapefile. Must be readable by PyShp's `shapefile.Reader()`.
    polygon_crs : str, optional
        CRS definition accepted by `pyproj.CRS.from_user_input()`.
    site_networks: str or list of strings, optional
        Name(s) of site networks. Can be a string with a single network name, or a list of strings
        containing strings for multiple available networks. There are currently supported networks for
        stream gages (dataset=='usgs_nwis', variable='streamflow') and groundwater wells (dataset=='usgs_nwis',
        variable='water_table_depth'). For streamflow, options include: 'gagesii', 'gagesii_reference', 'hcdn2009',
        and 'camels'. For water table depth, options include: 'climate_response_network'.
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
        data_df = _get_data_nc(site_list, var_id, *args, **kwargs)

    elif var_id == 5:
        data_df = _get_data_sql(conn, var_id, *args, **kwargs)

    conn.close()

    return data_df.reset_index().drop("index", axis=1)


def get_point_metadata(*args, **kwargs):
    """
    Return DataFrame with site metadata for the filtered sites.

    Parameters
    ----------
    dataset : str, required
        Source from which requested data originated. Currently supported: 'usgs_nwis', 'snotel',
        'scan', 'ameriflux'.
    variable : str, required
        Description of type of data requested. Currently supported: 'streamflow', 'water_table_depth', 'swe',
        'precipitation', 'air_temp', 'soil_moisture', 'latent_heat', 'sensible_heat',
        'downward_shortwave', 'downward_longwave', 'vapor_pressure_deficit', 'wind_speed'.
    temporal_resolution : str, required
        Collection frequency of data requested. Currently supported: 'daily', 'hourly', and 'instantaneous'.
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
    site_ids : str or list of strings, optional
        Single site ID string or list of desired (string) site identifiers.
    state : str, optional
        Two-letter postal code state abbreviation (example: state='NJ').
    polygon : str, optional
        Path to location of shapefile. Must be readable by PyShp's `shapefile.Reader()`.
    polygon_crs : str, optional
        CRS definition accepted by `pyproj.CRS.from_user_input()`.
    site_networks: str or list of strings, optional
        Name(s) of site networks. Can be a string with a single network name, or a list of strings
        containing strings for multiple available networks. There are currently supported networks for
        stream gages (dataset=='usgs_nwis', variable='streamflow') and groundwater wells (dataset=='usgs_nwis',
        variable='water_table_depth'). For streamflow, options include: 'gagesii', 'gagesii_reference', 'hcdn2009',
        and 'camels'. For water table depth, options include: 'climate_response_network'.

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
            """SELECT site_id, conus1_x, conus1_y, conus2_x, conus2_y,
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

    if "groundwater well" in metadata_df["site_type"].unique():
        attributes_df = pd.read_sql_query(
            """SELECT site_id, conus1_x, conus1_y, conus2_x, conus2_y,
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

    if ("SNOTEL station" in metadata_df["site_type"].unique()) or (
        "SCAN station" in metadata_df["site_type"].unique()
    ):
        attributes_df = pd.read_sql_query(
            """SELECT site_id, conus1_x, conus1_y, conus2_x, conus2_y,
                      elevation AS usda_elevation
               FROM snotel_station_attributes WHERE site_id IN (%s)"""
            % ",".join("?" * len(site_ids)),
            conn,
            params=site_ids,
        )
        metadata_df = pd.merge(metadata_df, attributes_df, how="left", on="site_id")

    if "flux tower" in metadata_df["site_type"].unique():
        attributes_df = pd.read_sql_query(
            """SELECT site_id, conus1_x, conus1_y, conus2_x, conus2_y,
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


def get_site_variables(*args, **kwargs):
    """
    Return DataFrame with available sites, variables, and the period of record.

    Parameters
    ----------
    dataset : str, optional
        Source from which requested data originated. Currently supported: 'usgs_nwis', 'snotel',
        'scan', 'ameriflux'.
    variable : str, required
        Description of type of data requested. Currently supported: 'streamflow', 'water_table_depth', 'swe',
        'precipitation', 'air_temp', 'soil_moisture', 'latent_heat', 'sensible_heat',
        'downward_shortwave', 'downward_longwave', 'vapor_pressure_deficit', 'wind_speed'.
    temporal_resolution : str, optional
        Collection frequency of data requested. Currently supported: 'daily', 'hourly', and 'instantaneous'.
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
    site_ids : str or list of strings, optional
        Single site ID string or list of desired (string) site identifiers.
    state : str, optional
        Two-letter postal code state abbreviation (example: state='NJ').
    polygon : str, optional
        Path to location of shapefile. Must be readable by PyShp's `shapefile.Reader()`.
    polygon_crs : str, optional
        CRS definition accepted by `pyproj.CRS.from_user_input()`.
    site_networks: str or list of strings, optional
        Name(s) of site networks. Can be a string with a single network name, or a list of strings
        containing strings for multiple available networks. There are currently supported networks for
        stream gages (dataset=='usgs_nwis', variable='streamflow') and groundwater wells (dataset=='usgs_nwis',
        variable='water_table_depth'). For streamflow, options include: 'gagesii', 'gagesii_reference', 'hcdn2009',
        and 'camels'. For water table depth, options include: 'climate_response_network'.

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
            assert options["dataset"] in ["usgs_nwis", "snotel", "scan", "ameriflux"]
        except:
            raise ValueError(
                f"dataset must be one of 'usgs_nwis', 'snotel', 'scan', 'ameriflux'. You provided {options['dataset']}"
            )

        if options["dataset"] == "usgs_nwis":
            dataset_query = """ AND agency == ?"""
            param_list.append("USGS")
        elif options["dataset"] == "ameriflux":
            dataset_query = """ AND agency == ?"""
            param_list.append("AmeriFlux")
        elif options["dataset"] == "snotel":
            dataset_query = """ AND site_type == ?"""
            param_list.append("SNOTEL station")
        elif options["dataset"] == "scan":
            dataset_query = """ AND site_type == ?"""
            param_list.append("SCAN station")
    else:
        dataset_query = """"""

    # Date start
    if "date_start" in options and options["date_start"] is not None:
        date_start_query = """ AND last_date_data_available >= ?"""
        param_list.append(options["date_start"])
    else:
        date_start_query = """"""

    # Date end
    if "date_end" in options and options["date_end"] is not None:
        date_end_query = """ AND first_date_data_available <= ?"""
        param_list.append(options["date_end"])
    else:
        date_end_query = """"""

    # Latitude
    if "latitude_range" in options and options["latitude_range"] is not None:
        lat_query = """ AND latitude BETWEEN ? AND ?"""
        param_list.append(options["latitude_range"][0])
        param_list.append(options["latitude_range"][1])
    else:
        lat_query = """"""

    # Longitude
    if "longitude_range" in options and options["longitude_range"] is not None:
        lon_query = """ AND longitude BETWEEN ? AND ?"""
        param_list.append(options["longitude_range"][0])
        param_list.append(options["longitude_range"][1])
    else:
        lon_query = """"""

    # Site ID
    if "site_ids" in options and options["site_ids"] is not None:
        if isinstance(options["site_ids"], list):
            site_query = """ AND s.site_id IN (%s)""" % ",".join(
                "?" * len(options["site_ids"])
            )
            for s in options["site_ids"]:
                param_list.append(s)
        elif isinstance(options["site_ids"], str):
            print(f"site_ids {options['site_ids']}")
            site_query = """ AND s.site_id == ?"""
            param_list.append(options["site_ids"])
        else:
            raise ValueError(
                "Parameter site_ids must be either a single site ID string, or a list of site ID strings"
            )
    else:
        site_query = """"""

    # State
    if "state" in options and options["state"] is not None:
        state_query = """ AND state == ?"""
        param_list.append(options["state"])
    else:
        state_query = """"""

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
        network_query = """"""

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
    conn.close()
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
            "temporal_resolution",
            "variable_type",
            "variable",
            "aggregation",
            "data_source",
            "depth_level",
        ]
    )

    # Re-order final columns
    ordered_cols = [
        "site_id",
        "site_name",
        "site_type",
        "agency",
        "state",
        "variable_name",
        "units",
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
        if key == "site_ids":
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
        if key == "site_ids":
            if isinstance(value, str):
                try:
                    options[key] = ast.literal_eval(value)
                    if isinstance(options[key], int):
                        options[key] = value
                except:
                    options[key] = value  # when site_id is a single str
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


def _get_point_citations(dataset):
    """
    Return a dictionary with relevant citation information.

    Parameters
    ----------
    dataset : str
        Source from which requested data originated. Currently supported: 'usgs_nwis', 'snotel',
        'scan', 'ameriflux'.

    Returns
    -------
    str
        String containing overall attribution instructions for the provided dataset.
    """
    try:
        assert dataset in ["usgs_nwis", "snotel", "scan", "ameriflux"]
    except:
        raise ValueError(
            f"Unexpected value of dataset, {dataset}. Supported values include 'usgs_nwis', 'snotel', 'scan', and 'ameriflux'"
        )

    if dataset == "usgs_nwis":
        c = (
            "Most U.S. Geological Survey (USGS) information resides in Public Domain and "
            "may be used without restriction, though they do ask that proper credit be given. "
            'An example credit statement would be: "(Product or data name) courtesy of the U.S. Geological Survey".\n'
            "Source: https://www.usgs.gov/information-policies-and-instructions/acknowledging-or-crediting-usgs"
        )

    elif dataset in ["snotel", "scan"]:
        c = (
            "Most information presented on the USDA Web site is considered public domain information. "
            "Public domain information may be freely distributed or copied, but use of appropriate "
            "byline/photo/image credits is requested. Attribution may be cited as follows: "
            '"U.S. Department of Agriculture"\nSource: https://www.usda.gov/policies-and-links'
        )

    elif dataset == "ameriflux":
        c = (
            "All AmeriFlux sites provided by the HydroData service follow the CC-BY-4.0 License. "
            "The CC-BY-4.0 license specifies that the data user is free to Share (copy and "
            "redistribute the material in any medium or format) and/or Adapt (remix, transform, "
            "and build upon the material) for any purpose. "
            "Users of this data must acknowledge the AmeriFlux data resource with the "
            'following statement: "Funding for the AmeriFlux data portal was provided by the U.S. '
            'Department of Energy Office of Science." '
            "Additionally, for each AmeriFlux site used, you must provide a citation to the site "
            "data product that includes the data product DOI. The DOI for each site is included in the "
            "DataFrame returned by the hf_hydrodata get_point_metadata method, in the doi column.\n"
            "Source: https://ameriflux.lbl.gov/data/data-policy/"
        )

    return c


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
        'scan', 'ameriflux'.
    variable : str, required
        Description of type of data requested. Currently supported: 'streamflow', 'water_table_depth', 'swe',
        'precipitation', 'air_temp', 'soil_moisture', 'latent_heat', 'sensible_heat',
        'downward_shortwave', 'downward_longwave', 'vapor_pressure_deficit', 'wind_speed'.
    temporal_resolution : str
        Collection frequency of data requested. Currently supported: 'daily', 'hourly', and 'instantaneous'.
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
        assert temporal_resolution in ["daily", "hourly", "instantaneous"]
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
            "instantaneous",
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
        assert dataset in ["usgs_nwis", "snotel", "scan", "ameriflux"]
    except:
        raise ValueError(
            f"Unexpected value for dataset, {dataset} Please see the documentation for allowed values."
        )

    if variable == "soil_moisture":
        try:
            assert "depth_level" in options
            assert options["depth_level"] in [2, 4, 8, 20, 40]
        except:
            raise ValueError(
                "Please provide depth_level with one of the supported values. Please see the documentation for allowed values."
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
        'scan', 'ameriflux'.
    variable : str, required
        Description of type of data requested. Currently supported: 'streamflow', 'water_table_depth', 'swe',
        'precipitation', 'air_temp', 'soil_moisture', 'latent_heat', 'sensible_heat',
        'downward_shortwave', 'downward_longwave', 'vapor_pressure_deficit', 'wind_speed'.
    temporal_resolution : str
        Collection frequency of data requested. Currently supported: 'daily', 'hourly', and 'instantaneous'.
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


def _get_dirpath(var_id):
    """
    Map variable with location of data on /hydrodata.

    Parameters
    ----------
    var_id : int
        Integer variable ID associated with combination of `dataset`,
        `variable`, `temporal_resolution`, and `aggregation`.

    Returns
    -------
    dirpath : str
        Directory path for observation data location.
    """
    dirpath_map = {
        1: "/hydrodata/national_obs/streamflow/data/hourly",
        2: "/hydrodata/national_obs/streamflow/data/daily",
        3: "/hydrodata/national_obs/groundwater/data/hourly",
        4: "/hydrodata/national_obs/groundwater/data/daily",
        5: "",
        6: "/hydrodata/national_obs/swe/data/daily",
        7: "/hydrodata/national_obs/point_meteorology/NRCS_precipitation/data/daily",
        8: "/hydrodata/national_obs/point_meteorology/NRCS_precipitation/data/daily",
        9: "/hydrodata/national_obs/point_meteorology/NRCS_precipitation/data/daily",
        10: "/hydrodata/national_obs/point_meteorology/NRCS_temperature/data/daily",
        11: "/hydrodata/national_obs/point_meteorology/NRCS_temperature/data/daily",
        12: "/hydrodata/national_obs/point_meteorology/NRCS_temperature/data/daily",
        13: "/hydrodata/national_obs/soil_moisture/data/daily",
        14: "/hydrodata/national_obs/soil_moisture/data/daily",
        15: "/hydrodata/national_obs/soil_moisture/data/daily",
        16: "/hydrodata/national_obs/soil_moisture/data/daily",
        17: "/hydrodata/national_obs/soil_moisture/data/daily",
        18: "/hydrodata/national_obs/ameriflux/data/hourly",
        19: "/hydrodata/national_obs/ameriflux/data/hourly",
        20: "/hydrodata/national_obs/ameriflux/data/hourly",
        21: "/hydrodata/national_obs/ameriflux/data/hourly",
        22: "/hydrodata/national_obs/ameriflux/data/hourly",
        23: "/hydrodata/national_obs/ameriflux/data/hourly",
        24: "/hydrodata/national_obs/ameriflux/data/hourly",
    }

    return dirpath_map[var_id]


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
        'scan', 'ameriflux'.
    variable : str, required
        Description of type of data requested. Currently supported: 'streamflow', 'water_table_depth', 'swe',
        'precipitation', 'air_temp', 'soil_moisture', 'latent_heat', 'sensible_heat',
        'downward_shortwave', 'downward_longwave', 'vapor_pressure_deficit', 'wind_speed'.
    temporal_resolution : str
        Collection frequency of data requested. Currently supported: 'daily', 'hourly', and 'instantaneous'.
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
    site_ids : str or list of strings, optional
        Single site ID string or list of desired (string) site identifiers.
    state : str, optional
        Two-letter postal code state abbreviation.
    polygon : str, optional
        Path to location of shapefile. Must be readable by PyShp's `shapefile.Reader()`.
    polygon_crs : str, optional
        CRS definition accepted by `pyproj.CRS.from_user_input()`.
    site_networks: str or list of strings, optional
        Name(s) of site networks. Can be a string with a single network name, or a list of strings
        containing strings for multiple available networks. There are currently supported networks for
        stream gages (dataset=='usgs_nwis', variable='streamflow') and groundwater wells (dataset=='usgs_nwis',
        variable='water_table_depth'). For streamflow, options include: 'gagesii', 'gagesii_reference', 'hcdn2009',
        and 'camels'. For water table depth, options include: 'climate_response_network'.


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
        site_type_query = """ AND s.site_type == ?"""
        if dataset == "snotel":
            param_list.append("SNOTEL station")
        elif dataset == "scan":
            param_list.append("SCAN station")
    else:
        site_type_query = """"""

    # Date start
    if "date_start" in options and options["date_start"] is not None:
        date_start_query = """ AND last_date_data_available >= ?"""
        param_list.append(options["date_start"])
    else:
        date_start_query = """"""

    # Date end
    if "date_end" in options and options["date_end"] is not None:
        date_end_query = """ AND first_date_data_available <= ?"""
        param_list.append(options["date_end"])
    else:
        date_end_query = """"""

    # Latitude
    if "latitude_range" in options and options["latitude_range"] is not None:
        lat_query = """ AND latitude BETWEEN ? AND ?"""
        param_list.append(options["latitude_range"][0])
        param_list.append(options["latitude_range"][1])
    else:
        lat_query = """"""

    # Longitude
    if "longitude_range" in options and options["longitude_range"] is not None:
        lon_query = """ AND longitude BETWEEN ? AND ?"""
        param_list.append(options["longitude_range"][0])
        param_list.append(options["longitude_range"][1])
    else:
        lon_query = """"""

    # Site ID
    if "site_ids" in options and options["site_ids"] is not None:
        if isinstance(options["site_ids"], list):
            site_query = """ AND s.site_id IN (%s)""" % ",".join(
                "?" * len(options["site_ids"])
            )
            for s in options["site_ids"]:
                param_list.append(s)
        elif isinstance(options["site_ids"], str):
            site_query = """ AND s.site_id == ?"""
            param_list.append(options["site_ids"])
        else:
            raise ValueError(
                "Parameter site_ids must be either a single site ID string, or a list of site ID strings"
            )
    else:
        site_query = """"""

    # State
    if "state" in options and options["state"] is not None:
        state_query = """ AND state == ?"""
        param_list.append(options["state"])
    else:
        state_query = """"""

    # Site Networks
    if "site_networks" in options and options["site_networks"] is not None:
        network_site_list = _get_network_site_list(
            dataset, variable, options["site_networks"]
        )
        network_query = """ AND s.site_id IN (%s)""" % ",".join(
            "?" * len(network_site_list)
        )
        for s in network_site_list:
            param_list.append(s)
    else:
        network_query = """"""

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
    latitude_range = (bbox_df["transform_y"].min(), bbox_df["transform_y"].max())
    longitude_range = (bbox_df["transform_x"].min(), bbox_df["transform_x"].max())

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
            "streamflow": ["camels", "gagesii_reference", "gagesii", "hcdn2009"],
            "wtd": ["climate_response_network"],
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
            df = pd.read_csv(
                f"{NETWORK_LISTS_PATH}/{dataset}/{variable}/{network}.csv",
                dtype=str,
                header=None,
                names=["site_id"],
            )
            site_list += list(df["site_id"])
        except:
            raise ValueError(
                f"Network option {network} is not recognized. Please make sure the .csv network_lists/{dataset}/{variable}/{network}.csv exists."
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


def _get_data_nc(site_list, var_id, *args, **kwargs):
    """
    Get observations data for data that is stored in NetCDF files.

    Parameters
    ----------
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
    if len(args) > 0 and isinstance(args[0], dict):
        options = args[0]
    else:
        options = kwargs

    dirpath = _get_dirpath(var_id)
    file_list = [f"{dirpath}/{site}.nc" for site in site_list]

    varname_map = {
        "1": "streamflow",
        "2": "streamflow",
        "3": "wtd",
        "4": "wtd",
        "5": "wtd",
        "6": "swe",
        "7": "precip_acc",
        "8": "precip_inc",
        "9": "precip_inc_sa",
        "10": "temp_min",
        "11": "temp_max",
        "12": "temp_avg",
        "13": "sms_2in",
        "14": "sms_4in",
        "15": "sms_8in",
        "16": "sms_20in",
        "17": "sms_40in",
        "18": "latent heat flux",
        "19": "sensible heat flux",
        "20": "shortwave radiation",
        "21": "longwave radiation",
        "22": "vapor pressure deficit",
        "23": "air temperature",
        "24": "wind speed",
    }

    varname = varname_map[str(var_id)]

    if "date_start" in options:
        date_start_dt = np.datetime64(options["date_start"])
    if "date_end" in options:
        date_end_dt = np.datetime64(options["date_end"])

    print("collecting data...")

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

    print("data collected.")

    data_df = _convert_to_pandas(ds)
    if "min_num_obs" in options and options["min_num_obs"] is not None:
        return _filter_min_num_obs(data_df, options["min_num_obs"])
    else:
        return data_df


def _get_data_sql(conn, var_id, *args, **kwargs):
    """
    Get observations data for data that is stored in a SQL table.

    Parameters
    ----------
    conn : Connection object
        The Connection object associated with the SQLite database to
        query from.
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
    assert var_id == 5

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

    if ("date_start" not in options) and ("date_end" not in options):
        date_query = """"""
        param_list = [min_num_obs]
    elif ("date_start" not in options) and ("date_end" in options):
        date_query = """ WHERE w.date <= ?"""
        param_list = [options["date_end"], min_num_obs, options["date_end"]]
    elif ("date_start" in options) and ("date_end" not in options):
        date_query = """ WHERE w.date >= ?"""
        param_list = [options["date_start"], min_num_obs, options["date_start"]]
    elif ("date_start" in options) and ("date_end" in options):
        date_query = """ WHERE w.date >= ? AND w.date <= ?"""
        param_list = [
            options["date_start"],
            options["date_end"],
            min_num_obs,
            options["date_start"],
            options["date_end"],
        ]

    query = (
        """
            SELECT w.site_id, w.date, w.wtd, w.pumping_status
            FROM wtd_discrete_data AS w
            INNER JOIN (SELECT w.site_id, COUNT(*) AS num_obs
                FROM wtd_discrete_data AS w
                """
        + date_query
        + """
                GROUP BY site_id
                HAVING num_obs >= ?) AS c
            ON w.site_id = c.site_id
            """
        + date_query
    )

    df = pd.read_sql_query(query, conn, params=param_list)

    return df
