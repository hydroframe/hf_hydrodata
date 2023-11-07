"""Module to retrieve point observations."""
# pylint: disable=C0301
import datetime
from typing import Tuple
import io
import ast
import os
import json
import sqlite3
import datetime as dt
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
HYDRODATA_URL = os.getenv("HYDRODATA_URL", "https://hydro-dev.princeton.edu")
NETWORK_LISTS_PATH = f"/{HYDRODATA}/national_obs/tools/network_lists"


def get_data(data_source, variable, temporal_resolution, aggregation, *args, **kwargs):
    """
    Collect observations data into a Pandas DataFrame.

    Observations collected from HydroData for the specified data source, variable, temporal
    resolution, and aggregation. Optional arguments can be supplied for filters such as
    date bounds, geography bounds, and/or the minimum number of per-site observations allowed.
    Please see the package documentation for the full set of supported combinations.

    Parameters
    ----------
    data_source : str
        Source from which requested data originated. Currently supported: 'usgs_nwis', 'usda_nrcs',
        'ameriflux'.
    variable : str
        Description of type of data requested. Currently supported: 'streamflow', 'wtd', 'swe',
        'precipitation', 'temperature', 'soil moisture', 'latent heat flux', 'sensible heat flux',
        'shortwave radiation', 'longwave radiation', 'vapor pressure deficit', 'wind speed'.
    temporal_resolution : str
        Collection frequency of data requested. Currently supported: 'daily', 'hourly', and 'instantaneous'.
        Please see the documentation for allowable combinations with `variable`.
    aggregation : str
        Additional information specifying the aggregation method for the variable to be returned.
        Options include descriptors such as 'average' and 'total'. Please see the documentation
        for allowable combinations with `variable`.
    \*args :
        Optional positional parameters that must be a dict with filter options. See 'Keyword Arguments' below.
    \**kwargs :
        Supports multiple named parameters with filter option values. See 'Keyword Arguments' below.

    Keyword Arguments
    --------------------
    depth_level : int
        Depth level in inches at which the measurement is taken. Necessary for `variable` = 'soil moisture'.
    date_start : str; default=None
        'YYYY-MM-DD' date indicating beginning of time range.
    date_end : str; default=None
        'YYYY-MM-DD' date indicating end of time range.
    latitude_range : tuple; default=None
        Latitude range bounds for the geographic domain; lesser value is provided first.
    longitude_range : tuple; default=None
        Longitude range bounds for the geographic domain; lesser value is provided first.
    site_ids : list; default=None
        List of desired (string) site identifiers.
    state : str; default=None
        Two-letter postal code state abbreviation.
    polygon : str
        Path to location of shapefile. Must be readable by PyShp's `shapefile.Reader()`.
    site_networks: list
        List of names of site networks. Can be a list with a single network name.
        Each network must have matching .csv file with a list of site ID values that comprise
        the network. This .csv file must be located under network_lists/{data_source}/{variable}
        in the package directory and named as 'network_name'.csv. Eg: `site_networks=['gagesii']`
    min_num_obs : int; default=1
        Value for the minimum number of observations desired for a site to have.

    Returns
    -------
    data_df : DataFrame
        Stacked observations data for a single variable, filtered to only sites that
        (optionally) have the minimum number of observations specified, within the
        defined geographic and/or date range.
    """

    if len(args) > 0 and isinstance(args[0], dict):
        options = args[0]
    else:
        options = kwargs

    run_remote = not os.path.exists(HYDRODATA)

    if run_remote:
        data_df = _get_data_from_api(
            "data_only",
            data_source,
            variable,
            temporal_resolution,
            aggregation,
            options,
        )

        return data_df

    kwargs = _convert_strings_to_type(options)

    # Create database connection
    conn = sqlite3.connect(DB_PATH)

    # Validation checks on inputs
    _check_inputs(
        data_source, variable, temporal_resolution, aggregation, *args, **kwargs
    )

    # Get associated variable IDs for requested data types and time periods
    var_id = _get_var_id(
        conn, data_source, variable, temporal_resolution, aggregation, *args, **kwargs
    )

    # Get site list
    sites_df = _get_sites(
        conn, data_source, variable, temporal_resolution, aggregation, *args, **kwargs
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


def get_metadata(data_source, variable, temporal_resolution, aggregation, *args, **kwargs):
    """
    Return DataFrame with site metadata for the requested site IDs.

    Parameters
    ----------
    data_source : str
        Source from which requested data originated. Currently supported: 'usgs_nwis', 'usda_nrcs',
        'ameriflux'.
    variable : str
        Description of type of data requested. Currently supported: 'streamflow', 'wtd', 'swe',
        'precipitation', 'temperature', 'soil moisture', 'latent heat flux', 'sensible heat flux',
        'shortwave radiation', 'longwave radiation', 'vapor pressure deficit', 'wind speed'.
    temporal_resolution : str
        Collection frequency of data requested. Currently supported: 'daily', 'hourly', and 'instantaneous'.
        Please see the documentation for allowable combinations with `variable`.
    aggregation : str
        Additional information specifying the aggregation method for the variable to be returned.
        Options include descriptors such as 'average' and 'total'. Please see the documentation
        for allowable combinations with `variable`.
    \*args :
        Optional positional parameters that must be a dict with filter options. See 'Keyword Arguments' below.
    \**kwargs :
        Supports multiple named parameters with filter option values. See 'Keyword Arguments' below.

    Keyword Arguments
    --------------------
    depth_level : int
        Depth level in inches at which the measurement is taken. Necessary for `variable` = 'soil moisture'.
    date_start : str; default=None
        'YYYY-MM-DD' date indicating beginning of time range.
    date_end : str; default=None
        'YYYY-MM-DD' date indicating end of time range.
    latitude_range : tuple; default=None
        Latitude range bounds for the geographic domain; lesser value is provided first.
    longitude_range : tuple; default=None
        Longitude range bounds for the geographic domain; lesser value is provided first.
    site_ids : list; default=None
        List of desired (string) site identifiers.
    state : str; default=None
        Two-letter postal code state abbreviation.
    polygon : str
        Path to location of shapefile. Must be readable by PyShp's `shapefile.Reader()`.
    site_networks: list
        List of names of site networks. Can be a list with a single network name.
        Each network must have matching .csv file with a list of site ID values that comprise
        the network. This .csv file must be located under network_lists/{data_source}/{variable}
        in the package directory and named as 'network_name'.csv. Eg: `site_networks=['gagesii']`

    Returns
    -------
    DataFrame
        Site-level DataFrame of site-level metadata.
    """

    if len(args) > 0 and isinstance(args[0], dict):
        options = args[0]
    else:
        options = kwargs

    run_remote = not os.path.exists(HYDRODATA)

    if run_remote:
        data_df = _get_data_from_api(
            "metadata_only",
            data_source,
            variable,
            temporal_resolution,
            aggregation,
            options,
        )

        return data_df

    options = _convert_strings_to_type(options)

    # Create database connection
    conn = sqlite3.connect(DB_PATH)

    metadata_df = _get_sites(
        conn, data_source, variable, temporal_resolution, aggregation, *args, **kwargs
    )

    # Clean up HUC to string of appropriate length
    metadata_df["huc8"] = metadata_df["huc"].apply(lambda x: _clean_huc(x))
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

    if ('SNOTEL station' in metadata_df['site_type'].unique()) or ('SCAN station' in metadata_df['site_type'].unique()):
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


def _get_data_from_api(
    data_type, data_source, variable, temporal_resolution, aggregation, options
):
    options = _convert_params_to_string_dict(options)

    q_params = _construct_string_from_qparams(
        data_type, data_source, variable, temporal_resolution, aggregation, options
    )

    point_data_url = f"{HYDRODATA_URL}/api/point-data-dataframe?{q_params}"

    try:
        headers = _validate_user()
        response = requests.get(point_data_url, headers=headers, timeout=180)
        if response.status_code != 200:
            raise ValueError(
                f"{response.content}."
            )

    except requests.exceptions.Timeout as e:
        raise ValueError(f"The point_data_url {point_data_url} has timed out.") from e

    data_df = pd.read_pickle(io.BytesIO(response.content))
    return data_df


def get_registered_api_pin() -> Tuple[str, str]:
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
        # Don't need below anymore?  Check with Amy D.
        """
        if key == "all_attributes":
            if not isinstance(value, str):
                options[key] = str(value)
        """
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
                options[key] = ast.literal_eval(value)
        if key == "site_networks":
            if isinstance(value, str):
                options[key] = ast.literal_eval(value)
        if key == "min_num_obs":
            if isinstance(value, str):
                options[key] = int(value)
        # Don't need below anymore?  Check with Amy D.
        """
        if key == "all_attributes":
            if isinstance(value, str):
                options[key] = bool(value)
        """

    return options


def _construct_string_from_qparams(
    data_type, data_source, variable, temporal_resolution, aggregation, options
):
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

    qparam_values = options
    qparam_values["data_type"] = data_type
    qparam_values["data_source"] = data_source
    qparam_values["variable"] = variable
    qparam_values["temporal_resolution"] = temporal_resolution
    qparam_values["aggregation"] = aggregation

    string_parts = [
        f"{name}={value}" for name, value in options.items() if value is not None
    ]
    result_string = "&".join(string_parts)
    return result_string


def get_citations(data_source, variable, temporal_resolution, aggregation, site_ids=None):
    """
    Print and/or return specific citation information.

    Parameters
    ----------
    data_source : str
        Source from which requested data originated. Currently supported: 'usgs_nwis', 'usda_nrcs',
        'ameriflux'.
    variable : str
        Description of type of data requested. Currently supported: 'streamflow', 'wtd', 'swe',
        'precipitation', 'temperature', 'soil moisture', 'latent heat flux', 'sensible heat flux',
        'shortwave radiation', 'longwave radiation', 'vapor pressure deficit', 'wind speed'.
    temporal_resolution : str
        Collection frequency of data requested. Currently supported: 'daily', 'hourly', and 'instantaneous'.
        Please see the documentation for allowable combinations with `variable`.
    aggregation : str
        Additional information specifying the aggregation method for the variable to be returned.
        Options include descriptors such as 'average' and 'total'. Please see the documentation
        for allowable combinations with `variable`.
    site_ids : list; default None
        If provided, the specific list of sites to return site DOIs for. This is only
        supported if `data_source` == 'ameriflux'.

    Returns
    -------
    None or DataFrame of site-specific DOIs
        Nothing returned unless data_source == `ameriflux` and the parameter `site_ids` is provided.
    """
    try:
        assert data_source in ["usgs_nwis", "usda_nrcs", "ameriflux"]
    except:
        raise ValueError(
            f"Unexpected value of data_source, {data_source}. Supported values include 'usgs_nwis', 'usda_nrcs', and 'ameriflux'"
        )

    if data_source == "usgs_nwis":
        print(
            """Most U.S. Geological Survey (USGS) information resides in Public Domain 
              and may be used without restriction, though they do ask that proper credit be given.
              An example credit statement would be: "(Product or data name) courtesy of the U.S. Geological Survey"
              Source: https://www.usgs.gov/information-policies-and-instructions/acknowledging-or-crediting-usgs"""
        )

    elif data_source == "usda_nrcs":
        print(
            """Most information presented on the USDA Web site is considered public domain information. 
                Public domain information may be freely distributed or copied, but use of appropriate
                byline/photo/image credits is requested. 
                Attribution may be cited as follows: "U.S. Department of Agriculture"
                Source: https://www.usda.gov/policies-and-links"""
        )

    elif data_source == "ameriflux":
        print(
            """All AmeriFlux sites provided by the HydroData service follow the CC-BY-4.0 License.
                The CC-BY-4.0 license specifies that the data user is free to Share (copy and redistribute 
                the material in any medium or format) and/or Adapt (remix, transform, and build upon the 
                material) for any purpose.
            
                Users of this data must acknowledge the AmeriFlux data resource with the following statement:
                "Funding for the AmeriFlux data portal was provided by the U.S. Department of Energy Office 
                of Science."
            
                Additionally, for each AmeriFlux site used, you must provide a citation to the site's 
                data product that includes the data product DOI. The DOI for each site is included in the 
                full metadata query. Alternately, a site list can be provided to this get_citation_information
                function to return each site-specific DOI.
            
                Source: https://ameriflux.lbl.gov/data/data-policy/"""
        )

        if site_ids is not None:
            metadata_df = get_metadata(data_source, variable, temporal_resolution, aggregation, site_ids=site_ids)
        return metadata_df[['site_id', 'doi']]


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
        if key == "return_metadata":
            if not isinstance(value, str):
                options[key] = str(value)
        if key == "all_attributes":
            if not isinstance(value, str):
                options[key] = str(value)
    return options


def _validate_user():
    email, pin = get_registered_api_pin()
    url_security = f"{HYDRODATA_URL}/api/api_pins?pin={pin}&email={email}"
    response = requests.get(url_security, headers=None, timeout=15)
    if not response.status_code == 200:
        raise ValueError(
            f"User Validation Failed.  The email '{email}' may not be registered at https://hydrogen.princeton.edu/pin, may not be registered on this machine using the register_pin() function, or may not be registered with the same pin at https://hydrogen.princeton.edu/pin as was registered on this machine using register_pin(). See documentation to register with an email and pin."
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


def get_registered_api_pin() -> Tuple[str, str]:
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


def _check_inputs(data_source, variable, temporal_resolution, aggregation, *args, **kwargs):
    """
    Checks on inputs to get_observations function.

    Parameters
    ----------
    data_source : str
        Source from which requested data originated. Currently supported: 'usgs_nwis', 'usda_nrcs', 
        'ameriflux'.
    variable : str
        Description of type of data requested. Currently supported: 'streamflow', 'wtd', 'swe', 
        'precipitation', 'temperature', 'soil moisture', 'latent heat flux', 'sensible heat flux', 
        'shortwave radiation', 'longwave radiation', 'vapor pressure deficit', 'wind speed'.
    temporal_resolution : str
        Collection frequency of data requested. Currently supported: 'daily', 'hourly', and 'instantaneous'.
    aggregation : str
        Additional information specifying the aggregation method for the variable to be returned. 
        Options include descriptors such as 'average' and 'total'. Please see the documentation
        for allowable combinations with `variable`.
    args :
        Optional positional parameters that must be a dict with filter options.
    kwargs :
        Supports multiple named parameters with filter option values.

    Keyword Arguments
    --------------------
    depth_level : int
        Depth level in inches at which the measurement is taken. Necessary for `variable` = 'soil moisture'.

    Returns
    -------
    None
    """
    if len(args) > 0 and isinstance(args[0], dict):
        options = args[0]
    else:
        options = kwargs

    try:
        assert temporal_resolution in ['daily', 'hourly', 'instantaneous']
    except:
        raise ValueError(
            f"Unexpected value for temporal_resolution, {temporal_resolution}. Please see the documentation for allowed values.")

    try:
        assert variable in ['streamflow', 'wtd', 'swe', 'precipitation', 'temperature', 'soil moisture',
                            'latent heat flux', 'sensible heat flux', 'shortwave radiation', 'longwave radiation',
                            'vapor pressure deficit', 'wind speed']
    except:
        raise ValueError(f"Unexpected value for variable, {variable}. Please see the documentation for allowed values.")

    try:
        assert aggregation in ['average', 'instantaneous', 'total', 'total, snow-adjusted',
                               'start-of-day', 'accumulated', 'minimum', 'maximum']
    except:
        raise ValueError(
            f"Unexpected value for aggregation, {aggregation}. Please see the documentation for allowed values.")

    try:
        assert data_source in ['usgs_nwis', 'usda_nrcs', 'ameriflux']
    except:
        raise ValueError(
            f"Unexpected value for data_source, {data_source} Please see the documentation for allowed values.")

    if variable == 'soil moisture':
        try:
            assert 'depth_level' in options
            assert options['depth_level'] in [2, 4, 8, 20, 40]
        except:
            raise ValueError(
                "Please provide depth_level with one of the supported values. Please see the documentation for allowed values.")


def _get_var_id(conn, data_source, variable, temporal_resolution, aggregation, *args, **kwargs):
    """
    Return mapped var_id.

    Parameters
    ----------
    conn : Connection object
        The Connection object associated with the SQLite database to 
        query from. 
    data_source : str
        Source from which requested data originated. Currently supported: 'usgs_nwis', 'usda_nrcs', 
        'ameriflux'.    
    variable : str
        Description of type of data requested. Currently supported: 'streamflow', 'wtd', 'swe', 
        'precipitation', 'temperature', 'soil moisture', 'latent heat flux', 'sensible heat flux', 
        'shortwave radiation', 'longwave radiation', 'vapor pressure deficit', 'wind speed'.
    temporal_resolution : str
        Collection frequency of data requested. Currently supported: 'daily', 'hourly', and 'instantaneous'.
    aggregation : str
        Additional information specifying the aggregation method for the variable to be returned. 
        Options include descriptors such as 'average' and 'total'. Please see the documentation
        for allowable combinations with `variable`.
    args :
        Optional positional parameters that must be a dict with filter options.
    kwargs :
        Supports multiple named parameters with filter option values.

    Keyword Arguments
    --------------------
    depth_level : int
        Depth level in inches at which the measurement is taken. Necessary for `variable` = 'soil moisture'.

    Returns
    -------
    var_id : int
        Integer variable ID associated with combination of `data_source`, `variable`, `temporal_resolution`,
        and `aggregation`.
    """
    if len(args) > 0 and isinstance(args[0], dict):
        options = args[0]
    else:
        options = kwargs

    if variable == 'soil moisture':
        query = """
                SELECT var_id 
                FROM variables
                WHERE data_source = ?
                    AND variable = ?
                    AND temporal_resolution = ?
                    AND aggregation = ?
                    AND depth_level = ?
                """
        param_list = [data_source, variable, temporal_resolution, aggregation, options['depth_level']]

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
        return int(result['var_id'][0])
    except:
        raise ValueError(
            'The provided combination of data_source, variable, temporal_resolution, and aggregation is not currently supported.')


def _get_dirpath(var_id):
    """
    Map variable with location of data on /hydrodata.

    Parameters
    ----------
    var_id : int
        Integer variable ID associated with combination of `data_source`, 
        `variable`, `temporal_resolution`, and `aggregation`.

    Returns
    -------
    dirpath : str
        Directory path for observation data location.
    """
    dirpath_map = {1: '/hydrodata/national_obs/streamflow/data/hourly',
                   2: '/hydrodata/national_obs/streamflow/data/daily',
                   3: '/hydrodata/national_obs/groundwater/data/hourly',
                   4: '/hydrodata/national_obs/groundwater/data/daily',
                   5: '',
                   6: '/hydrodata/national_obs/swe/data/daily',
                   7: '/hydrodata/national_obs/point_meteorology/NRCS_precipitation/data/daily',
                   8: '/hydrodata/national_obs/point_meteorology/NRCS_precipitation/data/daily',
                   9: '/hydrodata/national_obs/point_meteorology/NRCS_precipitation/data/daily',
                   10: '/hydrodata/national_obs/point_meteorology/NRCS_temperature/data/daily',
                   11: '/hydrodata/national_obs/point_meteorology/NRCS_temperature/data/daily',
                   12: '/hydrodata/national_obs/point_meteorology/NRCS_temperature/data/daily',
                   13: '/hydrodata/national_obs/soil_moisture/data/daily',
                   14: '/hydrodata/national_obs/soil_moisture/data/daily',
                   15: '/hydrodata/national_obs/soil_moisture/data/daily',
                   16: '/hydrodata/national_obs/soil_moisture/data/daily',
                   17: '/hydrodata/national_obs/soil_moisture/data/daily',
                   18: '/hydrodata/national_obs/ameriflux/data/hourly',
                   19: '/hydrodata/national_obs/ameriflux/data/hourly',
                   20: '/hydrodata/national_obs/ameriflux/data/hourly',
                   21: '/hydrodata/national_obs/ameriflux/data/hourly',
                   22: '/hydrodata/national_obs/ameriflux/data/hourly',
                   23: '/hydrodata/national_obs/ameriflux/data/hourly',
                   24: '/hydrodata/national_obs/ameriflux/data/hourly'}

    return dirpath_map[var_id]


def _get_sites(conn, data_source, variable, temporal_resolution, aggregation, *args, **kwargs):
    """
    Build DataFrame with site attribute metadata information.

    Parameters
    ----------
    conn : Connection object
        The Connection object associated with the SQLite database to 
        query from. 
    data_source : str
        Source from which requested data originated. Currently supported: 'usgs_nwis', 'usda_nrcs', 
        'ameriflux'.   
    variable : str
        Description of type of data requested. Currently supported: 'streamflow', 'wtd', 'swe', 
        'precipitation', 'temperature', 'soil moisture', 'latent heat flux', 'sensible heat flux', 
        'shortwave radiation', 'longwave radiation', 'vapor pressure deficit', 'wind speed'.
    temporal_resolution : str
        Collection frequency of data requested. Currently supported: 'daily', 'hourly', and 'instantaneous'.
        Please see the documentation for allowable combinations with `variable`.
    aggregation : str
        Additional information specifying the aggregation method for the variable to be returned. 
        Options include descriptors such as 'average' and 'total'. Please see the documentation
        for allowable combinations with `variable`.
    args :
        Optional positional parameters that must be a dict with filter options.
    kwargs :
        Supports multiple named parameters with filter option values.

    Keyword Arguments
    --------------------
    depth_level : int
        Depth level in inches at which the measurement is taken. Necessary for `variable` = 'soil moisture'.
    date_start : str; default=None
        'YYYY-MM-DD' date indicating beginning of time range.
    date_end : str; default=None
        'YYYY-MM-DD' date indicating end of time range.
    latitude_range : tuple; default=None
        Latitude range bounds for the geographic domain; lesser value is provided first.
    longitude_range : tuple; default=None
        Longitude range bounds for the geographic domain; lesser value is provided first.
    site_ids : list; default=None
        List of desired (string) site identifiers.
    state : str; default=None
        Two-letter postal code state abbreviation.
    polygon : str
        Path to location of shapefile. Must be readable by PyShp's `shapefile.Reader()`.
    site_networks: list
        List of names of site networks. Can be a list with a single network name.
        Each network must have matching .csv file with a list of site ID values that comprise
        the network. This .csv file must be located under network_lists/{data_source}/{variable}
        in the package directory and named as 'network_name'.csv. Eg: `site_networks=['gagesii']`

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
    var_id = _get_var_id(conn, data_source, variable, temporal_resolution, aggregation, *args, **kwargs)

    param_list = [var_id]

    # Date start
    if 'date_start' in options and options['date_start'] is not None:
        date_start_query = """ AND last_date_data_available >= ?"""
        param_list.append(options['date_start'])
    else:
        date_start_query = """"""

    # Date end
    if 'date_end' in options and options['date_end'] is not None:
        date_end_query = """ AND first_date_data_available <= ?"""
        param_list.append(options['date_end'])
    else:
        date_end_query = """"""

    # Latitude
    if 'latitude_range' in options and options['latitude_range'] is not None:
        lat_query = """ AND latitude BETWEEN ? AND ?"""
        param_list.append(options['latitude_range'][0])
        param_list.append(options['latitude_range'][1])
    else:
        lat_query = """"""

    # Longitude
    if 'longitude_range' in options and options['longitude_range'] is not None:
        lon_query = """ AND longitude BETWEEN ? AND ?"""
        param_list.append(options['longitude_range'][0])
        param_list.append(options['longitude_range'][1])
    else:
        lon_query = """"""

    # Site ID
    if 'site_ids' in options and options['site_ids'] is not None:
        site_query = """ AND s.site_id IN (%s)""" % ','.join('?'*len(options['site_ids']))
        for s in options['site_ids']:
            param_list.append(s)
    else:
        site_query = """"""

    # State
    if 'state' in options and options['state'] is not None:
        state_query = """ AND state == ?"""
        param_list.append(options['state'])
    else:
        state_query = """"""

    # Site Networks
    if 'site_networks' in options and options['site_networks'] is not None:
        network_site_list = _get_network_site_list(data_source, variable, options['site_networks'])
        network_query = """ AND s.site_id IN (%s)""" % ','.join('?'*len(network_site_list))
        for s in network_site_list:
            param_list.append(s)
    else:
        network_query = """"""

    query = """
            SELECT s.site_id, s.site_name, s.site_type, s.agency, s.state,
                   s.latitude, s.longitude, s.huc, o.first_date_data_available,
                   o.last_date_data_available, o.record_count, s.site_query_url,
                   s.date_metadata_last_updated, s.tz_cd, s.doi
            FROM sites s
            INNER JOIN observations o
            ON s.site_id = o.site_id AND o.var_id == ?
            WHERE first_date_data_available <> 'None'
            """ + date_start_query + date_end_query + lat_query + lon_query + site_query + state_query + network_query

    df = pd.read_sql_query(query, conn, params=param_list)

    # Polygon shapefile provided
    if 'polygon' in options and options['polygon'] is not None:

        # Read in shapefile
        shp = shapefile.Reader(options['polygon'])

        # Convert features to shapely geometries
        try:
            assert len(shp.shapeRecords()) == 1
        except:
            raise Exception("Please make sure your input shapefile contains only a single shape feature.")

        feature = shp.shapeRecords()[0].shape.__geo_interface__
        shp_geom = shape(feature)

        # Make sure CRS aligns between polygon and lat/lon points
        try:
            assert 'polygon_crs' in options and options['polygon_crs'] is not None
        except:
            raise Exception(
                """Please provide 'polygon_crs' with a CRS definition accepted by pyproj.CRS.from_user_input() 
                   to specify this shape's CRS.""")

        shp_crs = pyproj.CRS.from_user_input(options['polygon_crs'])

        project = pyproj.Transformer.from_crs(
            shp_crs, pyproj.CRS('EPSG:4326'), always_xy=True).transform
        shp_geom_crs = transform(project, shp_geom)

        # Clip points to only those within the polygon
        df['clip'] = df.apply(lambda x: contains_xy(shp_geom_crs, x['longitude'], x['latitude']), axis=1)
        clipped_df = df[df['clip'] == True].reset_index().drop(columns=['index', 'clip'])

        return clipped_df

        # gdf = gpd.GeoDataFrame(
        #     df, geometry=gpd.points_from_xy(df.longitude, df.latitude), crs="EPSG:4326")
        # mask = gpd.read_file(options['polygon'])

        # try:
        #     assert mask.crs is not None
        #     mask_proj = mask.to_crs('EPSG:4326')
        # except:
        #     raise Exception('Please make sure the referenced shapefile has a crs defined.')

        # clipped_points = gpd.clip(gdf, mask_proj)
        # clipped_df = pd.DataFrame(clipped_points.drop(columns='geometry'))
        # return clipped_df

    else:
        return df


def _get_network_site_list(data_source, variable, site_networks):
    """
    Return list of site IDs for desired network of observation sites.

    Parameters
    ----------
    data_source : str
        Source from which requested data originated. Currently supported: 'usgs_nwis', 'usda_nrcs', 
        'ameriflux'.   
    variable : str
        Description of type of data requested. Currently supported: 'streamflow', 'wtd', 'swe', 
        'precipitation', 'temperature', 'soil moisture', 'latent heat flux', 'sensible heat flux', 
        'shortwave radiation', 'longwave radiation', 'vapor pressure deficit', 'wind speed'.
    site_networks: list
        List of names of site networks. Can be a list with a single network name.
        Each network must have matching .csv file with a list of site ID values that comprise
        the network. This .csv file must be located under network_lists/{data_source}/{variable}
        in the package directory and named as 'network_name'.csv.

    Returns
    -------
    site_list: list
        List of site ID strings for sites belonging to named network.
    """
    network_options = {'usgs_nwis': {'streamflow': ['camels', 'gagesii_reference', 'gagesii', 'hcdn2009'],
                                     'wtd': ['climate_response_network']}}

    # Initialize final site list
    site_list = []

    # Append sites from desired network(s)
    for network in site_networks:
        try:
            assert network in network_options[data_source][variable]
            df = pd.read_csv(f'{NETWORK_LISTS_PATH}/{data_source}/{variable}/{network}.csv',
                             dtype=str, header=None, names=['site_id'])
            site_list += list(df['site_id'])
        except:
            raise ValueError(
                f'Network option {network} is not recognized. Please make sure the .csv network_lists/{data_source}/{variable}/{network}.csv exists.')

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
        cleaned_huc = ''

    # If 7 or 11 digits, add a leading 0
    elif len(huc) in (7, 11):
        huc = '0' + huc

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
    sites = pd.Series(ds['site'].to_numpy())
    dates = pd.Series(ds['date'].to_numpy()).astype(str)
    data = ds.to_numpy()

    df = pd.DataFrame(data.T, columns=sites)
    df['date'] = dates

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
        Integer variable ID associated with combination of `data_source`,
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
    file_list = [f'{dirpath}/{site}.nc' for site in site_list]

    varname_map = {'1': 'streamflow', '2': 'streamflow', '3': 'wtd', '4': 'wtd', '5': 'wtd',
                   '6': 'swe', '7': 'precip_acc', '8': 'precip_inc', '9': 'precip_inc_sa',
                   '10': 'temp_min', '11': 'temp_max', '12': 'temp_avg',
                   '13': 'sms_2in', '14': 'sms_4in', '15': 'sms_8in', '16': 'sms_20in', '17': 'sms_40in',
                   '18': 'latent heat flux', '19': 'sensible heat flux', '20': 'shortwave radiation',
                   '21': 'longwave radiation', '22': 'vapor pressure deficit', '23': 'air temperature',
                   '24': 'wind speed'}

    varname = varname_map[str(var_id)]

    if 'date_start' in options:
        date_start_dt = np.datetime64(options['date_start'])
    if 'date_end' in options:
        date_end_dt = np.datetime64(options['date_end'])

    print('collecting data...')

    for i in range(len(site_list)):

        # open single site file
        temp = xr.open_dataset(file_list[i])[varname]

        # make date variable name consistent
        date_var = list(temp.coords)[0]
        temp = temp.rename({date_var: 'datetime'})

        # convert date string to datetime values
        temp['datetime'] = pd.DatetimeIndex(temp['datetime'].values)

        # subset to only observations within desired time range
        if ('date_start' not in options) and ('date_end' not in options):
            temp_wy = temp
        elif ('date_start' not in options) and ('date_end' in options):
            temp_wy = temp.sel(datetime=(temp.datetime <= date_end_dt))
        elif ('date_start' in options) and ('date_end' not in options):
            temp_wy = temp.sel(datetime=(temp.datetime >= date_start_dt))
        elif ('date_start' in options) and ('date_end' in options):
            temp_wy = temp.sel(datetime=(temp.datetime >= date_start_dt) & (temp.datetime <= date_end_dt))

        if i == 0:
            ds = temp_wy
        else:
            ds = xr.concat([ds, temp_wy], dim='site')

    if len(site_list) == 1:
        ds = ds.expand_dims(dim='site')

    ds = ds.assign_coords({'site': (site_list)})
    ds = ds.rename({'datetime': 'date'})

    print('data collected.')

    data_df = _convert_to_pandas(ds)
    if 'min_num_obs' in options and options['min_num_obs'] is not None:
        return _filter_min_num_obs(data_df, options['min_num_obs'])
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
        Integer variable ID associated with combination of `data_source`, 
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
    if 'min_num_obs' not in options or options['min_num_obs'] is None:
        min_num_obs = 1
    else:
        min_num_obs = options['min_num_obs']

    if ('date_start' not in options) and ('date_end' not in options):
        date_query = """"""
        param_list = [min_num_obs]
    elif ('date_start' not in options) and ('date_end' in options):
        date_query = """ WHERE w.date <= ?"""
        param_list = [options['date_end'], min_num_obs, options['date_end']]
    elif ('date_start' in options) and ('date_end' not in options):
        date_query = """ WHERE w.date >= ?"""
        param_list = [options['date_start'], min_num_obs, options['date_start']]
    elif ('date_start' in options) and ('date_end' in options):
        date_query = """ WHERE w.date >= ? AND w.date <= ?"""
        param_list = [
            options['date_start'],
            options['date_end'],
            min_num_obs, options['date_start'],
            options['date_end']]

    query = """
            SELECT w.site_id, w.date, w.wtd, w.pumping_status
            FROM wtd_discrete_data AS w
            INNER JOIN (SELECT w.site_id, COUNT(*) AS num_obs
                FROM wtd_discrete_data AS w
                """ + date_query + """
                GROUP BY site_id
                HAVING num_obs >= ?) AS c
            ON w.site_id = c.site_id
            """ + date_query

    df = pd.read_sql_query(query, conn, params=param_list)

    return df
