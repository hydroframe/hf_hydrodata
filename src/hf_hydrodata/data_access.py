"""
Functions to access data from the data catalog index of the GPFS files.
"""

# pylint: disable=W0603,C0103,E0401,W0702,C0209,C0301,R0914,R0912,W1514,E0633,R0915,R0913,C0302,W0632
import os
import datetime
import logging
import io
from typing import List
import json
import shutil
import requests
import yaml
from dateutil import rrule
from dateutil.relativedelta import relativedelta
import pyproj
import numpy as np
import xarray as xr
from parflow import read_pfb_sequence
from parflow.tools.io import read_clm
from hf_hydrodata.projection import to_conic, from_conic
from hf_hydrodata.data_model_access import ModelTableRow
from hf_hydrodata.data_model_access import load_data_model

CONIC_TRANSFORMER = pyproj.Transformer.from_crs("EPSG:4326", "ESRI:102004")
LATLNG_TRANSFORMER = pyproj.Transformer.from_crs("ESRI:102004", "EPSG:4326")

C_PFB_MAP = {
    "eflx_lh_tot": 0,
    "eflx_lwrad_out": 1,
    "eflx_sh_tot": 2,
    "eflx_soil_grnd": 3,
    "qflx_evap_tot": 4,
    "qflx_evap_grnd": 5,
    "qflx_evap_soil": 6,
    "qflx_evap_veg": 7,
    "qflx_tran_veg": 8,
    "qflx_infl": 9,
    "swe_out": 10,
    "t_grnd": 11,
    "qflx_qirr": 12,
    "tsoil": 13,
}


HYDRODATA = "/hydrodata"
URL = "https://hydro-dev.princeton.edu/api/data-file?"


def get_catalog_entries(*args, **kwargs) -> List[ModelTableRow]:
    """
    Get data catalog entry rows selected by filter options.

    Args:
        args:           Optional positional parameter that must be a dict with filter options.
        kwargs:         Supports multiple named parameters with filter option values.
    Returns:
        A list of ModelTableRow entries that match the filter options.

    A filter option is any column name in the data_catalog_entry table.
    The value of the filter option argument must be a value of that column in the table.

    For example,

        entries = get_catalog_entries(
            dataset="NLDAS2", period="daily", 
            variable="precipitation", start_time="2003-01-01")

            or

        options = {"dataset": "NLDAS2", "period": "daily", 
                   "variable": "precipitation", "start_time":"2003-01-01"}

        entries = get_catalog_entries(options)

        # 9 forcing variables and all metadata

        assert len(entries) == 9

        assert len(entries[0].column_names()) == 20

        assert entries[0]["variable"] == "precipitation"

    Note, it is better to filter by dimension options like "variable" instead of free-form volitile options like "dataset_var".
    """

    result = []
    if len(args) > 0 and isinstance(args[0], dict):
        options = args[0]
    else:
        options = kwargs
    data_model = load_data_model()
    table = data_model.get_table("data_catalog_entry")
    for row_id in table.row_ids:
        row = table.get_row(row_id)
        if _is_row_match_options(row, options):
            result.append(row)
    return result


def get_catalog_entry(*args, **kwargs) -> ModelTableRow:
    """
    Get a single data catalog entry row selected by filter options.

    Args:
        args:           Optional positional parameters that must be a dict with filter options.
        kwargs:         Supports multiple named parameters with filter option values.
    Returns:
        A single of ModelTableRow entries that matches the filter options or None if no entry matches the filter options.
    Raises:
        ValueError      If the filter options do not uniquely select a single data_catalog_entry.

    This method is the same as get_catalog_entries() except returns a single row rather than a list of rows.
    For example,
        options = {"dataset": "NLDAS2", "period": "daily",
                   "variable": "precipitation", "start_time": "2005-7-1"}
        entry = get_catalog_entry(options)
    """

    entries = get_catalog_entries(*args, **kwargs)
    entry = _get_preferred_catalog_entry(entries)
    return entry


def _get_preferred_catalog_entry(entries: List[dict]) -> dict:
    """
    Return the preferred catalog entry.
    Parameters:
        entries:        A list of catalog entries that satisfy a filter.
    Returns:
        The preferred catalog entry or None if the entries are empty.
    Raises:
        ValueError if there is more than one entry with the same file_type.
    """
    if len(entries) == 0:
        result = None
    elif len(entries) == 1:
        result = entries[0]
    else:
        preferred_file_types = ["pfb", "tif", "netcdf"]
        id_1 = None
        file_type_1_index = 1000
        result = None
        for entry in entries:
            id_2 = entry["id"]
            file_type_2 = entry["file_type"]
            if file_type_2 in preferred_file_types:
                if preferred_file_types.index(file_type_2) < file_type_1_index:
                    result = entry
                    id_1 = id_2
                    file_type_1_index = preferred_file_types.index(file_type_2)
                elif preferred_file_types.index(file_type_2) == file_type_1_index:
                    raise ValueError(
                        f"Ambiguous filter result. Could be {id_1} or {id_2}."
                    )
            elif id_1 is None:
                result = entry
                id_1 = id_2
            elif file_type_1_index == 1000:
                raise ValueError(f"Ambiguous filter result. Could be {id_1} or {id_2}.")

    return result


def get_table_names() -> List[str]:
    """Return the list of table names in the data model."""

    data_model = load_data_model()
    return data_model.table_names


def get_table_rows(table_name: str, *args, **kwargs) -> List[ModelTableRow]:
    """
    Get rows of a data model table filtered by columns from that table.

    Args:
        table_name:     The name of a table in the data model.
        args:           Optional positional parameter that must be a dict with filter options.
        kwargs:         Supports multiple named parameters with filter option values.
    Returns:
        A list of ModelTableRow entries of the specified table_name that match the filter options.

    For example,
        rows = get_table_rows("variable", variable_type="atmospheric")

        assert row[0]["variable"] == "air_temp"
    """

    result = []
    if len(args) > 1 and isinstance(args[0], dict):
        options = args[0]
    else:
        options = kwargs
    data_model = load_data_model()
    table = data_model.get_table(table_name)
    for row_id in table.row_ids:
        row = table.get_row(row_id)
        if _is_row_match_options(row, options):
            result.append(row)
    return result


def get_table_row(table_name: str, *args, **kwargs) -> ModelTableRow:
    """
    Get one row of a data model table filtered by columns from that table.

    Args:
        table_name:     The name of a table in the data model.
        args:           Optional positional parameter that must be a dict with filter options.
        kwargs:         Supports multiple named parameters with filter option values.
    Returns:
        A single of ModelTableRow entries of the specified table_name that match the filter options or None if now row is found.
    Raises:
        ValueError if the filter options are ambiguous and this matches more than one row.
    For example,
        row = get_table_rows("variable", variable_type="atmospheric")

        assert row["variable"] == "air_temp"
    """

    rows = get_table_rows(table_name, *args, **kwargs)
    if len(rows) == 0:
        return None
    if len(rows) > 1:
        id1 = rows[0]["id"]
        id2 = rows[1]["id"]
        raise ValueError(f"Ambiguous result could be id {id1} or {id2}")
    return rows[0]


def get_file_paths(entry, *args, **kwargs) -> List[str]:
    """
    Get a list of file paths from the data catalog identified by the filter options.

    Args:
        entry:          Either a ModelTableRow or the ID number of a data_catalog_entry.
        args:           Optional positional parameter that must be a dict with data filter options.
        kwargs:         Supports multiple named parameters with data filter option values.
    Returns:
        A list of absolute path names to files of the data catalog entry
        after looping through time range and replacing substitution keys.

    A data filter option must be one of the following:
        * start_time:     A time as either a datetime object or a string in the form YYYY-MM-DD. Start of the date range for data.
        * end_time:       A time as either a datetime object or a string in the form YYYY-MM-DD. End of the date range for data.
        * site_id:        An observation point site id.

    If only start_time is specified then only the data at that time is used to substitute into path names.
    If both start_time and end_time is specified then dates in the date range are used to substitute into path names.

    For example, get 3 daily files bewteen 9/30/2021 and 10/3/2021.
        entry = get_catalog_entry(dataset="NLDAS2", period="daily",
                  variable="precipitation", start_time="2021-09-30", end_time="2021-10-03")

        paths = get_file_paths(entry, start_time="2021-09-30", end_time="2021-10-03")

        assert len(paths) = 3
    """
    result = []
    if isinstance(entry, ModelTableRow):
        path = entry["path"]
        period = entry["period"]
    elif isinstance(entry, (int, str)):
        data_model = load_data_model()
        table = data_model.get_table("data_catalog_entry")
        entry = table.get_row(str(entry))
        path = entry["path"]
        period = entry["period"]
    if len(args) > 0 and isinstance(args[0], dict):
        options = args[0]
    else:
        options = kwargs
    if entry is None:
        data_catalog_entry_id = options.get("data_catalog_entry_id")
        if data_catalog_entry_id is not None:
            entry = get_table_row("data_catalog_entry", id=data_catalog_entry_id)
            period = entry["period"]
            path = entry["path"]
        else:
            entry = get_catalog_entry(*args, **kwargs)
            period = entry["period"]
            path = entry["path"]
    if entry is None:
        raise ValueError("No data catalog entry provided")

    # Get option parameters
    start_time_value = _parse_time(options.get("start_time"))
    end_time_value = _parse_time(options.get("end_time"))

    # Populate result path names with path names for each time value in time period
    if period in ["daily"] and start_time_value:
        # Both daily and hourly are stored in files by day, but hourly just uses different substitution
        time_value = start_time_value
        if end_time_value is None:
            end_time_value = start_time_value + datetime.timedelta(days=1)
        while time_value < end_time_value:
            datapath = _substitute_datapath(path, entry, options, time_value=time_value)
            if datapath not in result:
                result.append(datapath)
            time_value = time_value + datetime.timedelta(days=1)
    elif period in ["hourly"] and start_time_value:
        # Both daily and hourly are stored in files by day, but hourly just uses different substitution
        time_value = start_time_value
        if end_time_value is None:
            end_time_value = start_time_value + datetime.timedelta(hours=1)
        while time_value < end_time_value:
            datapath = _substitute_datapath(path, entry, options, time_value=time_value)
            if datapath not in result:
                result.append(datapath)
            time_value = time_value + datetime.timedelta(hours=1)
    elif period == "monthly" and start_time_value:
        time_value = start_time_value
        if end_time_value is None:
            end_time_value = start_time_value + relativedelta(months=1)
        while time_value < end_time_value:
            datapath = _substitute_datapath(path, entry, options, time_value=time_value)
            if datapath not in result:
                result.append(datapath)
            time_value = time_value + relativedelta(months=1)
    else:
        time_value = start_time_value
        datapath = _substitute_datapath(path, entry, options, time_value=time_value)
        result.append(datapath)
    return result


def _construct_string_from_qparams(entry, options):
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
    qparam_values["dataset"] = entry["dataset"]
    qparam_values["period"] = entry["period"]
    qparam_values["variable"] = entry["variable"]
    qparam_values["file_type"] = entry["file_type"]
    qparam_values["grid"] = entry["grid"]

    string_parts = [
        f"{name}={value}" for name, value in qparam_values.items() if value is not None
    ]
    result_string = "&".join(string_parts)
    return result_string


def get_file_path(entry, *args, **kwargs) -> List[str]:
    """Get the file path for a data catalog entry.

    Args:
        entry:          Either a ModelTableRow or the ID number of a data_catalog_entry. If None use the entry found by the filters.
        args:           Optional positional parameter that must be a dict with data filter options.
        kwargs:         Supports multiple named parameters with data filter option values.
    Returns:
        A single path path for the data catalog entry
    Raises:
        ValueError      if there is no path or multiple paths for the data catalog entry.
    """
    paths = get_file_paths(entry, *args, **kwargs)
    if len(paths) == 0:
        raise ValueError("No file path found for data catalog entry")
    if len(paths) > 1:
        raise ValueError("More than one file path for data catalog entry")
    result = paths[0]
    return result


def get_numpy_data(*args, **kwargs) -> np.ndarray:
    """
    Get a numpy ndarray from files in /hydroframe. with the applied data filters.
    If a time_values data filter option is provided as an arrat this array will be populated with the
    date strings of the date dimentions of the ndarray that is returned. This is used for graphing use cases.

    Args:
        entry:          Either a ModelTableRow or the ID number of a data_catalog_entry. If None then get entry from options.
        args:           Optional positional parameter that must be a dict with data filter options.
        kwargs:         Supports multiple named parameters with data filter option values.
    Returns:
        A numpy ndarray containing the data loaded from the files identified by the entry and sliced by the data filter options.
    Raises:
        ValueError if both grid_bounds and latlng_bounds are specified as data filters.

    A data filter option must be one of the following:
        * start_time:     A time as either a datetime object or a string in the form YYYY-MM-DD. Start of the date range for data.
        * end_time:       A time as either a datetime object or a string in the form YYYY-MM-DD. End of the date range for data.
        * site_id:        An observation point site id.
        * grid_bounds:    An array (or string representing an array) of points [left, bottom, right, top] in xy grid corridates in the grid of the data.
        * latlng_bounds:  An array (or string representing an array) of points [left, bottom, right, top] in lat/lng coordinates mapped with the grid of the data.
        * depth:          A number 0-n of the depth index of the data to return.
        * z:              A value of the z dimension to be used as a filter to remove this dismension.
        * level:          A HUC level integer when reading HUC boundary files.

    For gridded results the returned numpy array has dimensions:
        * [hour, y, x]`````               period is hourly without z dimension
        * [day, y, x]                     period is daily without z dimension
        * [month, y, x]                   period is monthly without z dimension
        * [y, x]                          period is static or blank without z dimension

        * [hour, z, y, x]`````            period is hourly with z dimension
        * [day, z, y, x]                  period is daily with z dimension
        * [month, z, y, x]                period is monthly with z dimension
        * [z, y, x]                       period is static or blank with z dimension

    If the dataset has ensembles then there is an ensemble dimension at the beginning.

    Both start_time and end_time must be in the form "YYYY-MM-DD HH:MM:SS" or "YYYY-MM-DD" or a datetime object.

    If only start_time is specified than only that month/day/hour is returned.
    The start_time is inclusive the end_time is exclusive (data returned less than that time).

    If either grid_bounds or latlng_bounds is specified then the result is sliced by the x,y values in the bounds.

    If z is specified then the result is sliced by the z dimension.

    For example, to get data from the 3 daily files bewteen 9/30/2021 and 10/3/2021.
        bounds = [200, 200, 300, 250]

        options = {"dataset": "NLDAS2", "period": "daily", "variable": "precipitation",
                    "start_time="2021-09-30", end_time="2021-10-03", grid_bounds = bounds)
        data = get_numpy_data(options)
        metadata = get_catalog_entry(options)

        # The result has 3 days in the time dimension and sliced to x,y shape 100x50 at origin 200, 200 in the conus1 grid.

        assert data.shape == [3, 100, 50]
    """

    result = get_ndarray(None, *args, **kwargs)
    return result


def _construct_string_from_options(qparam_values):
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
        f"{name}={value}" for name, value in qparam_values.items() if value is not None
    ]
    result_string = "&".join(string_parts)
    return result_string


def _write_file_from_api(filepath, options):
    """Get the hydroframe file that is selected by the options to the given filepath.

    Args:
        filepath:          Either a ModelTableRow or the ID number of a data_catalog_entry. If None use the entry found by the filters.
        options:           Optional positional parameter that must be a dict with data filter options.
    Returns:
        None
    Raises:
        ValueError      if there are multiple paths selected from hydroframe.
    """

    q_params = _construct_string_from_options(options)
    url = f"{URL}{q_params}"

    try:
        response = requests.get(url, timeout=180)
        if response.status_code != 200:
            raise ValueError(
                f"The url {url} returned error code {response.status_code}."
            )

    except requests.exceptions.Timeout as e:
        raise ValueError(f"The url {url} has timed out.") from e

    file_obj = io.BytesIO(response.content)
    with open(filepath, "wb") as output_file:
        output_file.write(file_obj.read())


def get_raw_file(filepath, *args, **kwargs):
    """Get the hydroframe file that is selected by the options to the given filepath.

    Args:
        filepath:          Either a ModelTableRow or the ID number of a data_catalog_entry. If None use the entry found by the filters.
        options:           Optional positional parameter that must be a dict with data filter options.
    Returns:
        None
    Raises:
        ValueError      if there are multiple paths selected from hydroframe.
    """
    if len(args) > 0 and isinstance(args[0], dict):
        # The filter options are being passed using a dict
        options = args[0]
    else:
        # The filter options are just named parameters in the argument list
        options = kwargs

    run_remote = not os.path.exists(HYDRODATA)

    if run_remote:
        _write_file_from_api(filepath, options)

    else:
        hydro_filepath = get_file_path(None, options)
        shutil.copy(hydro_filepath, filepath)


def get_ndarray(entry, *args, **kwargs) -> np.ndarray:
    """
    Get a numpy ndarray from files in /hydroframe. with the applied data filters.
    If a time_values data filter option is provided as an arrat this array will be populated with the
    date strings of the date dimentions of the ndarray that is returned. This is used for graphing use cases.

    Args:
        entry:          Either a ModelTableRow or the ID number of a data_catalog_entry. If None then get entry from options.
        args:           Optional positional parameter that must be a dict with data filter options.
        kwargs:         Supports multiple named parameters with data filter option values.
    Returns:
        A numpy ndarray containing the data loaded from the files identified by the entry and sliced by the data filter options.
    Raises:
        ValueError if both grid_bounds and latlng_bounds are specified as data filters.

    A data filter option must be one of the following:
        * start_time:     A time as either a datetime object or a string in the form YYYY-MM-DD. Start of the date range for data.
        * end_time:       A time as either a datetime object or a string in the form YYYY-MM-DD. End of the date range for data.
        * site_id:        An observation point site id.
        * grid_bounds:    An array (or string representing an array) of points [left, bottom, right, top] in xy grid corridates in the grid of the data.
        * latlng_bounds:  An array (or string representing an array) of points [left, bottom, right, top] in lat/lng coordinates mapped with the grid of the data.
        * depth:          A number 0-n of the depth index of the data to return.
        * z:              A value of the z dimension to be used as a filter to remove this dismension.
        * level:          A HUC level integer when reading HUC boundary files.

    For gridded results the returned numpy array has dimensions:
        * [hour, y, x]`````               period is hourly without z dimension
        * [day, y, x]                     period is daily without z dimension
        * [month, y, x]                   period is monthly without z dimension
        * [y, x]                          period is static or blank without z dimension

        * [hour, z, y, x]`````            period is hourly with z dimension
        * [day, z, y, x]                  period is daily with z dimension
        * [month, z, y, x]                period is monthly with z dimension
        * [z, y, x]                       period is static or blank with z dimension

    If the dataset has ensembles then there is an ensemble dimension at the beginning.

    Both start_time and end_time must be in the form "YYYY-MM-DD HH:MM:SS" or "YYYY-MM-DD" or a datetime object.

    If only start_time is specified than only that month/day/hour is returned.
    The start_time is inclusive the end_time is exclusive (data returned less than that time).

    If either grid_bounds or latlng_bounds is specified then the result is sliced by the x,y values in the bounds.

    If z is specified then the result is sliced by the z dimension.

    For example, to get data from the 3 daily files bewteen 9/30/2021 and 10/3/2021.
        bounds = [200, 200, 300, 250]

        options = {"dataset": "NLDAS2", "period": "daily", "variable": "precipitation",
                    "start_time="2021-09-30", end_time="2021-10-03", grid_bounds = bounds)
        entry = get_catalog_entry(options)


        data = get_ndarray(entry, start_time="2021-09-30", end_time="2021-10-03", grid_bounds = bounds)

        # The result has 3 days in the time dimension and sliced to x,y shape 100x50 at origin 200, 200 in the conus1 grid.

        assert data.shape == [3, 100, 50]
    """

    if entry is not None and isinstance(entry, (int, str)):
        # The entry is an ID of data catalog entry, so get the entry row of the table
        data_model = load_data_model()
        table = data_model.get_table("data_catalog_entry")
        entry = table.get_row(str(entry))

    if len(args) > 0 and isinstance(args[0], dict):
        # The filter options are being passed using a dict
        options = args[0]
    else:
        # The filter options are just named parameters in the argument list
        options = kwargs

    if entry is None:
        data_catalog_entry_id = options.get("data_catalog_entry_id")
        if data_catalog_entry_id is not None:
            entry = get_table_row("data_catalog_entry", id=data_catalog_entry_id)
        else:
            entry = get_catalog_entry(*args, **kwargs)

    if entry is None:
        raise ValueError("No entry found in data catalog")
    # An optional empty array passed as an option to be populated with the time dimension for graphing.
    time_values = options.get("time_values")

    data = _get_ndarray_from_api(entry, options, time_values)

    if data is None:
        options = _convert_strings_to_json(options)
        time_values = options.get("time_values")

        file_type = entry["file_type"]
        structure_type = entry["structure_type"]
        data = None
        if file_type == "pfb":
            data = _read_and_filter_pfb_files(entry, options, time_values)
        elif file_type == "C.pfb":
            data = _read_and_filter_c_pfb_files(entry, options, time_values)
        elif file_type == "pfmetadata":
            data = _read_and_filter_pfmetadata_files(entry, options, time_values)
        elif file_type == "vegm":
            data = _read_and_filter_vegm_files(entry, options)
        elif file_type == "netcdf":
            data = _read_and_filter_netcdf_files(entry, options, time_values)
        elif file_type == "tiff":
            data = _read_and_filter_tiff_files(entry, options)
        else:
            raise ValueError(f"File type '{file_type}' is not supported yet.")
        if structure_type == "gridded":
            data = _adjust_dimensions(data, entry)
        options = _convert_json_to_strings(options)

    return data


def _convert_strings_to_json(options):
    """
    Converts strings to jsons.

    Parameters
    ----------
    options : dictionary
        request options.
    """
    for key, value in options.items():
        if key == "latlng_bounds":
            if isinstance(value, str):
                options[key] = json.loads(value)
        if key == "grid_bounds":
            if isinstance(value, str):
                options[key] = json.loads(value)
        if key == "time_values":
            if isinstance(value, str):
                options[key] = json.loads(value)

    return options


# Have to remember to addd any necessary conversions
# to this function, and _convert_strings_to_json
def _convert_json_to_strings(options):
    """
    Converts json input options to strings.

    Parameters
    ----------
    options : dictionary
        request options.
    """
    for key, value in options.items():
        if key == "grid_bounds":
            if not isinstance(value, str):
                options[key] = json.dumps(value)
        if key == "latlng_bounds":
            if not isinstance(value, str):
                options[key] = json.dumps(value)
        if key == "time_values":
            if not isinstance(value, str):
                options[key] = json.dumps(value)

    return options


def _get_ndarray_from_api(entry, options, time_values):
    """
    Constructs the query parameters from the entry and options provided.

    Parameters
    ----------
    entry : hydroframe.data_catalog.data_model_access.ModelTableRow
        variable to be downloaded.
    options : dictionary
        datast to which the variable belongs.
    options : array
        empty array to be populated with the time_values.

    Returns
    -------
    data : numpy array
        the requested data.
    """
    run_remote = not os.path.exists(HYDRODATA)

    if run_remote:
        options = _convert_json_to_strings(options)
        q_params = _construct_string_from_qparams(entry, options)
        url = f"https://hydro-dev.princeton.edu/api/gridded-data?{q_params}"

        try:
            response = requests.get(url, timeout=180)
            if response.status_code != 200:
                raise ValueError(
                    f"The url {url} returned error code {response.status_code}."
                )

        except requests.exceptions.Timeout as e:
            raise ValueError(f"The url {url} has timed out.") from e

        file_obj = io.BytesIO(response.content)
        netcdf_dataset = xr.open_dataset(file_obj)
        variable = entry["variable"]
        netcdf_variable = netcdf_dataset[f"{variable}"]
        data = netcdf_variable.values

        # Add time values if the data is not static
        if options.get("start_time") is not None:
            time_values_new = netcdf_dataset["time"]

            if time_values_new is not None and time_values is not None:
                time_values_np = time_values_new.values
                time_values_np_strings = np.datetime_as_string(time_values_np, unit="D")
                time_values.extend(time_values_np_strings)

        return data

    return None


def _adjust_dimensions(data: np.ndarray, entry: ModelTableRow) -> np.ndarray:
    """
    Reshape the dimensions of the data array to match the conventions for the entry period and expected variable.
    Args:
        data:       An numpy array to be returned by get_ndarray.
        entry:      A data catalog entry that contains period and variable attributes.
    Returns:
        A reshaped nd array with the same data, but possible different number of dimensions.

    Reshape dimensions to:
        * [hour, y, x]                    period is hourly without z dimension
        * [day, y, x]                     period is daily without z dimension
        * [month, y, x]                   period is monthly without z dimension
        * [y, x]                          period is static or blank without z dimension

        * [hour, z, y, x]                 period is hourly with z dimension
        * [day, z, y, x]                  period is daily with z dimension
        * [month, z, y, x]                period is monthly with z dimension
        * [z, y, x]                       period is static or blank with z dimension
    If the dataset has ensembles then there is an ensemble dimension at the beginning.
    """
    period = entry["period"]
    variable = entry["variable"]
    dataset = entry["dataset"]
    if entry["file_type"] == "vegm":
        # Do not adjust vegm files
        return data
    period = period if period in ["hourly", "daily", "monthly", "weekly"] else "static"
    variable_row = get_table_row("variable", id=variable)
    dataset_row = get_table_row("dataset", id=dataset)
    has_z = False
    has_ensemble = False
    if variable_row is not None:
        has_z = (
            variable_row["has_z"] is not None
            and variable_row["has_z"].lower() == "true"
        )
    if dataset_row is not None:
        has_ensemble = (
            dataset_row["has_ensemble"] is not None
            and dataset_row["has_ensemble"].lower() == "true"
        )
    existing_shape = data.shape
    new_shape = existing_shape
    existing_dim_size = len(existing_shape)
    result_dim_size = existing_dim_size
    if not has_z and not has_ensemble:
        if period in ("daily", "monthly", "weekly", "hourly"):
            result_dim_size = 3
        else:
            result_dim_size = 2
    elif has_z and has_ensemble:
        if period in ("daily", "monthly", "weekly", "hourly"):
            result_dim_size = 5
        else:
            result_dim_size = 4
    elif has_z or has_ensemble:
        if period in ("daily", "monthly", "weekly", "hourly"):
            result_dim_size = 4
        else:
            result_dim_size = 3

    # Adjust data shape
    if result_dim_size > existing_dim_size:
        for _ in range(0, result_dim_size - existing_dim_size):
            new_shape = (1,) + new_shape
        data = np.resize(data, new_shape)
    elif result_dim_size < existing_dim_size:
        if not has_z:
            if existing_dim_size == 3 and period == "static":
                data = data[0, :, :]
            elif existing_dim_size == 4 and period == "static":
                data = data[0, 0, :, :]
            elif (
                existing_dim_size == 4
                and period in ["monthly", "daily", "weekly"]
                and has_z
            ):
                data = data[:, 0, :, :]
            elif (
                existing_dim_size == 4
                and period in ["monthly", "daily", "weekly"]
                and not has_z
            ):
                data = data[0, :, :, :]
            elif existing_dim_size == 4 and period == "hourly":
                data = np.reshape(
                    data,
                    (
                        existing_shape[0] * existing_shape[1],
                        existing_shape[2],
                        existing_shape[3],
                    ),
                )
            elif existing_dim_size == 5 and period == "hourly":
                data = data[0, :, 0, :, :]
        elif has_z:
            if existing_dim_size == 4 and period == "static":
                data = data[0, :, :, :]

    return data


def _read_and_filter_pfb_files(
    entry: ModelTableRow,
    options: dict,
    time_values: List[datetime.datetime],
) -> np.ndarray:
    """
    Read the PFB files in the file paths of the entry filter and filter the data.

    Args:
        entry:          A modelTableRow containing the data catalog entry.
        options:        The options passed to get_ndarray as a dict.
        time_values:    An empty array that will be populated with the values from the data time dimension.
    Returns:
        An numpy ndarray of the filtered contents of the pfb files.
    If time_values is not None the array will be populated with the time dimension of the data
    only populated the time values after filtering the data.
    """

    start_time_value = _parse_time(options.get("start_time"))
    end_time_value = _parse_time(options.get("end_time"))
    paths = get_file_paths(entry, options)
    for path in paths:
        if not os.path.exists(path):
            raise ValueError(f"File {path} does not exist.")
    boundary_constraints = _get_pfb_boundary_constraints(
        entry["grid"],
        options.get("grid_bounds"),
        options.get("latlng_bounds"),
        options.get("x"),
        options.get("y"),
        options.get("z"),
    )
    boundary_constraints = _add_pfb_time_constraint(
        boundary_constraints, entry, start_time_value, end_time_value
    )
    data = read_pfb_sequence(paths, boundary_constraints)
    _collect_pfb_date_dimensions(time_values, data, start_time_value)

    return data


def _read_and_filter_c_pfb_files(
    entry: ModelTableRow,
    options: dict,
    time_values: List[datetime.datetime],
) -> np.ndarray:
    """
    Read the c.PFB files in the file paths of the entry filter and filter the data.

    Args:
        entry:          A modelTableRow containing the data catalog entry.
        options:        The options passed to get_ndarray as a dict.
        time_values:    An empty array that will be populated with the values from the data time dimension.
    Returns:
        An numpy ndarray of the filtered contents of the pfb files.
    If time_values is not None the array will be populated with the time dimension of the data
    only populated the time values after filtering the data.
    """

    start_time_value = _parse_time(options.get("start_time"))
    end_time_value = _parse_time(options.get("end_time"))
    paths = get_file_paths(entry, options)
    boundary_constraints = _get_pfb_boundary_constraints(
        entry["grid"],
        options.get("grid_bounds"),
        options.get("latlng_bounds"),
        options.get("x"),
        options.get("y"),
        options.get("z"),
    )
    if boundary_constraints is None:
        # No boundary constraint specified in input arguments
        # So default to full grid so we can add the z constraint for the C.pfb data filter
        grid = entry["grid"]
        shape = get_table_rows("grid", id=grid)[0]["shape"]
        boundary_constraints = {
            "x": {"start": 0, "stop": int(shape[2])},
            "y": {"start": 0, "stop": int(shape[1])},
            "z": {"start": 0, "stop": 0},
        }
    boundary_constraints = _add_pfb_time_constraint(
        boundary_constraints, entry, start_time_value, end_time_value
    )
    dataset_var = entry["dataset_var"]
    z = C_PFB_MAP.get(dataset_var)
    if z is not None:
        boundary_constraints["z"] = {"start": int(z), "stop": int(z)}
    else:
        entry_id = entry.get("id")
        raise ValueError(f"Unknown dataset_var for C.pfb entry {entry_id}.")
    data = read_pfb_sequence(paths, boundary_constraints)
    _collect_pfb_date_dimensions(time_values, data, start_time_value)
    return data


def _read_and_filter_pfmetadata_files(
    entry: ModelTableRow,
    options: dict,
    time_values: List[datetime.datetime],
) -> np.ndarray:
    """
    Read the pfmetadata files in the file paths of the entry filter and filter the data.

    Args:
        entry:          A modelTableRow containing the data catalog entry.
        options:        The options passed to get_ndarray as a dict.
        time_values:    An empty array that will be populated with the values from the data time dimension.
    Returns:
        An numpy ndarray of the filtered contents of the pfb files.
    If time_values is not None the array will be populated with the time dimension of the data
    only populated the time values after filtering the data.
    """

    start_time_value = _parse_time(options.get("start_time"))
    paths = get_file_paths(entry, options)

    dataset_var = entry["dataset_var"]
    ds = xr.open_dataset(paths[0])
    da = ds[dataset_var]
    da = _slice_da_bounds(
        da, entry["grid"], options.get("grid_bounds"), options.get("latlng_bounds")
    )
    data = da
    _collect_pfb_date_dimensions(time_values, data, start_time_value)
    return data


def _read_and_filter_vegm_files(
    entry: ModelTableRow,
    options: dict,
) -> np.ndarray:
    """
    Read the vegm files in the file paths of the entry filter and filter the data.

    Args:
        entry:          A modelTableRow containing the data catalog entry.
        options:        The options passed to get_ndarray as a dict.
    Returns:
        An numpy ndarray of the filtered contents of the pfb vegm files.
    """
    paths = get_file_paths(entry, options)
    file_path = paths[0]
    data = read_clm(file_path, type="vegm")
    grid_bounds = options.get("grid_bounds")
    if grid_bounds is not None:
        imin, jmin, imax, jmax = grid_bounds
        data = data[jmin:jmax, imin:imax, :]
    # Return dimensions in the order z, y, x where z is the vegm type
    data = np.transpose(data, (2, 0, 1))
    return data


def _read_and_filter_netcdf_files(
    entry: ModelTableRow,
    options: dict,
    time_values: List[datetime.datetime],
) -> np.ndarray:
    """
    Read the PFB files in the file paths of the entry filter and filter the data.

    Args:
        entry:          A modelTableRow containing the data catalog entry.
        options:        The options passed to get_ndarray as a dict.
        time_values:    An empty array that will be populated with the values from the data time dimension.
    Returns:
        An numpy ndarray of the filtered contents of the pfb files.
    If time_values is not None the array will be populated with the time dimension of the data
    only populated the time values after filtering the data.
    """

    paths = get_file_paths(entry, options)
    if len(paths) == 0:
        raise ValueError(f"No file path found for {entry['id']}")
    file_path = paths[0]
    if file_path.endswith("*"):
        # Data path contins a wild card so use that to find the filename
        file_path = _match_filename_wild_card(file_path)

    if not os.path.exists(file_path):
        raise ValueError(f"File '{file_path} does not exist.")
    # Get the data array of the variable from the entry and slice the data array by filter options
    variable = entry["dataset_var"]
    data_ds = xr.open_dataset(file_path)
    data_da = data_ds[variable]
    da_indexers = _create_da_indexer(options, entry, data_ds, data_da, file_path)
    data_da = data_da.isel(da_indexers)
    data = data_da.to_numpy()
    if time_values is not None:
        if "date" in list(data_ds.coords.keys()):
            for t in data_ds["date"].values:
                time_values.append(str(t))
        elif "time" in list(data_ds.coords.keys()):
            for t in data_da["time"].values:
                time_values.append(str(t))
        elif "datetime" in list(data_ds.coords.keys()):
            for t in data_da["datetime"].values:
                time_values.append(str(t))
    if data_da.dims == ("x", "y"):
        # The NetCDF file dimensions are in the order x, y
        # But get_ndarray must return the order y, x
        data = np.transpose(data)
    if len(data_da.dims) == 3 and data_da.dims[-1] == "y" and data_da.dims[-2] == "x":
        data = np.transpose(data, (0, 2, 1))
    return data


def _read_and_filter_tiff_files(
    entry: ModelTableRow,
    options: dict,
) -> np.ndarray:
    """
    Read the tiff files in the file paths of the entry filter and filter the data.

    Args:
        entry:          A modelTableRow containing the data catalog entry.
        options:        The options passed to get_ndarray as a dict.
        start_time_value: The start time option of the option converted to a datetime or None.
    Returns:
        An numpy ndarray of the filtered contents of the pfb files.
    """
    paths = get_file_paths(entry, options)
    file_path = paths[0]
    variable = entry["dataset_var"]
    data_ds = xr.open_dataset(file_path)
    data_da = data_ds[variable]
    da_indexers = _create_da_indexer(options, entry, data_ds, data_da, file_path)
    data_da = data_da.isel(da_indexers)
    data = data_da.to_numpy()
    return data


def get_yaml_data_catalog() -> dict:
    """
    Get the parsed yaml data catalog.

    Returns:
        The hydrodata_catalog.yaml file loaded into a dict.

    The parsed yaml file representation of the data catalog is useful for use
    by non-python clients such as javascript that want access to the data model information.
    """

    try:
        current_file_path = os.path.dirname(__file__)
        config_file_path = f"{current_file_path}/hydrodata_catalog.yaml"
        with open(config_file_path, "r") as stream:
            contents = stream.read()
            result = yaml.safe_load(contents)
    except:
        logging.exception("Unable to load hydrodata_catalog.yaml")
        result = None
    if result is None:
        result = {}
    return result


def grid_to_latlng(grid: str, *args) -> List[float]:
    """
    Convert grid x,y coordinates to lat,lng.

    Args:
        grid:       The name of a grid dimension from the data catalog grid table (e.g. conus1 or conus2).
        args:       A list of numbers of (x,y) values that are coordinates in the grid (may be int or float).
    Returns:
        An array of lat,lng points converted from each of the (x,y) grid coordinates in args.

    Note, this may be used to convert a single point or a bounds of 2 points or a large array of points.

    This conversion is fast. It is about 100K+ points/second.

    For example,
        (lat, lng) = grid_to_latlng("conus1", 10, 10)

        latlng_bounds = grid_to_latlng("conus1", *[0, 0, 20, 20])

        (lat, lng) = grid_to_latlng("conus1", 10.5, 10.5)
    """
    result = []
    grid_row = get_table_row("grid", id=grid.lower())
    if grid_row is None:
        raise ValueError(f"No such grid {grid} available.")
    grid_resolution = float(grid_row["resolution_meters"])
    if len(args) == 0:
        raise ValueError("At least two x, y values must be provided.")
    if len(args) % 2 == 1:
        raise ValueError(
            "Number of args must be even number. E.g. list of (x, y) coordinates."
        )

    for index in range(0, len(args), 2):
        x = int(args[index] * grid_resolution)
        y = int(args[index + 1] * grid_resolution)
        lat, lng = from_conic(x, y, grid)
        result.append(lat)
        result.append(lng)
    return result


def latlng_to_grid(grid: str, *args) -> List[float]:
    """
    Convert grid lat,lng coordinates to x,y.

    Args:
        grid:       The name of a grid dimension from the data catalog grid table (e.g. conus1 or conus2).
        args:       A list of floating pairs if (lat,lng) values.
    Returns:
        An array of x,y integer points converted from each of the (lat,lng) grid coordinates in args.

    Note, this may be used to convert a single point or a bounds of 2 points or a large array of points.

    This conversion is fast. It is about 100K+ points/second.

    For example,
        (x, y) = grid_to_latlng("conus1", 31.759219, -115.902573)

        latlng_bounds = grid_to_latlng("conus1", *[31.651836, -115.982367, 31.759219, -115.902573])
    """
    result = []
    grid_row = get_table_row("grid", id=grid.lower())
    if grid_row is None:
        raise ValueError(f"No such grid {grid} available.")
    grid_resolution = float(grid_row["resolution_meters"])
    if len(args) == 0:
        raise ValueError("At least two x, y values must be provided.")
    if len(args) % 2 == 1:
        raise ValueError(
            "Number of args must be even number. E.g. list of (lat,lng) coordinates."
        )

    for index in range(0, len(args), 2):
        lat = args[index]
        lng = args[index + 1]
        (x, y) = to_conic(lat, lng, grid)
        result.append(round((x) / grid_resolution))
        result.append(round((y) / grid_resolution))
    return result


def get_huc_from_latlng(grid: str, level: int, lat: float, lng: float) -> str:
    """
        Get a HUC id at a lat/lng point for a given grid and level.

        Args:
            grid:   grid name (e.g. conus1 or conus2)
            level:  HUC level (length of HUC id to be returned)\
            lat:    lattitude of point
            lng:    longitude of point
        Returns:
            The HUC id string containing the lat/lng point or None.
    """
    huc_id = None
    tiff_ds = __get_geotiff(grid, level)
    [x, y] = latlng_to_grid(grid, lat, lng)
    data = np.flip(tiff_ds[0].to_numpy(), 0)
    if 0 <= x <= data.shape[1] and 0 <= y <= data.shape[0]:
        huc_id = np.flip(tiff_ds[0].to_numpy(), 0)[y][x].item()
        if isinstance(huc_id, float):
            huc_id = str(huc_id).replace(".0", "")
    return huc_id


def get_huc_from_xy(grid: str, level: int, x: int, y: int) -> str:
    """
        Get a HUC id at an xy point for a given grid and level.

        Args:
            grid:   grid name (e.g. conus1 or conus2)
            level:  HUC level (length of HUC id to be returned)\
            x:      x coordinate in the grid
            y:      y coordinate in the grid
        Returns:
            The HUC id string containing the lat/lng point or None.
    """
    tiff_ds = __get_geotiff(grid, level)
    data = np.flip(tiff_ds[0].to_numpy(), 0)
    huc_id = None
    if 0 <= x <= data.shape[1] and 0 <= y <= data.shape[0]:
        huc_id = data[y][x].item()
        if isinstance(huc_id, float):
            huc_id = str(huc_id).replace(".0", "")
    return huc_id


def get_huc_bbox(grid: str, huc_id_list: List[str]) -> List[int]:
    """
    Get the grid bounding box containing all the HUC ids.

    Args:
        grid:           A grid id from the data catalog (e.g. conus1 or conus2)
        huc_id_list:    A list of HUC id strings of HUCs in the grid.
    Returns:
        A bounding box in grid coordinates as a list of int (i_min, j_min, i_max, j_max)
    Raises:
        ValueError if all the HUC id are not at the same level (same length).
        ValueError if grid is not valid.
    """
    # Make sure all HUC ids in the list are the same length
    level = None
    for huc_id in huc_id_list:
        if level is None:
            level = len(huc_id)
        elif len(huc_id) != level:
            raise ValueError("All HUC ids in the list must be the same length.")

    # Open the TIFF file of the grid and level
    tiff_ds = __get_geotiff(grid, level)

    result_imin = 1000000
    result_imax = 0
    result_jmin = 1000000
    result_jmax = 0
    for huc_id in huc_id_list:
        tiff_value = int(huc_id) if grid == "conus1" else float(huc_id)
        sel_huc = (tiff_ds == tiff_value).squeeze()

        # First find where along the y direction has "valid" cells
        y_mask = (sel_huc.sum(dim="x") > 0).astype(int)

        # Then, taking a diff along that dimension let's us see where the boundaries of that mask ar
        diffed_y_mask = y_mask.diff(dim="y")

        # Taking the argmin and argmax get's us the locations of the boundaries
        arr_jmax = (
            np.argmin(diffed_y_mask.values) + 1
        )  # this one is because you want to include this right bound in your slice
        arr_jmin = (
            np.argmax(diffed_y_mask.values) + 1
        )  # because of the point you actually want to indicate from the diff function

        jmin = tiff_ds.shape[1] - arr_jmax
        jmax = tiff_ds.shape[1] - arr_jmin

        # Do the exact same thing for the x dimension
        diffed_x_mask = (sel_huc.sum(dim="y") > 0).astype(int).diff(dim="x")
        imax = np.argmin(diffed_x_mask.values) + 1
        imin = np.argmax(diffed_x_mask.values) + 1

        # Extend the result values to combine multiple HUC ids
        result_imin = imin if imin < result_imin else result_imin
        result_imax = imax if imax > result_imax else result_imax
        result_jmin = jmin if jmin < result_jmin else result_jmin
        result_jmax = jmax if jmax > result_jmax else result_jmax

    return (result_imin, result_jmin, result_imax, result_jmax)


def __get_geotiff(grid: str, level: int) -> xr.Dataset:
    """
    Get an xarray dataset of the geotiff file for the grid at the level.

    Args:
        grid:   grid name (e.g. conus1 or conus2)
        level:  HUC level (length of HUC id to be returned)\
    Returns:
        An xarray dataset with the contents of the geotiff file for the grid and level.
    """

    entry = get_catalog_entry(
        dataset="huc_mapping", file_type="tiff", variable="huc_map", grid=grid
    )
    if entry is None:
        raise ValueError("No mapping entry for grid '{grid}' and level '{level}'")
    paths = get_file_paths(entry, level=level)
    file_path = paths[0]
    variable = entry["dataset_var"]

    if not os.path.exists(file_path):
        raise ValueError("File '{file_path}' does not exist.")

    # Open TIFF file
    tiff_ds = xr.open_dataset(file_path).drop_vars(("x", "y"))[variable]
    return tiff_ds


def _collect_pfb_date_dimensions(
    time_values: List[str], data: np.ndarray, start_time_value: datetime.datetime
):
    """
    Create the date strings of the time dimension of the data and add them to the
    time_values array.

    Args:
        time_values:     An empty array to be filled with date strings.
        data:               The ndarray returned by get_ndarray method
        start_time_value:   The start_time of the data filter.
    Assumes that the time dimension is the first dimension of the data array.
    Assumes that the period is daily (for now).
    """
    if time_values is not None and start_time_value and data.shape[0] > 0:
        dt = start_time_value
        for _ in range(0, data.shape[0]):
            time_values.append(dt.strftime("%Y-%m-%d"))
            dt = dt + datetime.timedelta(days=1)


def _match_filename_wild_card(data_path: str) -> str:
    """The data_path ends with a * wild card. Use this to find a file matching that file prefix."""

    data_path = data_path.replace("*", "")
    pos = data_path.rfind("/")
    directory = data_path[0:pos]
    file_name = data_path[pos + 1 :]
    for f in os.listdir(directory):
        if f.startswith(file_name):
            data_path = f"{directory}/{f}"
            break
    return data_path


def _slice_da_bounds(
    da: xr.DataArray,
    grid: str = None,
    grid_bounds: List[float] = None,
    latlng_bounds: List[float] = None,
) -> xr.DataArray:
    if grid_bounds and latlng_bounds:
        raise ValueError("Cannot specify both grid_bounds and latlng_bounds")
    if latlng_bounds:
        grid_bounds = latlng_to_grid(grid, *latlng_bounds)

    if grid_bounds:
        result = da[:, grid_bounds[1] : grid_bounds[3], grid_bounds[0] : grid_bounds[2]]
    else:
        result = da
    return result


def _get_pfb_boundary_constraints(
    grid: str = None,
    grid_bounds: List[float] = None,
    latlng_bounds: List[float] = None,
    x: int = None,
    y: int = None,
    z: int = None,
) -> dict:
    """
    Get a PFB boundary constraint given either a grid_bounds or latlng_bounds

    Args:
        grid:           The name of a grid from the data catalog.
        grid_bounds:    An array of [left, bottom, right, top] in the xy coordinates of the grid.
        latlng_bounds:  An array of [left, bottom, right, top] in the latlng coordinates.
        x:              x coordinate in the xy coordinates of the grid
        y:              y coordinate in the xy coordinates of the grid
        z:              z coordinate in the xy coordinates of the grid
    Returns:
        A PFB boundary constraint dict with attributes: x, y, z or None if no bounds is specified.
    Raises:
        ValueError if both grid_bounds and latlng_bounds are specified
    If x,y,z are specified then the boundary is filter to include only the point at that location.
    If x,y are specified, but not z then z is filtered to include only point 0.
    """

    if grid_bounds and latlng_bounds:
        raise ValueError("Cannot specify both grid_bounds and latlng_bounds")
    if latlng_bounds:
        grid_bounds = latlng_to_grid(grid, *latlng_bounds)

    result = None
    if x is not None:
        if y is None:
            raise ValueError("If x point is specified then y must be specified")
        z = int(z) if z is not None else 0
        result = {
            "x": {"start": int(x), "stop": int(x)},
            "y": {"start": int(y), "stop": int(y)},
            "z": {"start": z, "stop": z},
        }
    elif grid_bounds:
        result = {
            "x": {"start": int(grid_bounds[0]), "stop": int(grid_bounds[2])},
            "y": {"start": int(grid_bounds[1]), "stop": int(grid_bounds[3])},
            "z": {"start": 0, "stop": 0},
        }

    return result


def _add_pfb_time_constraint(
    boundary_constraints: dict,
    entry: ModelTableRow,
    start_time_value: datetime.datetime,
    end_time_value: datetime.datetime,
) -> dict:
    """
    Add a PFB constraint to the z dimension when the PFB z dimension is being used as a time dimension.

    Args:
        boundary_constraints:   A previously constructed PFT boundary constraint object,
        entry:                  A data catalog entry
        start_time_value        Start time of the filter
        end_time_value:         End time of the filter
    Returns:
        The updated boundary_constraints
    """
    period = entry["period"]
    variable = entry["variable"]
    variable_row = get_table_row("variable", id=variable)
    if (
        not (variable_row is not None and variable_row["has_z"].lower() == "true")
        and start_time_value is not None
        and period in ["daily", "hourly", "monthly", "weekly"]
    ):
        if boundary_constraints is None:
            grid = entry["grid"]
            grid_row = get_table_row("grid", id=grid.lower())
            if grid_row is None:
                raise ValueError(f"No such grid {grid} available.")
            grid_shape = grid_row["shape"]
            boundary_constraints = {
                "x": {"start": int(0), "stop": int(grid_shape[2])},
                "y": {"start": int(0), "stop": int(grid_shape[1])},
                "z": {"start": 0, "stop": 0},
            }
        # The variable does not have a z access so the z dimensions contains day or hour dimension
        if period == "daily":
            # We are going to assume the file contains all days in a water year
            (_, wy_start) = _get_water_year(start_time_value)
            wy_daynum = (start_time_value - wy_start).days
            if end_time_value is not None:
                end_wy_daynum = wy_daynum + (end_time_value - start_time_value).days
            else:
                end_wy_daynum = wy_daynum + 1
            boundary_constraints["z"] = {"start": wy_daynum, "stop": end_wy_daynum}
        elif period == "monthly":
            # We are going to assume the file contains all months in a water year
            (_, wy_start) = _get_water_year(start_time_value)
            month_start = (
                rrule.rrule(
                    rrule.MONTHLY, dtstart=wy_start, until=start_time_value
                ).count()
                - 1
            )
            if end_time_value is not None:
                month_end = (
                    rrule.rrule(
                        rrule.MONTHLY, dtstart=wy_start, until=end_time_value
                    ).count()
                    - 1
                )
            else:
                month_end = month_start + 1
            boundary_constraints["z"] = {"start": month_start, "stop": month_end}
        elif period == "weekly":
            # We are going to assume the file contains all months in a water year
            (_, wy_start) = _get_water_year(start_time_value)
            month_start = (
                rrule.rrule(
                    rrule.WEEKLY, dtstart=wy_start, until=start_time_value
                ).count()
                - 1
            )
            if end_time_value is not None:
                month_end = (
                    rrule.rrule(
                        rrule.WEEKLY, dtstart=wy_start, until=end_time_value
                    ).count()
                    - 1
                )
            else:
                month_end = month_start + 1
            boundary_constraints["z"] = {"start": month_start, "stop": month_end}
        elif period == "hourly":
            start_hour = start_time_value.hour
            if end_time_value is not None:
                days = (end_time_value - start_time_value).days
                hours = int((end_time_value - start_time_value).seconds / 3600)
                if days > 0:
                    # PFB Files never have more than 24 hours in one file
                    hours = hours + 24 * 1
                end_hour = hours
            else:
                end_hour = start_hour + 1
            boundary_constraints["z"] = {
                "start": int(start_hour),
                "stop": int(end_hour),
            }
    return boundary_constraints


def _is_row_match_options(row: ModelTableRow, options: dict) -> bool:
    """
    Return True if the row matches the constraints in the options.

    Args:
        row:        Row from the ModelTable row with the meta data.
        options:    Dict of filter option values.
    Returns:
        True if all of the filter options match metadata in the row.
    """

    result = True
    for option in options.keys():
        option_value = options.get(option)
        row_value = row[option]
        if row_value is not None and option_value and row_value != option_value:
            result = False
            break
    return result


def _substitute_datapath(
    path: str,
    entry: ModelTableRow,
    options: dict,
    time_value: datetime.datetime,
) -> str:
    """
    Replace any substitution keys in the datapath with values from metadata, options, or time_value.

    Args:
        path:           The path template of the data_catalog_entry.
        entry:          A ModelTableRow of the data_catalog_entry the defines the paths
        options:        A dict with the request options.
        time_value:     A time value of the request.
    Returns:
        The value of datapath after substituting in values.
    The time_value is converted into the various possible substitution values that may be used in a data path.

    Substitute the scenario_id or the domain_path from the options into the file path if they exists.
    This option is provided to support hydrogen specific file path substitutions.
    """
    dataset_var = entry["dataset_var"]
    wy = ""
    wy_plus1 = ""
    wy_minus1 = ""
    wy_daynum = 0
    wy_mdy = ""
    mdy = ""
    ymd = ""
    wy_hour = 0
    wy_start_24hr = 0
    month_num = 0
    wy_end_24hr = 0
    mmddyyyy = ""
    scenario_id = options.get("scenario_id")
    domain_path = options.get("domain_path")
    scenario_from_date = options.get("scenario_from_date")
    scenario_to_date = options.get("scenario_to_date")
    run_number = options.get("run_number")
    site_id = options.get("site_id")
    level = options.get("level")
    if time_value:
        (wy, wy_start) = _get_water_year(time_value)
        wy_plus1 = str(int(wy) + 1)
        wy_minus1 = str(int(wy) - 1)
        wy_daynum = (time_value - wy_start).days + 1
        wy_mdy = time_value.strftime("%m%d%Y")
        mdy = time_value.strftime("%m%d%Y")
        ymd = time_value.strftime("%Y%m%d")
        month_num = time_value.month
        wy_hour = int((time_value - wy_start).total_seconds() / 3600) + 1
        wy_start_24hr = (time_value - wy_start).days * 24 + 1
        wy_end_24hr = (time_value - wy_start).days * 24 + 24
        mmddyyyy = datetime.datetime.strftime(time_value, "%m%d%Y")
    datapath = path.format(
        dataset_var=dataset_var,
        wy=wy,
        wy_daynum=wy_daynum,
        wy_mdy=wy_mdy,
        ymd=ymd,
        mdy=mdy,
        wy_start_24hr=wy_start_24hr,
        wy_end_24hr=wy_end_24hr,
        wy_hour=wy_hour,
        wy_plus1=wy_plus1,
        wy_minus1=wy_minus1,
        month=month_num,
        mmddyyyy=mmddyyyy,
        site_id=site_id,
        scenario_id=scenario_id,
        domain_path=domain_path,
        scenario_from_date=scenario_from_date,
        scenario_to_date=scenario_to_date,
        run_number=run_number,
        level=level,
    )
    return datapath


def _get_water_year(dt: datetime.datetime):
    """Get the water year and water year start date containing the date dt.

    Args:
        dt:     a date
    Returns:
        A tuple (water-year, water-year-date)
    The water-year is a string with the water year.
    The water-year-date is a datetime of the start of the water year.
    """

    if dt.month >= 10:
        wy = f"{dt.year+1}"
        wy_start = datetime.datetime.strptime(f"{dt.year}-10-01", "%Y-%m-%d")
    else:
        wy = f"{dt.year}"
        wy_start = datetime.datetime.strptime(f"{dt.year-1}-10-01", "%Y-%m-%d")
    return (wy, wy_start)


def _parse_time(value: str) -> datetime.datetime:
    """Parse a value as a date time.

    Args:
        input:  A string or a datetime object.
    Returns:
        A datetime object
    """

    result = None
    if isinstance(value, datetime.datetime):
        result = value
    elif isinstance(value, str):
        try:
            result = datetime.datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        except:
            try:
                result = datetime.datetime.strptime(value, "%Y-%m-%d")
            except:
                try:
                    result = datetime.datetime.strptime(value, "%m-%d-%Y")
                except:
                    try:
                        result = datetime.datetime.strptime(value, "%m/%d/%Y, %H:%M:%S")
                    except:
                        try:
                            result = datetime.datetime.strptime(
                                value, "%m/%d/%Y %H:%M:%S"
                            )
                        except:
                            try:
                                result = datetime.datetime.strptime(
                                    value, "%Y-%m-%dT%H:%M:%S.000000000"
                                )
                            except:
                                result = None
    return result


def _create_da_indexer(options: dict, entry, data_ds, data_da, file_path: str) -> dict:
    """Create an xarray data array indexer object for common filters.

    Args:
        options:    Data options passed to data access request.
        entry:      A data catalog entry of the data to be indexed
        data_ds:    An xarray dataset of the data.
        data_ds:    An xarray dataset of the variable in the dataset.
        file_path:  The file path of the data
    Returns:
        A dict in the format for an xarray data array indexer to be passed to isel().
    """
    da_indexers = {}
    start_time_value = _parse_time(options.get("start_time"))
    end_time_value = _parse_time(options.get("end_time"))
    grid = entry["grid"]
    period = entry["period"]
    grid_bounds = options.get("grid_bounds")
    latlng_bounds = options.get("latlng_bounds")
    x = options.get("x")
    y = options.get("y")
    if "member" in data_da.dims:
        # Slice the requested ensemble member
        run_number = options.get("run_number")
        run_index = int(run_number) - 1 if run_number is not None else 0
        da_indexers["member"] = run_index
    if "z" in data_da.dims:
        z = options.get("z")
        z_index = data_da.dims.index("z")
        data_z = data_da.shape[z_index]
        z = int(z) if z is not None else data_z - 1
        da_indexers["z"] = z
    (time_dimension_name, time_coord_name) = _get_time_dimension_name(data_ds, data_da)
    if time_dimension_name is not None:
        # If start_time is specified in options then slice the time dimension to return only that time
        if start_time_value is not None:
            # Get the first value of the time dimension from the netcdf file
            dimension_start_time = None
            if time_coord_name is not None:
                dimension_start_time = _parse_time(
                    str(data_ds[time_coord_name][0].to_numpy())
                )
            else:
                dimension_start_time = _parse_time(
                    str(data_da[time_dimension_name][0].to_numpy())
                )
            if dimension_start_time is None:
                # There is no time dimension values in NetCDF file so assume it relative to water year
                (_, wy_start) = _get_water_year(start_time_value)
                dimension_start_time = wy_start
            # Put the time_index of the start_time option into the da_indexers to slice the data by time
            if period == "daily":
                time_index = (start_time_value - dimension_start_time).days
                dimension_size = int(data_da[time_dimension_name].shape[0])
                if time_index < 0 or time_index >= dimension_size:
                    raise ValueError(
                        f"The start_date '{options.get('start_time')}' implies time dimension {time_index} that is outside the time dimension range {dimension_size} of the netcdf file '{file_path}'."
                    )
            elif period == "hourly":
                time_index = int(
                    (start_time_value - dimension_start_time).seconds / 3600
                )
            elif period == "monthly":
                time_index = (
                    rrule.rrule(
                        rrule.MONTHLY,
                        dtstart=dimension_start_time,
                        until=start_time_value,
                    ).count()
                    - 1
                )
            elif period == "weekly":
                time_index = (
                    rrule.rrule(
                        rrule.WEEKLY,
                        dtstart=dimension_start_time,
                        until=start_time_value,
                    ).count()
                    - 1
                )

            if end_time_value is None:
                # Slice time dimension to a single point in time because only start_time specified
                da_indexers[time_dimension_name] = time_index
            else:
                # Slice time dimension to a range of times
                if period == "daily":
                    end_time_index = (end_time_value - dimension_start_time).days
                elif period == "hourly":
                    end_time_index = int(
                        (end_time_value - dimension_start_time).seconds / 3600
                    )
                elif period == "monthly":
                    end_time_index = (
                        rrule.rrule(
                            rrule.MONTHLY,
                            dtstart=dimension_start_time,
                            until=end_time_value,
                        ).count()
                        - 1
                    )
                elif period == "weekly":
                    end_time_index = (
                        rrule.rrule(
                            rrule.WEEKLY,
                            dtstart=dimension_start_time,
                            until=end_time_value,
                        ).count()
                        - 1
                    )

                da_indexers[time_dimension_name] = slice(time_index, end_time_index)
    if grid_bounds is not None and latlng_bounds is not None:
        raise ValueError("Cannot specify both grid_bounds and latlng_bounds")
    if latlng_bounds:
        grid_bounds = latlng_to_grid(grid, *latlng_bounds)
    if x is not None:
        if y is None:
            raise ValueError("If x is specified then y must be specified.")
        x = int(x)
        y = int(y)
        da_indexers["x"] = slice(x, x + 1)
        da_indexers["y"] = slice(y, y + 1)
    elif grid_bounds:
        da_indexers["x"] = slice(grid_bounds[0], grid_bounds[2])
        da_indexers["y"] = slice(grid_bounds[1], grid_bounds[3])
    return da_indexers


def _get_time_dimension_name(ds: xr.Dataset, da: xr.DataArray):
    """
    Get the name of the time dimension in the xarray data set.

    Args:
        ds:     An xarray dataset.
        da:     An xrray data array.
    Return:
        A tuple (dimension_name:str, coord_name:str) with the name of the time dimension and time coord or None if no time dimension
    """
    time_dimension_name = None
    time_coord_name = None
    for dim in da.dims:
        if dim in ["time", "date", "TimeStamp", "datetime"]:
            time_dimension_name = dim
            break
    for coord in ds.coords:
        if coord in ["time", "date", "TimeStamp", "datetime"]:
            time_coord_name = coord
            break
    for coord in ds.keys():
        if coord in ["time", "date", "TimeStamp"]:
            time_coord_name = coord
            break
    return (time_dimension_name, time_coord_name)
