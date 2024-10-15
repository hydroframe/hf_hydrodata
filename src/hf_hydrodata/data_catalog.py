"""
Functions to access data_catalog metadata.
"""

# pylint: disable=W0603,C0103,E0401,W0702,C0209,C0301,R0914,R0912,W1514,E0633,R0915,R0913,C0302,W0632,R1732,R1702,W0212

import os
from typing import List
import threading
from hf_hydrodata.data_model_access import (
    ModelTableRow,
    load_data_model
)

HYDRODATA = "/hydrodata"
JWT_TOKEN = None
USER_ROLES = None
THREAD_LOCK = threading.Lock()

HYDRODATA_URL = os.getenv("HYDRODATA_URL", "https://hydrogen.princeton.edu")


def get_citations(*args, **kwargs) -> str:
    """
    Get citation references for a dataset.

    Args:
        dataset:    The name of a dataset/
    Returns:
        A string containing citation references of the dataset.

    The citation references consist of a description of the dataset with relavent URL references to papers or websites.

    The dataset parameter can be passed as a named or un-named parameter or as a dict containing a dataset option.

    Example:

    .. code-block:: python

        import hf_hydrodata as hf

        citations = hf.get_citations("NLDAS2")
        print(citations)

        citations = hf.get_citations(dataset = "NLDAS2")
        print(citations)

        options = {"dataset": "NLDAS2", "temporal_resolution": "daily"}
        citations = hf.get_citations(options)
    """

    if len(args) > 0 and isinstance(args[0], dict):
        options = args[0]
        dataset = options.get("dataset")
    elif len(args) > 0 and isinstance(args[0], str):
        dataset = args[0]
    else:
        options = kwargs
        dataset = options.get("dataset")

    if not dataset:
        raise ValueError("Dataset is not specified.")

    # If the dataset is a point observation dataset return the citation from point observation module
    if dataset in [
        "usgs_nwis",
        "snotel",
        "scan",
        "ameriflux",
        "jasechko_2024",
        "fan_2013",
    ]:
        return _get_point_citations(dataset)

    entries = get_catalog_entries(dataset=dataset)
    if entries is None or len(entries) == 0:
        raise ValueError(f"No such dataset '{dataset}'")
    entry = entries[0]
    result = ""
    description = entry.get("description")
    paper_dois = entry.get("paper_dois")
    dataset_dois = entry.get("dataset_dois")
    result = result + f"{description}\n"
    found_reference = False
    if paper_dois:
        for doi in paper_dois.split(" "):
            if doi:
                doi = doi.replace(";", "")
                result = result + f"  Source: https://doi.org/{doi}\n"
                found_reference = True
    if dataset_dois:
        for doi in dataset_dois.split(" "):
            if doi:
                doi = doi.replace(";", "")
                result = result + f"  Source: {doi}\n"
                found_reference = True
    if not found_reference:
        result = result + "No paper references available.\n"
    return result


def register_api_pin(email: str, pin: str):
    """
    Register the email and pin that was created with the website in the users home directory.

    Args:
        email:      Email address used to create an API pin.
        pin:        The 4 digit pin registered to be able to use the API.

    This only needs to be execute once per machine to register the pin.
    You can signup for an account using https://hydrogen.princeton.edu/signup.
    You can create a pin using the URL https://hydrogen.princeton.edu/pin.

    Example:

    .. code-block:: python

        import hf_hydrodata as hf

        hf.register_api_pin("dummy@gmail.com", "1234")
    """

    pin_dir = os.path.expanduser("~/.hydrodata")
    os.makedirs(pin_dir, mode=0o700, exist_ok=True)
    pin_path = f"{pin_dir}/pin.json"
    with open(pin_path, "w+") as stream:
        template = '"email": "{email}", "pin":"{pin}"'
        line = template.format(email=email, pin=pin)
        stream.write("{")
        stream.write(line)
        stream.write("}")
    os.chmod(pin_path, 0o700)


def get_datasets(*args, **kwargs) -> List[str]:
    """
    Get available datasets.

    The parameters to the function can be specified either by passing a dict with the parameter values
    or by passing named parameters to the function.

    Args:
        dataset:        A dataset name (see Gridded Data documentation).
        variable:       A variable from a dataset.
        temporal_resolution:  The temporal_resolution (e.g. hourly, daily, weekly, monthly) of a dataset variable.
        grid:           A grid supported by a dataset (e.g. conus1 or conus2). Normally this is determined by the dataset.
        aggregation:    One of mean, max, min. Normally, only needed for temperature variables.
    Returns:
        A list of dataset names that contain a data catalog entry filtered by the parameters. If no options are provided returns all available datasets.

    Examples:
       .. code-block:: python

        import hf_hydrodata as hf

        datasets = hf.get_datasets()
        assert len(datasets) == 13
        assert datasets[0] == "CW3E"

        datasets = hf.get_datasets(variable = "air_temp")
        assert len(datasets) == 5
        assert datasets[0] == "CW3E"

        datasets = hf.get_datasets(grid = "conus2")
        assert len(datasets) == 5
        assert datasets[0] == "CW3E"

        options = {"variable": "air_temp", "grid": "conus1"}
        datasets = hf.get_datasets(options)
        assert len(datasets) == 3
        assert datasets[0] == "NLDAS2"

    """

    result = []
    entries = get_catalog_entries(*args, **kwargs)
    for entry in entries:
        dataset = entry.get("dataset")
        if dataset not in result:
            result.append(dataset)
    result.sort()
    return result


def get_variables(*args, **kwargs) -> List[str]:
    """
    Get available variables.

    The parameters to the function can be specified either by passing a dict with the parameter values
    or by passing named parameters to the function.

    Args:
        dataset:        A dataset name (see Gridded Data documentation).
        variable:       A variable from a dataset.
        temporal_resolution:         The temporal_resolution (e.g. hourly, daily, weekly, monthly) of a dataset variable.
        grid:           A grid supported by a dataset (e.g. conus1 or conus2). Normally this is determined by the dataset.
        aggregation:    One of mean, max, min. Normally, only needed for temperature variables.
    Returns:
        A list of variable names that contain a data catalog entry filtered by the parameters. If no options are provided returns all available variables.

    Examples:
       .. code-block:: python

        import hf_hydrodata as hf

        variables = hf.get_variables()
        assert len(variables) == 63
        assert variables[0] == "air_temp"

        variables = hf.get_variables(dataset = "CW3E")
        assert len(variables) == 8
        assert variables[0] == "air_temp"

        variables = hf.get_variables(grid = "conus2")
        assert len(variables) == 30
        assert variables[0] == "air_temp"

        options = {"dataset": "NLDAS2", "grid": "conus1"}
        variables = hf.get_variables(options)
        assert len(variables) == 8
        assert variables[0] == "air_temp"

    """

    result = []
    entries = get_catalog_entries(*args, **kwargs)
    for entry in entries:
        dataset = entry.get("variable")
        if dataset not in result:
            result.append(dataset)
    result.sort()
    return result


def get_catalog_entries(*args, **kwargs) -> List[ModelTableRow]:
    """
    Get data catalog entry rows selected by filter options.

    The parameters to the function can be specified either by passing a dict with the parameter values
    or by passing named parameters to the function.

    Args:
        dataset:        A dataset name (see Gridded Data documentation).
        variable:       A variable from a dataset.
        temporal_resolution:         The temporal_resolution (e.g. hourly, daily, weekly, monthly) of a dataset variable.
        grid:           A grid supported by a dataset (e.g. conus1 or conus2). Normally this is determined by the dataset.
        aggregation:    One of mean, max, min. Normally, only needed for temperature variables.
        start_time:     A time as either a datetime object or a string in the form YYYY-MM-DD. Start of the date range for data.
        end_time:       A time as either a datetime object or a string in the form YYYY-MM-DD. End of the date range for data.
        grid_bounds:    An array (or string representing an array) of points [left, bottom, right, top] in xy grid corridates in the grid of the data.
        latlng_bounds:  An array (or string representing an array) of points [left, bottom, right, top] in lat/lng coordinates mapped with the grid of the data.
        grid_point:     An array (or string representing an array) of points [x, y] in grid corridates of a point in the grid.
        latlng_point:   An array (or string representing an array) of points [lat, long] in lat/lng coordinates of a point in the grid.
        z:              A value of the z dimension to be used as a filter for this dismension when loading data.
        level:          A HUC level integer when reading HUC boundary files.
        site_id:        Used when reading data associated with an observation site.
        data_catalog_entry_id: Optional. The id of an entry in the data catalog to identify an entry.
    Returns:
        A list of ModelTableRow entries that match the filter options.

    A ModelTableRow contains the attributes of the hf_hydrodata model of a data catalog entry.
    The attributes can be accessed by indexing by the attribute name (e.g. entry["dataset"]).
    You can get the attribute names of an entry using column_names() (e.g. entry.column_names()).

    ModelTableRow metadata attributes:
        * dataset:          A dataset name (see Gridded Data documentation).
        * variable:         A variable from a dataset.
        * temporal_resolution:           The temporal_resolution (e.g. hourly, daily, weekly, monthly) of a dataset variable.
        * grid:             A grid supported by a dataset (e.g. conus1 or conus2). Normally this is determined by the dataset.
        * aggregation:      One of mean, max, min. Normally, only needed for temperature variables.
        * entry_start_date: Earliest available date of data.
        * entry_end_date:   Latest available date of data.
        * units:            Units of the data.
        * file_type:        Type of file in hf_hydrodata GPFS.
        * dataset_type:     A classification type of the dataset.
        * paper_dois:       A space seperate list of DOI references to published papers.
        * structure_type:   Structure of the data: gridded or point.
        * description:      Short description of the dataset containing the data.
        * summary:          Longer summary of the dataset containing the data.
        * id:               The unique id of the entry in the data catalog.

    Example:

    .. code-block:: python

        import hf_hydrodata as hf

        entries = hf.get_catalog_entries(dataset="NLDAS2", temporal_resolution="daily")

        options = {"dataset": "NLDAS2", "temporal_resolution": "daily"}
        entries = hf.get_catalog_entries(options)
        assert len(entries) == 20
        entry = entries[0]
        assert entry["dataset"] == "NLDAS2"
    """

    result = []
    if len(args) > 0 and isinstance(args[0], dict):
        options = args[0]
    else:
        options = kwargs

    result = []
    data_model = load_data_model()
    table = data_model.get_table("data_catalog_entry")

    row_id = options.get("data_catalog_entry_id")
    row_id = row_id if row_id else options.get("id")
    if row_id:
        # Use table.get_row() if we have a row_id because this is cached if already read
        row = table.get_row(row_id)
        if row:
            result = [row]
            return result
    # Get entries from the SQL DB with the API
    if options.get("period") and not options.get("temporal_resolution"):
        options["temporal_resolution"] = options.get("period")

    rows = table._query_data_catalog(options)
    if rows:
        result = [ModelTableRow(rows.get(id)) for id in rows.keys()]
        # Add the query results to the cached results in the table.
        for row_id in rows.keys():
            if row_id not in table.row_ids:
                table.row_ids.append(row_id)
                table.rows[row_id] = rows.get(row_id)
    return result


def get_catalog_entry(*args, **kwargs) -> ModelTableRow:
    """
    Get a single data catalog entry row selected by filter options.

    The parameters to the function can be specified either by passing a dict with the parameter values
    or by passing named parameters to the function.

    Args:
        dataset:        A dataset name (see Gridded Data documentation).
        variable:       A variable from a dataset.
        temporal_resolution:         The temporal_resolution (e.g. hourly, daily, weekly, monthly) of a dataset variable.
        grid:           A grid supported by a dataset (e.g. conus1 or conus2). Normally this is determined by the dataset.
        aggregation:    One of mean, max, min. Normally, only needed for temperature variables.
        start_time:     A time as either a datetime object or a string in the form YYYY-MM-DD. Start of the date range for data.
        end_time:       A time as either a datetime object or a string in the form YYYY-MM-DD. End of the date range for data.
        grid_bounds:    An array (or string representing an array) of points [left, bottom, right, top] in xy grid corridates in the grid of the data.
        latlng_bounds:  An array (or string representing an array) of points [left, bottom, right, top] in lat/lng coordinates mapped with the grid of the data.
        grid_point:     An array (or string representing an array) of points [x, y] in grid corridates of a point in the grid.
        latlng_point:   An array (or string representing an array) of points [lat, lon] in lat/lng coordinates of a point in the grid.
        z:              A value of the z dimension to be used as a filter for this dismension when loading data.
        level:          A HUC level integer when reading HUC boundary files.
        site_id:        Used when reading data associated with an observation site.
        data_catalog_entry_id: Optional. The id of an entry in the data catalog to identify an entry.

    Returns:
        A single ModelTableRow entry that match the filter options or None if no entry is found.

    Raises:
        ValueError:     If the filter options do not uniquely identify a single entry.

    A ModelTableRow contains the attributes of the hf_hydrodata model of a data catalog entry.
    The attributes can be accessed by indexing by the attribute name (e.g. entry["dataset"]).
    You can get the attribute names of an entry using column_names() (e.g. entry.column_names()).

    ModelTableRow metadata attributes:
        * dataset:          A dataset name (see Gridded Data documentation).
        * variable:         A variable from a dataset.
        * temporal_resolution:           The temporal_resolution (e.g. hourly, daily, weekly, monthly) of a dataset variable.
        * grid:             A grid supported by a dataset (e.g. conus1 or conus2). Normally this is determined by the dataset.
        * aggregation:      One of mean, max, min. Normally, only needed for temperature variables.
        * entry_start_date: Earliest available date of data.
        * entry_end_date:   Latest available date of data.
        * units:            Units of the data.
        * file_type:        Type of file in hf_hydrodata GPFS.
        * dataset_type:     A classification type of the dataset.
        * paper_dois:       A space seperate list of DOI references to published papers.
        * structure_type:   Structure of the data: gridded or point.
        * description:      Short description of the dataset containing the data.
        * summary           Longer summary of the dataset containing the data.
        * id:               The unique id of the entry in the data catalog.

    Example:

    .. code-block:: python

        import hf_hydrodata as hf

        options = {
            "dataset": "NLDAS2", "temporal_resolution": "daily",
            "variable": "precipitation", "start_time": "2005-7-1"
        }
        entry = hf.get_catalog_entry(options)
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
        ValueError: If there is more than one entry with the same file_type.
    """
    if len(entries) == 0:
        result = None
    elif len(entries) == 1:
        result = entries[0]
    else:
        # There is more than one entry that matches the filter so pick the preferred entry
        preference_states = [
            {
                "preference_state_key": "file_type",
                "preferred_values": ["pfb", "tif", "netcdf"],
            },
            {
                "preference_state_key": "aggregation",
                "preferred_values": ["mean", "sum", "median", "static", "max", "min"],
            },
            {"preference_state_key": "dataset_version", "preferred_values": None},
        ]

        # evaluate the ambiguous data catalog entries against a preference state to find preferences
        ambiguous_entries = entries.copy()
        for preference_state in preference_states:
            for entry in ambiguous_entries:
                _update_preference_state(preference_state, entry)

            # If there are no ambiguous entries from this preference state then use preferred entry
            ambiguous_entries = preference_state.get("ambiguous")
            if ambiguous_entries is None or len(ambiguous_entries) <= 1:
                result = preference_state.get("preferred_entry")
                break

        # If there are still ambiguous entries then raise an error
        if ambiguous_entries is not None and len(ambiguous_entries) > 1:
            raise ValueError(
                _ambiguous_error_message(ambiguous_entries[0], ambiguous_entries[1])
            )

    return result


def _update_preference_state(preference_state, new_entry):
    """
    Update the preference_state of one data catalog column for one new ambiguous entrry.
    This selects a preferred entry for a preferred value of the data catalog column (key).
    It also updates the list of ambiguous data catalog entries for the state for this column.
    """

    preference_state_key = preference_state.get("preference_state_key")
    new_entry_value = new_entry[preference_state_key]
    preferred_values = preference_state.get("preferred_values")
    preferred_entry = preference_state.get("preferred_entry")
    preferred_value = preference_state.get("preferred_value")
    if preferred_values is None:
        # For preferences like dataset_version the preferred value is the largest value
        if preferred_entry is None:
            preference_state["preferred_entry"] = new_entry
            preference_state["preferred_value"] = new_entry_value
            preference_state["ambiguous"] = None
        else:
            if preferred_value == new_entry_value:
                # There are two entries with the same value this is ambiguous
                ambiguous = preference_state.get("ambiguous")
                if ambiguous is None:
                    ambiguous = [preferred_entry]
                ambiguous.append(new_entry)
                preference_state["ambiguous"] = ambiguous
            elif new_entry_value > preferred_value:
                # The new entry is preferred over the previous preferred
                preference_state["preferred_entry"] = new_entry
                preference_state["preferred_value"] = new_entry_value
                preference_state["ambiguous"] = None

    else:
        # For preferences with an ordered list of preferences use first values
        if new_entry_value in preferred_values:
            # new value of the new entry is one of the preferred values
            if preferred_entry is None:
                preference_state["preferred_entry"] = new_entry
                preference_state["preferred_value"] = new_entry_value
                preference_state["ambiguous"] = None
            else:
                if preferred_value == new_entry_value:
                    # There is more than one entry with the same preferred value
                    ambiguous = preference_state.get("ambiguous")
                    if ambiguous is None:
                        ambiguous = [preferred_entry]
                    ambiguous.append(new_entry)
                    preference_state["ambiguous"] = ambiguous
                elif preferred_values.index(new_entry_value) < preferred_values.index(
                    preferred_entry[preference_state_key]
                ):
                    # The new preferred value is preferred over the previous preferred value
                    preference_state["preferred_entry"] = new_entry
                    preference_state["preferred_value"] = new_entry_value
                    preference_state["ambiguous"] = None
        elif preferred_entry is None:
            # This is a value that is not preferred with no previous preferred value
            ambiguous = preference_state.get("ambiguous")
            if ambiguous is None:
                ambiguous = []
            ambiguous.append(new_entry)
            preference_state["ambiguous"] = ambiguous


def _ambiguous_error_message(entry_1: dict, entry_2: dict) -> str:
    """Returns an error message describing who entry_1 and entry_2 are ambiguous."""

    diff_list = []
    key_variables = [
        "dataset",
        "temporal_resolution",
        "aggregation",
        "grid",
        "variable",
        "site_type",
    ]
    for variable in key_variables:
        value_1 = entry_1[variable]
        value_2 = entry_2[variable]
        if value_1 and value_2 and not value_1 == value_2:
            diff_list.append(f"{variable} = '{value_1}' or '{value_2}'")
    if len(diff_list) > 0:
        differences = ", ".join(diff_list)
    else:
        id_1 = entry_1["id"]
        id_2 = entry_2["id"]
        differences = f"id = '{id_1}' or'{id_2}'"
    return f"Ambiguous filter. Could be {differences}."


def get_table_names() -> List[str]:
    """
    Get the list of table names in the data model.

    Returns:
        List of of all the table names in the hf_hydrodata data catalog model.

    Example:

    .. code-block:: python

        import hf_hydrodata as hf

        names  = hf.get_table_names()
    """

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

    Example:

    .. code-block:: python

        import hf_hydrodata as hf

        rows = hf.get_table_rows("variable", variable_type="atmospheric")
        assert len(rows) == 8
        assert rows[0]["id"] == "air_temp"
    """

    result = []
    if len(args) > 1 and isinstance(args[0], dict):
        options = args[0]
    else:
        options = kwargs
    data_model = load_data_model()
    table = data_model.get_table(table_name)
    row_id = options.get("id")
    if row_id:
        # Use table.get_row() if the request is by Id since this is cached if already read
        row = table.get_row(row_id)
        result = [row] if row else []
    else:
        rows = table._query_data_catalog(options)
        result = [ModelTableRow(rows.get(id)) for id in rows.keys()]

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
        ValueError:     If the filter options are ambiguous and this matches more than one row.

    Example:

    .. code-block:: python

        import hf_hydrodata as hf

        row = hf.get_table_row("variable", variable_type="atmospheric", unit_type="pressure")
        assert row["id"] == "atmospheric_pressure"
    """

    rows = get_table_rows(table_name, *args, **kwargs)
    if len(rows) == 0:
        return None
    if len(rows) > 1:
        id1 = rows[0]["id"]
        id2 = rows[1]["id"]
        raise ValueError(f"Ambiguous result could be id {id1} or {id2}")
    return rows[0]


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
        # Support data catalog filter using data_catalog_entry_id option
        row_value = row["id"] if option == "data_catalog_entry_id" else row[option]
        if row_value is not None and option_value and row_value != option_value:
            result = False
            break
    return result


def _get_point_citations(dataset):
    """
    Return a dictionary with relevant citation information.

    Parameters
    ----------
    dataset : str
        Source from which requested data originated. Currently supported: 'usgs_nwis', 'snotel',
        'scan', 'ameriflux', 'jasechko_2024', 'fan_2013'.

    Returns
    -------
    str
        String containing overall attribution instructions for the provided dataset.
    """

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

    elif dataset == "jasechko_2024":
        c = "Dataset DOI: 10.1038/s41586-023-06879-8"

    elif dataset == "fan_2013":
        c = "Dataset DOI: 10.1126/science.1229881"
    else:
        c = ""

    return c
