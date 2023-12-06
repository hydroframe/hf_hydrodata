"""
    Generate the .vst markdown files for the read-the-docs documumenation of data catalog entries.
    Generate this these files into the directory docs/source with the gen_ prefix.
    These will be included bv the hand referenced .vst files in the same directory.
"""
# pylint: disable=E0401,C0413,C0103,C0301,W1514,R0914,W1309
import sys
import os
import yaml

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))
from hf_hydrodata.data_model_access import load_data_model


def main():
    """
    Main function for the docs generator.
    """

    load_data_model(load_from_api=False)
    generate_datasets()


def generate_datasets():
    """Generate the documentation of datasets"""

    directory = os.path.abspath(os.path.join(os.path.dirname(__file__), "source"))
    (dataset_type_ids, dataset_ids, variable_ids, grid_ids, temporal_resolution_ids) = _collect_visible_ids()

    gen_dataset_list_path = f"{directory}/gen_dataset_list.rst"
    with open(gen_dataset_list_path, "w+") as stream:
        pass
    for dataset_type_id in dataset_type_ids:
        _generate_dataset_type_docs(dataset_type_id, dataset_ids, directory)
    _generate_variable_docs(variable_ids, directory)
    _generate_grid_list_docs(directory)
    generate_temporal_resolution_list_docs(directory)

def _generate_dataset_type_docs(
    dataset_type_id, dataset_ids, directory):
    """
    Generate files for each each dataset for the dataset type.
    Generate a file named gen_{dataset_id}.rst for each dataset in the type.
    Append a table with links to each dataset file to the file gen_dataset_list.rst.
    """
    gen_dataset_list_path = f"{directory}/gen_dataset_list.rst"
    with open(gen_dataset_list_path, "a+") as stream:
        data_model = load_data_model()
        dataset_type_table = data_model.get_table("dataset_type")
        dataset_table = data_model.get_table("dataset")
        dataset_type_row = dataset_type_table.get_row(dataset_type_id)
        dataset_type_description = dataset_type_row["description"]
        dataset_text_map = _load_dataset_text_map()

        stream.write(f".. gen_dataset_type_{dataset_type_id}:\n\n")
        stream.write(f"{dataset_type_description}\n")
        stream.write("^^^^^^^^^^^^^^^^^^^\n")
        stream.write("\n")
        stream.write(f".. list-table:: Datasets\n")
        stream.write("    :widths: 25 100\n")
        stream.write("    :header-rows: 1\n")
        stream.write("\n")
        stream.write("    * - Dateset\n")
        stream.write("      - Description\n")
        for dataset_id in dataset_table.row_ids:
            if dataset_id in dataset_ids:
                dataset_row = dataset_table.get_row(dataset_id)
                if dataset_row["dataset_type"] == dataset_type_id:
                    dataset_description = dataset_row["description"]
                    stream.write(f"    * - :doc:`{dataset_id} <gen_{dataset_id}>`\n")
                    stream.write(f"      - {dataset_description}\n")
                    _generate_dataset_docs(dataset_id, dataset_text_map, directory)
        stream.write("\n")
        stream.write("\n")

def _generate_dataset_docs(dataset_id, dataset_text_map, directory):
    """
    Generate rst file for documentation of the dataset.
    """

    data_model = load_data_model()
    dataset_table = data_model.get_table("dataset")
    grid_table = data_model.get_table("grid")
    dataset_row = dataset_table.get_row(dataset_id)

    dataset_text_entry = dataset_text_map.get(dataset_id)
    dataset_summary = dataset_text_entry.get("summary") if dataset_text_entry else None
    processing_notes = dataset_text_entry.get("processing_notes") if dataset_text_entry else None
    description = dataset_row["description"]
    datasource = dataset_row["datasource"]
    paper_dois = dataset_row["paper_dois"]
    dataset_dois = dataset_row["dataset_dois"]
    dataset_start_date = dataset_row["dataset_start_date"]
    dataset_end_date = dataset_row["dataset_end_date"]
    grids = _collect_grids_in_dataset(dataset_row)
    gen_dataset_docs_path = f"{directory}/gen_{dataset_id}.rst"
    with open(gen_dataset_docs_path, "w+") as stream:
        stream.write(f".. _gen_{dataset_id}:\n")
        stream.write("\n")
        stream.write(f"{description}\n")
        stream.write("^^^^^^^^^^^^^^^^^^\n")
        stream.write("\n")
        if dataset_summary:
            stream.write(f"{dataset_summary}\n\n")

        stream.write(f"**Dataset Name**: {dataset_id}\n\n")
        if datasource and len(datasource.split(" ")) == 1:
            stream.write(f"**Data Source**: {datasource}\n\n")
        if datasource and len(datasource.split(" ")) > 1:
            stream.write(f"**Data Sources**: {datasource}\n\n")
        if processing_notes:
            stream.write("**Data Collection or Processing Notes:**\n\n")
            for paragraph in processing_notes.split("\n"):
                stream.write(f"    {paragraph}\n\n")
        if paper_dois or dataset_dois:
            stream.write("**Citations:**\n\n")
            stream.write("Please refer to the following citations for more information on this dataset and cite them if you use the data\n\n")
            for entry in paper_dois.split(" "):
                if entry:
                    stream.write(f"* https://doi.org/{entry}\n\n")
            for entry in dataset_dois.split(" "):
                if entry:
                    stream.write(f"* {entry}\n\n")

        if dataset_start_date or len(grids) > 0:
            stream.write("**Extent and Resolution**:\n\n")
            stream.write("* Available Date Range: ")
            if dataset_start_date:
                stream.write(f"{dataset_start_date}")
            if dataset_end_date:
                stream.write(f" to {dataset_end_date}")
            stream.write("\n\n")
            _generate_grid_extent_docs(grids, stream)

        _generate_dataset_variable_docs(dataset_row, stream)


def _generate_grid_extent_docs(grids, stream):
    """Generate documentation of the grids into the stream."""
    
    data_model = load_data_model()
    grid_table = data_model.get_table("grid")
    for grid in grids:
        grid_row = grid_table.get_row(grid)
        resolution = grid_row["resolution_meters"]
        resolution = f"{resolution} meters" if resolution else ""
        shape = grid_row["shape"]
        shape_x = shape[2] if shape else ""
        shape_y = shape[1] if shape else ""
        latlng_bounds = grid_row["latlng_bounds"]
        if latlng_bounds:
            latlng_extent = f"{latlng_bounds[1]}, {latlng_bounds[0]},  {latlng_bounds[3]}, {latlng_bounds[2]}"
        else:
            latlng_extent = ""
        crs = grid_row["crs"]
        crs = crs.strip() if crs else ""
        stream.write(f"* Grid: {grid}\n\n")
        stream.write(f"  - Spacial Resolution:  {resolution}\n\n")
        if shape_x and shape_y:
            stream.write(f"  - XY Grid Spacial Extent:  {shape_x} x {shape_y}\n\n")
        if latlng_extent:
            stream.write(f"  - Spacial Exent:  {latlng_extent}\n\n")
        stream.write(f"  - Projection: {crs}\n\n")

def _generate_dataset_variable_docs(dataset_row, stream):
    """
    Generate documentation for variables of the dataset.
    """
    data_model = load_data_model()
    variable_table = data_model.get_table("variable")
    dataset_id = dataset_row["id"]
    structure_type = dataset_row["structure_type"]
    variables = _collect_variables_in_dataset(dataset_row)
    if not variables:
        return
    stream.write(f"Variables\n")
    stream.write("^^^^^^^^^^^^^^^^^^\n")
    stream.write("\n")
    stream.write("This describes the available variables of the dataset.\n")
    stream.write(
        "Use the dataset, variables and temporal_resolution in python access functions as described in the Working with Gridded Data, and Working with Point Observations.\n\n"
    )

    variable_types = _collect_variable_types_of_variables(variables)
    variable_types.sort()
    for variable_type_id in variable_types:
        variable_type_name = variable_type_id.strip().replace("_", " ").title()

        stream.write(f".. list-table:: {variable_type_name} Variables\n")
        if _has_multiple_aggregations(dataset_row, variables) or dataset_id in ["usgs_nwis", "snotel", "scan", "ameriflux"]:
            stream.write("    :widths: 25 60 30 20 20 20")
        else:
            stream.write("    :widths: 25 60 30 20 20")
        if structure_type == "gridded":
            stream.write(" 20")
        stream.write("\n")
        stream.write("    :header-rows: 1\n\n")
        stream.write(f"    * - variable\n")
        stream.write(f"      - description\n")
        stream.write(f"      - temporal_resolution\n")
        stream.write(f"      - units\n")
        if _has_multiple_aggregations(dataset_row, variables) or dataset_id in ["usgs_nwis", "snotel", "scan", "ameriflux"]:
            stream.write(f"      - aggregation\n")
        if structure_type == "gridded":
            stream.write(f"      - grid\n")
        stream.write(f"      - 3D\n")
        for variable_id in variables:
            variable_row = variable_table.get_row(variable_id)
            if variable_row["variable_type"] == variable_type_id:
                variable_title = variable_row["title"]
                variable_description = variable_row["description"]
                variable_description = variable_description if variable_description.strip() else variable_title
                variable_description = (
                    variable_description if variable_description else "No description"
                )
                variable_description = variable_description.strip().capitalize()
                variable_units = _collect_variable_units(dataset_id, variable_id)
                variable_periods = _collect_variable_periods(dataset_id, variable_id)
                variable_grids = _collect_grids_in_variables(dataset_row, variable_id)
                variable_aggregations = ", ".join(
                    _collect_variable_aggregation(dataset_row, variable_id)
                )
                z_dim = "yes" if variable_row["has_z"].lower() == "true" else "no"
                stream.write(f"    * - {variable_id}\n")
                stream.write(f"      - {variable_description}\n")
                stream.write(f"      - {variable_periods}\n")
                stream.write(f"      - {variable_units}\n")
                if _has_multiple_aggregations(dataset_row, variables) or dataset_id in ["usgs_nwis", "snotel", "scan", "ameriflux"]:
                    stream.write(f"      - {variable_aggregations}\n")
                if structure_type == "gridded":
                    stream.write(f"      - {variable_grids}\n")
                stream.write(f"      - {z_dim}\n")

        stream.write("\n")
        stream.write("\n")

def _generate_variable_docs(variable_ids, directory):
    """
    Generate files for each each dataset for the dataset type.
    Generate a file named gen_{dataset_id}.rst for each dataset in the type.
    Append a table with links to each dataset file to the file gen_dataset_list.rst.
    """
    variable_type_ids = _collect_variable_types(variable_ids)
    variable_type_ids.sort()
    gen_variable_list_path = f"{directory}/gen_variable_list.rst"
    with open(gen_variable_list_path, "w+") as stream:
        data_model = load_data_model()
        variable_table = data_model.get_table("variable")
        variable_type_table = data_model.get_table("variable_type")
        for variable_type_id in variable_type_ids:
            variable_type_name = variable_type_id.strip().replace("_", " ").title()
            stream.write(f"{variable_type_name}\n")
            stream.write("----------------------\n\n")
            stream.write(f".. list-table::\n")
            stream.write("    :widths: 20 20 30 20 20 20")
            stream.write("\n")
            stream.write("    :class: longtable\n")
            stream.write("    :header-rows: 1\n\n")
            stream.write(f"    * - variable\n")
            stream.write(f"      - description\n")
            stream.write(f"      - temporal_resolutions\n")
            stream.write(f"      - units\n")
            stream.write(f"      - aggregations\n")
            stream.write(f"      - datasets\n")
            for variable_id in variable_ids:
                variable_row = variable_table.get_row(variable_id)
                if variable_row["variable_type"] == variable_type_id:
                    (dataset_ids, unit_ids, aggregation_ids, grid_ids, temporal_resolution_ids) = _collect_variable_ids(variable_id)
                    variable_title = variable_row["title"]
                    variable_description = variable_row["description"]
                    variable_description = variable_description if variable_description.strip() else variable_title
                    variable_description = (
                        variable_description if variable_description else "No description"
                    )
                    variable_description = variable_description.strip().capitalize()
                    temporal_resolution_list = ", ".join(temporal_resolution_ids)
                    unit_lists = ", ".join(unit_ids)
                    dataset_list = ", ".join(dataset_ids)
                    aggregation_list = ", ".join(aggregation_ids)
                    if not aggregation_list:
                        aggregation_list = " "
                    grid_list = ", ".join(grid_ids)
                    stream.write(f"    * - {variable_id}\n")
                    stream.write(f"      - {variable_description}\n")
                    stream.write(f"      - {temporal_resolution_list}\n")
                    stream.write(f"      - {unit_lists}\n")
                    stream.write(f"      - {aggregation_list}\n")
                    stream.write(f"      - {dataset_list}\n")

def _generate_grid_list_docs(directory):
    """
    Generate documentation for all the grids in the data model.
    """
    gen_variable_list_path = f"{directory}/gen_grid_list.rst"
    with open(gen_variable_list_path, "w+") as stream:
        data_model = load_data_model()
        grid_table = data_model.get_table("grid")
        _generate_grid_extent_docs(grid_table.row_ids, stream)

def generate_temporal_resolution_list_docs(directory):
    """Generate the gen_temporal_resolution_list.rst file to document temporal resolutions"""

    gen_temportal_resolution_list_path = f"{directory}/gen_temporal_resolution_list.rst"
    data_model = load_data_model()
    temporal_resolution_table = data_model.get_table("temporal_resolution")
    with open(gen_temportal_resolution_list_path, "w+") as stream:
        data_model = load_data_model()
        temporal_resolution_table = data_model.get_table("temporal_resolution")

        stream.write(".. list-table::\n")
        stream.write("    :widths: 20 20\n")
        stream.write("    :header-rows: 1\n\n")
        stream.write("    * - temporal_resolution\n")
        stream.write("      - description\n")
        for temporal_resolution_id in temporal_resolution_table.row_ids:
            temporal_resolution_row = temporal_resolution_table.get_row(temporal_resolution_id)
            description = temporal_resolution_row["period_description"]
            stream.write(f"    * - {temporal_resolution_id}\n")
            stream.write(f"      - {description}.\n")


def _has_multiple_aggregations(dataset_row, variables):
    """Determine if the any of the variables in the dataset has multiple aggregations"""

    result = False
    for variable_id in variables:
        aggregations = _collect_variable_aggregation(dataset_row, variable_id)
        if len(aggregations) > 1:
            result = True
            break
    return result

def _collect_variable_ids(variable_id):
    """
    Collect ids associated with variable_id.
    
    Returns:
        A tuple of arrays (dataset_ids, unit_ids, aggregation_ids, grid_ids, temporal_resolution_ids)
    """

    datasets = []
    units = []
    aggregations = []
    grids = []
    temporal_resolutions = []

    data_model = load_data_model()
    data_catalog_entry_table = data_model.get_table("data_catalog_entry")
    for data_catalog_entry_id in data_catalog_entry_table.row_ids:
        data_catalog_entry_row = data_catalog_entry_table.get_row(data_catalog_entry_id)
        dataset_variable_id = data_catalog_entry_row["variable"]
        dataset_id = data_catalog_entry_row["dataset"]
        unit_id = data_catalog_entry_row["units"]
        temporal_resolution_id = data_catalog_entry_row["temporal_resolution"]
        aggregation_id = data_catalog_entry_row["aggregation"]
        security_level = data_catalog_entry_row["security_level"]
        grid_id = data_catalog_entry_row["grid"]
        if dataset_variable_id == variable_id and _is_entry_visible(security_level):
            if grid_id and grid_id not in grids:
                grids.append(grid_id)
            if unit_id and not unit_id == "None" and unit_id not in units:
                if unit_id == "" or unit_id == "-":
                    unit_id = "static"
                if unit_id not in units:
                    units.append(unit_id)
            if temporal_resolution_id and temporal_resolution_id not in temporal_resolutions:
                temporal_resolutions.append(temporal_resolution_id)
            if aggregation_id and aggregation_id != "" and aggregation_id != "-" and aggregation_id not in aggregations:
                aggregations.append(aggregation_id)
            if dataset_id not in datasets:
                datasets.append(dataset_id)
    return (datasets, units, aggregations, grids, temporal_resolutions)

def _collect_variable_types(variable_ids):
    """Return the list of variable_type_ids that contain one of the variables in variable_ids."""

    variable_types = []
    data_model = load_data_model()
    variable_table = data_model.get_table("variable")
    for variable_id in variable_ids:
        variable_row = variable_table.get_row(variable_id)
        variable_type_id = variable_row["variable_type"]
        if variable_type_id and variable_type_id not in variable_types:
            variable_types.append(variable_type_id)
    return variable_types

def _collect_variable_periods(dataset_id, variable_id):
    periods = []
    data_model = load_data_model()
    data_catalog_entry_table = data_model.get_table("data_catalog_entry")
    for data_catalog_entry_id in data_catalog_entry_table.row_ids:
        data_catalog_entry_row = data_catalog_entry_table.get_row(data_catalog_entry_id)
        security_level = data_catalog_entry_row["security_level"]
        if _is_entry_visible(security_level):
            if (
                data_catalog_entry_row["dataset"] == dataset_id
                and data_catalog_entry_row["variable"] == variable_id
            ):
                period = data_catalog_entry_row["period"]
                period = period if period else "static"
                if period not in periods:
                    periods.append(period)
    return ", ".join(periods)

def _collect_variable_units(dataset_id, variable_id):
    units_list = []
    data_model = load_data_model()
    data_catalog_entry_table = data_model.get_table("data_catalog_entry")
    for data_catalog_entry_id in data_catalog_entry_table.row_ids:
        data_catalog_entry_row = data_catalog_entry_table.get_row(data_catalog_entry_id)
        security_level = data_catalog_entry_row["security_level"]
        if _is_entry_visible(security_level):
            if (
                data_catalog_entry_row["dataset"] == dataset_id
                and data_catalog_entry_row["variable"] == variable_id
            ):
                units = data_catalog_entry_row["units"]
                units = units if units and units not in ["-"] else "unitless"
                if units not in units_list:
                    units_list.append(units)
    return ", ".join(units_list)


def _collect_variable_aggregation(dataset_row, variable_id):
    aggregations = []
    data_model = load_data_model()
    data_catalog_entry_table = data_model.get_table("data_catalog_entry")
    dataset_id = dataset_row["id"]
    for data_catalog_entry_id in data_catalog_entry_table.row_ids:
        data_catalog_entry_row = data_catalog_entry_table.get_row(data_catalog_entry_id)
        security_level = data_catalog_entry_row["security_level"]
        if _is_entry_visible(security_level):
            if (
                data_catalog_entry_row["dataset"] == dataset_id
                and data_catalog_entry_row["variable"] == variable_id
            ):
                aggregation = data_catalog_entry_row["aggregation"].strip()
                aggregation = (
                    aggregation
                    if len(aggregation) > 0 and not aggregation == "-"
                    else ""
                )
                if aggregation and aggregation not in aggregations:
                    aggregations.append(aggregation)
    return aggregations


def _generate_dataset_projection_docs(dataset_row, stream):
    """
    Generate documention of the projections used in the dataset into the stream of the rst document.
    """
    grids = _collect_grids_in_dataset(dataset_row)
    if len(grids) > 0:
        stream.write("Projections\n")
        stream.write("^^^^^^^^^^^^^^^^^^\n")
        stream.write("\n")
        if len(grids) == 1:
            _generate_grid_docs(grids[0], stream)
        elif len(grids) > 1:
            stream.write(
                f"There are several different grids supported by this dataset.\n\n"
            )
            for grid_id in grids:
                stream.write(f"Grid: {grid_id}\n")
                stream.write("^^^^^^^^^^^^^^^\n")
                _generate_grid_docs(grid_id, stream)


def _generate_grid_docs(grid_id, stream):
    """Generate documentation of grid to stream of rst file."""

    data_model = load_data_model()
    grid_table = data_model.get_table("grid")

    grid_row = grid_table.get_row(grid_id)
    latlng_bounds = grid_row["latlng_bounds"]
    resolution = grid_row["resolution_meters"]
    grid_crs = grid_row["crs"]
    grid_crs_dict = _parse_crs_to_dict(grid_crs)
    a = grid_crs_dict.get("a")
    b = grid_crs_dict.get("b")
    lat1 = grid_crs_dict.get("lat_1")
    lat2 = grid_crs_dict.get("lat_2")
    lat0 = grid_crs_dict.get("lat_0")
    lon0 = grid_crs_dict.get("lon_0")
    stream.write(f"The projected coordinate system of '{grid_id}' is Lambert Conformal Conic.\n")
    if resolution:
        stream.write(f"The resolution is {resolution} meters.\n")
    stream.write("\n")
    if not a or not b:
        return
    x0 = grid_crs_dict.get("x_0")
    y0 = grid_crs_dict.get("y_0")

    if round(float(a)) == round(float(b)):
        stream.write(
            f"The underlying datum of grid '{grid_id}' is a sphere with radius {a} meters.\n\n"
        )
    else:
        stream.write(
            f"The underlying datum of grid '{grid_id}' is an elipsoid with equatorial radius {a} meters and polar radius {b} meters.\n\n"
        )
    stream.write(
        f"The first parallel is {lat1}. The second parallel is {lat2}. The origin latitude is {lat0}. The origin longitude is {lon0}. "
    )
    stream.write(
        f"The false easting is {x0} meters. The false northing is {y0} meters."
    )
    stream.write("\n")
    stream.write("\n")
    shape = grid_row["shape"]
    x = shape[2]
    y = shape[1]
    z = shape[0]
    stream.write(f"The grid '{grid_id}' has dimensions X={x},  Y={y},  Z={z}.\n")
    if latlng_bounds and len(latlng_bounds) == 4:
        south = latlng_bounds[0]
        west = latlng_bounds[1]
        north = latlng_bounds[2]
        east = latlng_bounds[3]
        stream.write(f"\nThe grid has lat/lon bounds of south {south}, west {west}, north {north}, east {east}.\n")
    stream.write("\n")

def _parse_crs_to_dict(grid_crs: str) -> dict:
    """Parse the crs string into a dict of attributes."""
    result = {}
    for entry in grid_crs.split(" "):
        if entry:
            parts = entry.split("=")
            name = parts[0].strip()
            name = name.replace("+", "")
            value = parts[1]
            result[name] = value
    return result


def _collect_variable_types_of_variables(variables):
    """
    Collect the variable types of the list of variables:
    Returns:
        List of variable type id.
    """

    result = []
    data_model = load_data_model()
    variable_table = data_model.get_table("variable")

    for variable_id in variables:
        variable_row = variable_table.get_row(variable_id)
        variable_type = variable_row["variable_type"]
        if variable_row["id"] == variable_id:
            if variable_type and not variable_type in result:
                result.append(variable_type)
    return result


def _collect_variables_in_dataset(dataset_row):
    """
    Collect the list of variables used in the dataset.

    Returns:
        A list of variable ids.
    """
    result = []
    dataset_id = dataset_row["id"]
    data_model = load_data_model()
    data_catalog_entry_table = data_model.get_table("data_catalog_entry")
    for data_catalog_entry_id in data_catalog_entry_table.row_ids:
        data_catalog_entry_row = data_catalog_entry_table.get_row(data_catalog_entry_id)
        variable_id = data_catalog_entry_row["variable"]
        security_level = data_catalog_entry_row["security_level"]
        if data_catalog_entry_row["dataset"] == dataset_id and _is_entry_visible(
            security_level
        ):
            if variable_id and not variable_id in result:
                result.append(variable_id)
    return result


def _collect_grids_in_variables(dataset_row, variable_id):
    """
    Collect the list of grids used in variable_id of the dataset.

    Returns:
        A list of variable ids.
    """
    grids = []
    dataset_id = dataset_row["id"]
    data_model = load_data_model()
    data_catalog_entry_table = data_model.get_table("data_catalog_entry")
    for data_catalog_entry_id in data_catalog_entry_table.row_ids:
        data_catalog_entry_row = data_catalog_entry_table.get_row(data_catalog_entry_id)
        dataset_variable_id = data_catalog_entry_row["variable"]
        security_level = data_catalog_entry_row["security_level"]
        grid = data_catalog_entry_row["grid"]
        if (
            data_catalog_entry_row["dataset"] == dataset_id
            and dataset_variable_id == variable_id
            and _is_entry_visible(security_level)
        ):
            if grid and not grid in grids:
                grids.append(grid)
    return ", ".join(grids)


def _collect_grids_in_dataset(dataset_row):
    """
    Collect the list of grids used in the dataset.

    Returns:
        A list of grid ids.
    """
    result = []
    dataset_id = dataset_row["id"]
    data_model = load_data_model()
    data_catalog_entry_table = data_model.get_table("data_catalog_entry")
    for data_catalog_entry_id in data_catalog_entry_table.row_ids:
        data_catalog_entry_row = data_catalog_entry_table.get_row(data_catalog_entry_id)
        grid = data_catalog_entry_row["grid"]
        if data_catalog_entry_row["dataset"] == dataset_id:
            if grid and not grid in result:
                result.append(grid)
    return result


def _generate_references_docs(dataset_row, stream):
    """
    Generate documention of references of the dataset into the stream of the RST documentation.
    """

    paper_dois = dataset_row["paper_dois"]
    dataset_dois = dataset_row["dataset_dois"]
    if paper_dois or dataset_dois:
        stream.write("References\n")
        stream.write("^^^^^^^^^^\n")
        stream.write(
            "Papers describing the generation of some variables in the dataset.\n"
        )
        stream.write("\n")
        paper_dois_list = paper_dois.split(";")
        for paper_dois_entry in paper_dois_list:
            paper_dois_entry = paper_dois_entry.strip()
            stream.write(f"* `{paper_dois_entry}`_\n")
            stream.write(
                f".. _`{paper_dois_entry}`: https://doi.org/{paper_dois_entry}\n"
            )


def _collect_visible_ids():
    """
    Collect the visible datasets and dataset types.

    Returns:
        A tuple (dataset_types, datasets, variables, grids, temporal_resolutions) with the list of ids of datset_types and datasets.
    """
    dataset_type_ids = []
    dataset_ids = []
    variable_ids = []
    grid_ids = []
    temporal_resolution_ids = []
    data_model = load_data_model()
    dataset_type_table = data_model.get_table("dataset_type")
    dataset_table = data_model.get_table("dataset")
    data_catalog_entry_table = data_model.get_table("data_catalog_entry")
    for dataset_type_id in dataset_type_table.row_ids:
        for dataset_id in dataset_table.row_ids:
            for data_catalog_entry_id in data_catalog_entry_table.row_ids:
                data_catalog_entry_row = data_catalog_entry_table.get_row(
                    data_catalog_entry_id
                )
                security_level = data_catalog_entry_row["security_level"]
                variable_id = data_catalog_entry_row["variable"]
                grid_id = data_catalog_entry_row["grid"]
                temporal_resolution_id = data_catalog_entry_row["temporal_resolution"]
                if dataset_id == data_catalog_entry_row["dataset"]:
                    if _is_entry_visible(security_level):
                        if not dataset_type_id in dataset_type_ids:
                            dataset_type_ids.append(dataset_type_id)
                        if not dataset_id in dataset_ids:
                            dataset_ids.append(dataset_id)
                        if not variable_id in variable_ids:
                            variable_ids.append(variable_id)
                        if not grid_id in grid_id:
                            grid_ids.append(grid_id)
                        if not temporal_resolution_id in temporal_resolution_ids:
                            temporal_resolution_ids.append(temporal_resolution_id)

    return (dataset_type_ids, dataset_ids, variable_ids, grid_ids, temporal_resolution_ids)


def _is_entry_visible(security_level: str) -> bool:
    """Return true if security_level is vislble"""
    result = security_level in ["1", "2", "3"]
    return result

def _load_dataset_text_map()->dict:
    """
    Load the dataset_text.yaml file
    Returns:
        A dict containing the dataset keys with the value of the a sub dict with summary, and processing_notes keys.
    """
    result = {}
    path = os.path.abspath(os.path.join(os.path.dirname(__file__), "dataset_text.yaml"))
    with open(path, "r") as stream:
        result = yaml.safe_load(stream)
        result = result.get("datasets")
    return result

if __name__ == "__main__":
    main()
