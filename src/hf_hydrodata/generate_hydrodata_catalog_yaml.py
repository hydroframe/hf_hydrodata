"""
Functions to generate the hydroframe_catalog_yaml file.
"""
# pylint: disable=C0301,E0401,W0718,C0103,W1514,R1702,C0413

import sys
import os
import logging
from typing import List

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from hf_hydrodata.data_model_access import load_data_model, DataModel

YAML_FILE = "hydrodata_catalog.yaml"


def generate_yaml(output_file: str = None):
    """
    Generate the hydrodata_catalog.yaml file from the data model loaded from the csv files.

    Args:
        output_file:    The path name for the yaml file to be generated. Default is hydroframe_catalog in current directory.

    This prints an error and a fails with a non-zero return code if a column value is invalid
    in the data_catalog_entry.csv file or if a columnn is invalid in a dimension table.

    A column value is invalid if there exists a dimension table with a csv file the same name
    as the column and the value of the column is not one of the key column values from the
    dimension table.

    Returns:
        Exit with an non-zero status code if a validation error is found.
    """

    found_error = False
    try:
        data_model = load_data_model(False)
        found_error = _validate_data_model(data_model)
        output_file = output_file if output_file is not None else f"{YAML_FILE}"
        _generate_yaml_file(output_file, data_model)

    except Exception as e:
        logging.error("Validation error: %s", str(e))
        found_error = True

    if found_error:
        raise ValueError("Failed validation check")


def _generate_yaml_file(output_file: str, data_model: DataModel):
    """
    Generate the hydroframe_catalog.yaml file from the data model.

    Args:
        output_file:    The path name to the yaml file to be generated.
        data_model:     The data model loaded from csv files.
    """
    with open(output_file, "w+") as stream:
        stream.write("##########\n")
        stream.write("# hydroframe_dataset_catalog.yaml\n")
        stream.write("# Meta data about data files in the Princeton GPFS file share.\n")
        stream.write("##########\n")
        stream.write("\nmetadata:\n")
        stream.write("  version: 1\n\n")
        stream.write("sources:\n")
        data_catalog_entry_table = data_model.get_table("data_catalog_entry")
        for data_catalog_entry_id in data_catalog_entry_table.row_ids:
            if data_catalog_entry_id and data_catalog_entry_id.strip():
                row = data_catalog_entry_table.get_row(data_catalog_entry_id)
                stream.write(f'  "{data_catalog_entry_id}":\n')
                stream.write("    metadata:\n")
                for column_name in sorted(data_catalog_entry_table.column_names):
                    if column_name.lower() != "id":
                        value = row.get_value(column_name)
                        if value:
                            stream.write(
                                f"      {column_name}: {_quote_value(value)}\n"
                            )

        stream.write("\n##########\n")
        stream.write("# Dimension Tables\n")
        stream.write("##########\n")
        for table_name in data_model.table_names:
            if table_name != "data_catalog_entry":
                stream.write("\n")
                stream.write(f"{table_name}:\n")
                table = data_model.get_table(table_name)
                for table_id in table.row_ids:
                    if table_id and table_id.strip():
                        row = table.get_row(table_id)
                        stream.write(f'  "{table_id}":\n')
                        for column_name in table.column_names:
                            if column_name.lower() != "id":
                                value = row.get_value(column_name)
                                if value:
                                    stream.write(
                                        f"      {column_name}: {_quote_value(value)}\n"
                                    )


def _quote_value(value):
    """Add quotes around a string value."""
    if isinstance(value, List):
        return value
    return f'"{value}"'


def _validate_data_model(data_model: DataModel):
    """
    Validate the data model. Print errors if any errors are found.

    Args:
        data_model:     The data model loaded from csv files.
    Returns:
        True if a validation error is found.
    """
    found_error = False
    for table_name in data_model.table_names:
        entry_table = data_model.get_table(table_name)
        for entry_table_id in entry_table.row_ids:
            row = entry_table.get_row(entry_table_id)
            for column_name in entry_table.column_names:
                row_value = row.get_value(column_name)
                if _validate_cell_value(
                    row_value, data_model, table_name, entry_table_id, column_name
                ):
                    found_error = True
    return found_error


def _validate_cell_value(
    cell_value: str,
    data_model: DataModel,
    table_name: str,
    table_row_id: str,
    table_column_name: str,
):
    """
    Validate a cell value in a table. Print an message if the value is invalid
    Returns:
        True if the cell value is invalid.
    """
    result = False
    dimension_table = data_model.get_table(table_column_name)
    if dimension_table is not None:
        if cell_value and dimension_table.get_row(cell_value) is None:
            print(
                f"Invalid value '{cell_value}' in column '{table_column_name}' of row '{table_row_id}' of {table_name} table."
            )
            result = True
    return result


if __name__ == "__main__":
    try:
        generate_yaml()
    except Exception as ex:
        print(ex)
        sys.exit(-1)
