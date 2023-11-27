"""
Validate the data model by checking for referrential integrity constraints
between columns and tables.

Display any violoations to stdout and return a non-zero return code if any
violations are found
"""
# pylint: disable=C0301,E0401,W0718,C0103,W1514,R1702,C0413

import sys
import os
from typing import List

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from hf_hydrodata.data_model_access import load_data_model, DataModel

YAML_FILE = "hydrodata_catalog.yaml"

def main():
    try:
        data_model = load_data_model(False)
        found_error = _validate_data_model(data_model)
        if found_error:
            print("Failed validation check!")
            sys.exit(-1)
    except Exception:
        print("Failed validation check!")
        sys.exit(-1)

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
        unique_id_map = {}
        for entry_table_id in entry_table.row_ids:
            if unique_id_map.get(entry_table_id):
                print(f"Duplicate id '{entry_table_id} in table '{table_name}'")
                found_error = True
            unique_id_map[entry_table_id] = True
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
    main()
