"""
Functions to load the csv files of the data catalog model into a DataModel object.

  Contains one function load_data_model() to load the model and several classes
  to represent the loaded model.

  This can be used to load the data catalog model and access all the tables and rows and columns
  of the model.

  Also see the data_access.py module for methods to access information from /hydrodatas
  using the model.

  Usage:
    data_model = load_data_model()

    print(data_model.table_names)
"""
# pylint: disable=R0903,W0603,W1514,C0103,R0912,R0914,W0718,W0707

import os
import sys
import csv
import json
import threading
import logging
from warnings import warn
import requests

HYDRODATA_URL = os.getenv("HYDRODATA_URL", "https://hydrogen.princeton.edu")
THREAD_LOCK = threading.Lock()
DATA_MODEL_CACHE = None
REMOTE_DATA_CATALOG_VERSION = "1.0.3"

class ModelTableRow:
    """Represents one row in a model table."""

    def __init__(self):
        """Constructor"""
        self.row_values = {}

    def column_names(self):
        """Return the column names of the row."""
        return list(self.row_values.keys())

    def get_value(self, column_name: str) -> str:
        """Get the value of the named column in the row."""
        return self.row_values.get(column_name)

    def set_value(self, column_name: str, value):
        """Set the value of the named column in the row."""
        self.row_values[column_name] = value

    def __getitem__(self, column_name):
        return self.row_values.get(column_name)

    def __repr__(self):
        """Custom string representation of row."""
        return str(self.row_values)


class ModelTable:
    """Represents a model table."""

    def __init__(self):
        """Constructor."""

        self.column_names = []
        """A list of the column names in the table."""
        self.row_ids = []
        """A list of row IDs in the table."""
        self.rows = {}

    def get_row(self, row_id: str) -> ModelTableRow:
        """Get the ModelTableRow of a row ID."""
        return self.rows.get(row_id)


class DataModel:
    """Represents a data catalog model."""

    def __init__(self):
        """Constructor"""

        self.table_names = []
        """A list of table names of the model."""
        self.table_index = {}

    def get_table(self, table_name: str) -> ModelTable:
        """Get the ModelTable object with the table_name."""

        return self.table_index.get(table_name)

    def export_to_dict(self) -> dict:
        """Export the csf files of the data model to a dict"""

        result = {}
        model_dir = f"{os.path.abspath(os.path.dirname(__file__))}/model"
        for file_name in os.listdir(model_dir):
            if file_name.endswith(".csv"):
                try:
                    table_name = file_name.replace(".csv", "")
                    with open(f"{model_dir}/{file_name}") as csv_file:
                        model_table_entries = []
                        rows = csv.reader(csv_file, delimiter=",")
                        for row_count, row in enumerate(list(rows)):
                            if row_count == 0:
                                # Read header
                                column_names = row
                            else:
                                entry = {}
                                for col_count, col_name in enumerate(column_names):
                                    col_value = row[col_count]
                                    entry[col_name] = col_value
                                model_table_entries.append(entry)
                        result[table_name] = model_table_entries
                except Exception as e:
                    raise ValueError(f"Error reading '{file_name}' {str(e)}") from e
        return result

    def import_from_dict(self, model: dict):
        """Import the latest data model from the dict created by export_to_dict."""

        if model is None:
            return

        # Clear out old rows from tables
        for table_name in model.keys():
            table = self.get_table(table_name)
            table.row_ids = []
            table.rows = {}
            table.column_names = []

        # Load new rows from dict
        for table_name in model.keys():
            table = self.get_table(table_name)
            rows = model.get(table_name)
            for row in rows:
                table_row = ModelTableRow()
                id_value = None
                for col_name in row.keys():
                    if not col_name in table.column_names:
                        table.column_names.append(col_name)
                    col_value = _parse_column_value(row.get(col_name))
                    if col_name == "id":
                        id_value = col_value
                    table_row.row_values[col_name] = col_value
                table.row_ids.append(id_value)
                table.rows[id_value] = table_row

        _add_columns_to_catalog_entry_table(self)
        self.table_names.sort()


def load_data_model(load_from_api=True) -> DataModel:
    """
    Load the data catalog model from CSV files.

    Returns:
        A DataModel object containing all the tables of the data model.
    """

    global DATA_MODEL_CACHE
    with THREAD_LOCK:
        if DATA_MODEL_CACHE is not None:
            return DATA_MODEL_CACHE
        data_model = DataModel()

        model_dir = f"{os.path.abspath(os.path.dirname(__file__))}/model"
        for file_name in os.listdir(model_dir):
            if file_name.endswith(".csv"):
                try:
                    table_name = file_name.replace(".csv", "")
                    data_model.table_names.append(table_name)
                    model_table = ModelTable()
                    data_model.table_index[table_name] = model_table
                    with open(f"{model_dir}/{file_name}") as csv_file:
                        rows = csv.reader(csv_file, delimiter=",")
                        for row_count, row in enumerate(list(rows)):
                            if row_count == 0:
                                for col_count, col in enumerate(list(row)):
                                    if col_count == 0:
                                        model_table.column_names.append("id")
                                    else:
                                        model_table.column_names.append(col)
                            else:
                                table_row = ModelTableRow()
                                for col_count, col in enumerate(list(row)):
                                    if col_count == 0:
                                        table_row.row_values["id"] = col
                                        model_table.row_ids.append(col)
                                        model_table.rows[col] = table_row
                                    else:
                                        table_row.row_values[
                                            model_table.column_names[col_count]
                                        ] = _parse_column_value(col)

                except Exception as e:
                    raise ValueError(f"Error reading '{file_name}' {str(e)}") from e
        _add_period_temporal_resolution_column(data_model)
        _add_columns_to_catalog_entry_table(data_model)
        data_model.table_names.sort()

        if load_from_api:
            # Replace the data model with the version from the API
            sys.tracebacklimit = 0
            _load_model_from_api(data_model)
        DATA_MODEL_CACHE = data_model

    return data_model


def _add_period_temporal_resolution_column(data_model: DataModel):
    """
    Add column so data_catalog_entry table has both period and temporal_resolution columns.
    """
    data_catalog_entry_table = data_model.get_table("data_catalog_entry")
    if (
        "period" in data_catalog_entry_table.column_names
        and "temporal_resolution" in data_catalog_entry_table.column_names
    ):
        return
    data_catalog_entry_table.column_names.append("period")
    for row_id in data_catalog_entry_table.row_ids:
        row = data_catalog_entry_table.get_row(row_id)
        if row["temporal_resolution"]:
            period = row["temporal_resolution"]
            row.set_value("period", period)
        elif row["period"]:
            period = row["period"]
            row.set_value("temporal_resolution", period)


def _add_columns_to_catalog_entry_table(data_model: DataModel):
    """
    Add columns to data_catalog_entry_table

    Add a column if there is an existing column with a dimension table name and
    that dimension table contains other columns that are also dimension colunmn names.
    Add such columns to the data_catalog_entry table if it does not already exists.

    Also add all columns (except id colunn) from the dataset dimension table to the
    data_catalog_entry table.
    """

    data_catalog_entry_table = data_model.get_table("data_catalog_entry")
    dataset_table = data_model.get_table("dataset")
    column_index = len(data_catalog_entry_table.column_names)

    # Add dimension columns from dimension tables
    column_index = data_catalog_entry_table.column_names.index("dataset") + 1
    for table_name in data_model.table_names:
        if table_name != "data_catalog_entry":
            dimension_table = data_model.get_table(table_name)
            if table_name in data_catalog_entry_table.column_names:
                column_index = (
                    data_catalog_entry_table.column_names.index(table_name) + 1
                )
                for column_name in dimension_table.column_names:
                    if (
                        column_name.lower() != "id"
                        and column_name not in data_catalog_entry_table.column_names
                        and data_model.get_table(column_name) is not None
                    ):
                        data_catalog_entry_table.column_names.insert(
                            column_index, column_name
                        )
                        column_index = column_index + 1
                        _fill_data_catalog_entry_table_column_value(
                            data_model, table_name, column_name
                        )

    # Add columns from dataset table
    column_index = len(data_catalog_entry_table.column_names)
    for column_name in dataset_table.column_names:
        if (
            column_name.lower() != "id"
            and column_name not in data_catalog_entry_table.column_names
        ):
            data_catalog_entry_table.column_names.insert(column_index, column_name)
            column_index = column_index + 1
            _fill_data_catalog_entry_table_column_value(
                data_model, "dataset", column_name
            )


def _fill_data_catalog_entry_table_column_value(
    data_model: DataModel, table_name: str, column_name: str
):
    """
    Fill in the value of the new column_name in the data_catalog_entry_table for each row
    by getting the value of the table_name column from the data_catalog_entry table and
    getting the row with that ID from the table_name dimension table and then the value
    from the dimension table from the column_name of the dimension table.

    This populates the column name of the data_catalog_entry to match the value of the
    corresponding column from the dimension table.
    """

    data_catalog_entry_table = data_model.get_table("data_catalog_entry")
    table = data_model.get_table(table_name)
    for row_id in data_catalog_entry_table.row_ids:
        row = data_catalog_entry_table.get_row(row_id)
        key_value = row.get_value(table_name)
        if key_value:
            key_row = table.get_row(key_value)
            if key_row:
                key_row_value = key_row.get_value(column_name)
                row.set_value(column_name, key_row_value)


def _parse_column_value(column_value: str):
    """
    Parse the value of a column in case the string from the csv file represent an array.

    Returns:
        An array object if the string represents an array, otherwise the column value.
    """

    if len(column_value) > 0 and column_value[0] == "[":
        # This is an array, not an atomic value
        column_value = column_value.replace("'", '"')
        column_value = json.loads(column_value)
        return column_value
    return column_value


def _load_model_from_api(data_model: DataModel):
    """Load the latest version of the model from model in the API."""

    url = f"{HYDRODATA_URL}/api/config/data_catalog_model?version={REMOTE_DATA_CATALOG_VERSION}"
    try:
        response = requests.get(url, timeout=120)
        if response.status_code == 200:
            response_json = json.loads(response.text)
            if "temporal_resolution" not in response_json.keys():
                return
            data_model.import_from_dict(response_json)
            _add_period_temporal_resolution_column(data_model)
        else:
            warn(
                f"Unable to update model from API (no internet access?) Error {response.status_code} from '{url}'"
            )
            # Do not cache data model if an API error occurred
    except requests.exceptions.ReadTimeout:
        raise ValueError(
            "Timeout while trying to load latest model from server. Try again later."
        )
    except Exception:
        warn(
            f"Warning - unable to update model from API (no internet access?) using '{url}'"
        )
