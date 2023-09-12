"""
Functions to load the csv files of the data catalog model into a DataModel object.

  Contains one function load_data_model() to load the model and several classes
  to represent the loaded model.

  This can be used to load the data catalog model and access all the tables and rows and columns
  of the model.

  Also see the data_access.py module for methods to access information from /hydrodata
  using the model.

  Usage:
    data_model = load_data_model()

    print(data_model.table_names)
"""
# pylint: disable=R0903,W0603,W1514,C0103,R0912

import os
import csv
import json


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
        self.table_names = []
        """A list of table names of the model."""
        self.table_index = {}

    def get_table(self, table_name: str) -> ModelTable:
        """Get the ModelTable object with the table_name."""
        return self.table_index.get(table_name)


DATA_MODEL_CACHE: DataModel = None


def load_data_model() -> DataModel:
    """
    Load the data catalog model from CSV files.

    Returns:
        A DataModel object containing all the tables of the data model.
    """

    global DATA_MODEL_CACHE
    if DATA_MODEL_CACHE is not None:
        return DATA_MODEL_CACHE
    model_dir = f"{os.path.abspath(os.path.dirname(__file__))}/model"
    data_model = DataModel()
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
    _add_columns_to_catalog_entry_table(data_model)
    data_model.table_names.sort()
    DATA_MODEL_CACHE = data_model
    return data_model


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
