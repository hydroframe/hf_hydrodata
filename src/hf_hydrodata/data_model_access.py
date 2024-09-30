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

# pylint: disable=R0903,W0603,W1514,C0103,R0912,R0914,W0718,W0707,C0301,E1102

import os
import json
import threading
import requests

HYDRODATA_URL = os.getenv("HYDRODATA_URL", "https://hydrogen.princeton.edu")
THREAD_LOCK = threading.Lock()
DATA_MODEL_CACHE = None
READ_DC_CALLBACK = None


class ModelTableRow:
    """Represents one row in a model table."""

    def __init__(self, values=None):
        """Constructor"""
        self.row_values = values if values else {}

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
        self.table_name = None
        self.column_names = []
        """A list of the column names in the table."""
        self.row_ids = []
        """A list of row IDs in the table."""
        self.rows = {}

    def get_row(self, row_id: str) -> ModelTableRow:
        """Get the ModelTableRow of a row ID."""
        result = self.rows.get(row_id)
        if result is None:
            response = self._query_data_catalog({"id": row_id})
            if response is not None:
                result = response.get(row_id)
                if result is not None:
                    result = ModelTableRow(result)
                    self.rows[row_id] = result
        return result

    def _query_data_catalog(self, options: dict):
        """
        Call the API to get information from the data catalog using the options filter.
        """

        # Pass any options as parameters
        parameter_options = dict(options)
        parameter_options["table"] = self.table_name
        data_catalog_secret = _get_data_catalog_secret()
        if data_catalog_secret:
            # pass the secret key if the process is running on verde with access to /hydrodata
            # With the secret key the result will return private dc information such as the file path
            # Without the correct secret key only public dc information will be returned
            parameter_options["secret"] = data_catalog_secret

        # Pass the data catalog schema to use to get the data catalog (for unit testing)
        data_catalog_schema = _get_data_catalog_schema()
        parameter_options["schema"] = data_catalog_schema

        if READ_DC_CALLBACK:
            # A callback function is registered to read the DB
            response_json = READ_DC_CALLBACK(parameter_options)
        else:
            # Make an API call to get the data catalog information from the database
            parameters = [f"{key}={parameter_options.get(key)}" for key in parameter_options.keys()]
            parameter_list = "&".join(parameters)
            url = f"{HYDRODATA_URL}/api/v2/data_catalog?{parameter_list}"
            response = requests.get(url, timeout=120)
            if response.status_code == 200:
                response_json = json.loads(response.text)
            else:
                raise ValueError(
                    f"Unable to connect to '{HYDRODATA_URL}' code = '{response.status_code}' to get data catalog information."
                )
        return response_json


def _get_data_catalog_secret():
    """
    Get the data catalog secret if running on /hydrodata
    """
    result = ""
    secret_file = "/hydrodata/.data_catalog_secret"
    if os.path.exists(secret_file):
        with open(secret_file) as src:
            result = src.read()
    return result


def _get_data_catalog_schema():
    """
    Get the data catalog schema to be used to get the catalog from the SQL db.
    This is normally the public schema, but is overridden by DC_SCHEMA env variable.
    This is so unit tests can be run using the public schema by setting env variable.
    """

    result = os.environ.get("DC_SCHEMA", "public")
    return result


class DataModel:
    """Represents a data catalog model."""

    def __init__(self):
        """Constructor"""

        self.table_names = []
        """A list of table names of the model."""
        self.table_index = {}

    def get_table(self, table_name: str) -> ModelTable:
        """Get the ModelTable object with the table_name."""

        table = self.table_index.get(table_name)
        if table is None:
            table = ModelTable()
            table.table_name = table_name
            self.table_index[table_name] = table
        return table
        # return self.table_index.get(table_name)

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
        DATA_MODEL_CACHE = data_model
        return data_model



