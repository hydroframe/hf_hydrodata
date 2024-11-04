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
import datetime
from typing import Tuple
import threading
import platform
import requests

HYDRODATA_URL = os.getenv("HYDRODATA_URL", "https://hydrogen.princeton.edu")
THREAD_LOCK = threading.Lock()
DATA_MODEL_CACHE = None
READ_DC_CALLBACK = None
HYDRODATA = "/hydrodata"
JWT_TOKEN = None
USER_ROLES = None


class ModelTableRow:
    """Represents one row in a model table."""

    def __init__(self, values=None):
        """Constructor"""
        self.row_values = values if values else {}

    def get(self, column_name: str, default_value=None) -> str:
        """Get the value of the named column in the row (simulate dict get method)."""
        return self.row_values.get(column_name, default_value)

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
        parameter_options = {key:options.get(key) for key in options if options.get(key) is not None}
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
            parameters = [
                f"{key}={parameter_options.get(key)}"
                for key in parameter_options.keys()
            ]
            parameter_list = "&".join(parameters)
            url = f"{HYDRODATA_URL}/api/v2/data_catalog?{parameter_list}"
            headers = _get_api_headers(False)
            response = requests.get(url, timeout=120, headers=headers)
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


def load_data_model() -> DataModel:
    """
    Create and return the data model object.

    Returns:
        A DataModel object to access all the tables of the data model.
    """

    global DATA_MODEL_CACHE
    with THREAD_LOCK:
        if DATA_MODEL_CACHE is not None:
            return DATA_MODEL_CACHE
        data_model = DataModel()
        DATA_MODEL_CACHE = data_model
        return data_model


def _get_api_headers(required=True) -> dict:
    """
    Get the API headers containing the jwt token to be passed to API calls.
    Parameters:
        required:   If False then return None if no PIN is registered
    Returns:
        A dict containing an 'Authorization' attribute with a JWT bearer token or None.
    Raises:
        ValueError if no API key is registered or unable to create a JWT token.
    """

    global JWT_TOKEN
    global USER_ROLES
    if not JWT_TOKEN:
        # Only do this if we do not already have a JWT_TOKEN cached in the global variable

        if "verde-" in platform.node() and not os.getenv("https_proxy"):
            # This is to configure a proxy for a princeton environment if not already specified
            os.environ["https_proxy"] = "http://verde:8080"
        email, pin = get_registered_api_pin(required)
        if not required and not email:
            return {}
        url_security = f"{HYDRODATA_URL}/api/api_pins?pin={pin}&email={email}"
        response = requests.get(url_security, timeout=1200)
        if not response.status_code == 200:
            if not required:
                # The PIN is not required so it is ok that the API request returned an error.
                return {}
            raise ValueError(
                f"No registered PIN for '{email}' (expired?). Re-register a pin with https://hydrogen.princeton.edu/pin . Signup with https://hydrogen.princeton.edu/signup. Register the pin with python by executing 'hf_hydrodata.register_api_pin()'."
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
                    "PIN has expired. Re-register a pin with https://hydrogen.princeton.edu/pin . Signup with https://hydrogen.princeton.edu/signup. Register the pin with python by executing 'hf_hydrodata.register_api_pin()'."
                )
        JWT_TOKEN = jwt_json["jwt_token"]
        USER_ROLES = jwt_json.get("user_roles")

    headers = {}
    headers["Authorization"] = f"Bearer {JWT_TOKEN}"
    return headers


def get_registered_api_pin(required=True) -> Tuple[str, str]:
    """
    Get the email and pin registered by the current user on the current machine.

    Parameter:
        required:   If False then return (None, None) if no PIN is registered.
    Returns:
        A tuple (email, pin).
    Raises:
        ValueError:  if no email/pin was registered.

    Example:

    .. code-block:: python

        import hf_hydrodata as hf
        (email, pin) = hf.get_registered_api_pin()
    """

    pin_dir = os.path.expanduser("~/.hydrodata")
    pin_path = f"{pin_dir}/pin.json"
    if not os.path.exists(pin_path):
        if required:
            raise ValueError(
                "No email/pin was registered'. Signup for an account with https://hydrogen.princeton.edu/signup. Create a pin with https://hydrogen.princeton.edu/pin. Register your pin with the python call 'hf_hydrodata.register_api_pin()'."
            )
        else:
            return (None, None)
    try:
        with open(pin_path, "r") as stream:
            contents = stream.read()
            parsed_contents = json.loads(contents)
            email = parsed_contents.get("email")
            pin = parsed_contents.get("pin")
            return (email, pin)
    except Exception as e:
        if not required:
            return (None, None)
        raise ValueError(
            "No email/pin was registered'. Signup for an account with https://hydrogen.princeton.edu/signup. Create a pin with https://hydrogen.princeton.edu/pin. Register your pin with the python call 'hf_hydrodata.register_api_pin()'."
        ) from e
