"""
    Program to release the development schema to the public schema.
    This should be called from a Jenkins job running on the hmei-hydrodata account.
    This account contains the DB configuration to get rights to write to the public schema.

    Normal developer accounts or the normal hmei-hydro account only have read only
    permissions to the public schema and write access to the development schema.

    This release can be done manually be executing the DDL commands in the public_release.ddl file.
    This program exists to execute this operation from a Jenkins job using credentials from server.
"""

# pylint: disable=C0301,W1514

import os
import datetime
import psycopg
from psycopg.rows import dict_row


def main():
    """Copy tables from development schema to public schema using DDL file."""
    publish_schema("development", "public")


def publish_schema(source_schema: str, target_schema: str):
    """
    Copy the tables from the source schema to the target schema.
    Copy table in an order to avoid referental integrety constraint violations.
    Use the public_release.ddl file in current directory for order of tables.
    """
    connection = _get_connection(target_schema)

    with open("public_release.ddl", "r") as fp:
        ddl = fp.read()
        # Split line by ; and execute each DDL command in the file seperately
        for line in ddl.split(";"):
            line = line.strip()
            line = line.replace("\n", " ")
            # Use a temp_schema during replacement to allow swap of public/development schema 
            line = line.replace("public.", "temp_schema.")
            line = line.replace("public.", f"{target_schema}.")
            line = line.replace("development.", f"{source_schema}.")
            line = line.replace("temp_schema.", f"{target_schema}.")
            if len(line) > 0 and not line.startswith("#"):
                print(line)
                _execute_sql(connection, line)

        _add_current_date_version(connection)


def _add_current_date_version(connection):
    """Add a entry to the version table with the current date if such a version row does not exist yet."""

    current_date = datetime.datetime.now().strftime("%Y-%m-%d")
    sql = f"SELECT id from version WHERE id='{current_date}'"
    rows = _sql_query(connection, sql)
    if len(rows) == 0:
        # Insert new version with current date into version table before exporting to csv
        sql = f"INSERT INTO version (id,modified_by,comments) VALUES ('{current_date}', 'jenkins job', 'exported by jenkins job')"
        _execute_sql(connection, sql)


def _execute_sql(connection, sql, data_values=None):
    """Execute a SQL statement on the database connection"""

    cur = connection.cursor()
    if data_values:
        cur.execute(sql, data_values)
    else:
        cur.execute(sql)


def _sql_query(connection, sql):
    """Execute a SQL query on the database connection and return dict with the result."""

    result = []
    with connection.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            item = {}
            for key in row.keys():
                item[key] = row[key]
            result.append(item)
        return result


def _get_connection(schema: str) -> psycopg.connection:
    """Get the database connection of the data catalog database"""

    try:
        params = _get_db_configuration(schema)
        params["autocommit"] = True
        params["options"] = f"-c search_path={schema}"
        params["row_factory"] = dict_row
        connection = psycopg.connect(**params)
        return connection
    except Exception as e:
        raise ValueError("Unable to connect to data_catalog database.") from e


def _get_db_configuration(schema: str):
    """Read the database configuration options from a configuration file in protected user home directory."""

    result = {}
    if schema in ["public", "public_test"]:
        config_file = os.path.expanduser("~/.data_catalog/db_credentials_public")
    else:
        config_file = os.path.expanduser("~/.data_catalog/db_credentials_development")
    print(config_file)

    if not os.path.exists(config_file):
        raise ValueError(f"Missing database configuration file '{config_file}'")
    with open(config_file, "r") as stream:
        lines = stream.readlines()
        for line in lines:
            line = line.strip()
            if line:
                key, value = line.split("=")
                key = key.strip()
                value = value.strip()
                value = int(value) if key == "port" else value
                result[key] = value
    return result


if __name__ == "__main__":
    main()
