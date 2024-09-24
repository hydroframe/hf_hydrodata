"""
    Import/Export CSV model to SQL database in either the unittest or public schema
    and validate the files. 
    
    This program exists to create SQL csv backups to save or restore publish release points.
    Also used to initialize the SQL db when migrating from csv model files.
"""

# pylint: disable=C0301,E1129,W1514,R0914,R0911,W0702

import sys
import os
import datetime
import csv
from typing import List
import psycopg
from psycopg.rows import dict_row


def main():
    """Main routine to import/export data catalog to unittest or public schema based on command line argument."""

    if len(sys.argv) <= 1:
        print(
            "Usage: python hf_data_catalog <operation> <schema> <model_path> [<ro_db_users>] [<rw_db_users>]"
        )
        return -1
    if len(sys.argv) <= 2:
        print(
            "Error: Must specify schema as either development, public or public_test in command line argument."
        )
        return -1
    if len(sys.argv) <= 3:
        print("Error: Must specify the model path in the command line argument.")
        return -1

    operation = sys.argv[1]
    schema = sys.argv[2]
    model_path = sys.argv[3]
    ro_users_str = sys.argv[4] if len(sys.argv) > 4 else None
    rw_users_str = sys.argv[5] if len(sys.argv) > 5 else None

    if len(sys.argv) > 6:
        print("Error: Too many arguments specified")
        return -1

    # Allow the optional list of ro_users and rw_users to have a default value
    ro_users = (
        ro_users_str.split(",")
        if ro_users_str is not None
        else ["hmei-hydro", "data_catalog-rw", "data_catalog-ro"]
    )
    rw_users = rw_users_str.split(",") if rw_users_str is not None else ["hmei-hydro"]

    if operation not in ["drop", "import", "export"]:
        print("Error: The operation must be drop, import or export")
        return -1
    if schema not in ["development", "public_test", "public"]:
        print("Error: The schema must be development, public or public_test")
        return -1
    if not os.path.exists(model_path):
        print(f"Model path '${model_path}' does not exist.")

    if operation == "drop":
        drop_all_tables(model_path, schema)
    elif operation == "import":
        import_to_sql(model_path, schema, ro_users, rw_users)
    elif operation == "export":
        export_to_csv(model_path, schema)
    return 0


def export_to_csv(model_path: str, schema: str = "development"):
    """Export the model from the SQL schema into the model_path directory as csv files for each table."""

    if not os.path.exists(model_path):
        raise ValueError(f"The folder '{model_path}' does not exist.")
    connection = get_connection(schema)
    with connection:
        _add_current_date_version(connection)
        table_names = _get_list_of_tables(connection, schema)
        for table_name in table_names:
            column_names = _get_columns_of_table(connection, schema, table_name)
            rows = _get_row_values_from_table(connection, table_name)
            with open(f"{model_path}/{table_name}.csv", "w+") as stream:
                header = ",".join(column_names)
                stream.write(f"{header}\n")
                for row in rows:
                    data_values = [
                        _format_export(row.get(column_name), column_name)
                        for column_name in column_names
                    ]
                    data_row = ",".join(data_values)
                    stream.write(f"{data_row}\n")


def import_to_sql(
    model_path: str, schema: str = "development", ro_users=None, rw_users=None
):
    """
    Import the CSV files of the model from the repo to the SQL DB in the specified schema.
    Raises:
        ValueError if a duplicate record or referential integrity error is found exporting to SQL.
    """

    connection = get_connection(schema)
    with connection:
        table_names = _get_list_of_tables_from_csv(model_path)
        for table_name in table_names:
            _drop_table(connection, table_name)
            _create_table(connection, model_path, table_name)
            _populate_table(connection, model_path, table_name)
        if ro_users:
            _grant_rights(connection, schema, ro_users, rw_users)
        _add_referential_constraints(connection, schema, table_names)


def _drop_table(connection, table_name):
    """Drop the table in the schema of the connection."""

    try:
        sql = f"DROP TABLE IF EXISTS {table_name} CASCADE;"
        _execute_sql(connection, sql)
    except Exception as e:
        raise ValueError(
            f"Unable to drop table '{table_name}' while exporting to SQL."
        ) from e


def _create_table(connection, model_path, table_name):
    """Create a new table in the SQL database in the schema of the connection with the table name."""

    try:
        column_names = _get_columns_of_csv(model_path, table_name)
        primary_key = column_names[0]
        column_def = []
        for column_name in column_names:
            column_size = (
                "1500"
                if column_name
                in [
                    "description",
                    "notes",
                    "title",
                    "path",
                    "documentation_notes",
                    "level_description",
                ]
                else "100"
            )
            data_type = "date" if "date" in column_name else "json" if column_name in ["shape", "latlng_bounds", "origin"] else f"varchar({column_size})"
            column_def.append(f"{column_name} {data_type}")
        column_def.append(f"PRIMARY KEY ({primary_key})")
        column_def_list = ", ".join(column_def)
        sql = f"CREATE TABLE {table_name} ({column_def_list});"
        _execute_sql(connection, sql)
    except Exception as e:
        raise ValueError(
            f"Unable to create table '{table_name}' while exporting to SQL."
        ) from e


def _populate_table(connection, model_path, table_name):
    """Populate the rows in the SQL table using data from the model CSV file."""

    column_names = _get_columns_of_csv(model_path, table_name)
    rows = _get_row_values_from_csv(model_path, table_name)
    column_name_list = ", ".join(column_names)
    for row in rows:
        parameter_values_placeholders = ",".join(["%s" for column_name in column_names])
        data_values = [
            _format_insert(row.get(column_name), column_name)
            for column_name in column_names
        ]
        key = data_values[0]
        sql = f"INSERT INTO {table_name} ({column_name_list}) VALUES ({parameter_values_placeholders})"
        try:
            _execute_sql(connection, sql, tuple(data_values))
        except psycopg.errors.UniqueViolation as uv:
            raise ValueError(
                f"Duplicate key '{key}' while inserting into table '{table_name}'"
            ) from uv
        except Exception as e:
            raise ValueError(
                f"Unable to insert data catalog row '{key}' into table '{table_name}' while exporting to SQL."
            ) from e


def _format_export(value, column_name):
    """Format a value for exporting and writing into a DB column to csv."""
    if "date" in column_name:
        if value and isinstance(value, datetime.date):
            result = value.strftime("%Y-%m-%d")
        else:
            result = ""
    elif column_name in ["shape", "latlng_bounds", "origin"]:
        result = f'"{value}"' if value else ""
    else:
        result = f'"{value}"'
    return result


def _format_insert(value, column_name):
    """Format a value for inserting into a DB column with the column_name."""
    if "date" in column_name:
        try:
            result = datetime.datetime.strptime(value, "%Y-%m-%d").strftime("%Y-%m-%d")
        except:
            try:
                result = datetime.datetime.strptime(value, "%m-%d-%Y").strftime(
                    "%Y-%m-%d"
                )
            except:
                try:
                    result = datetime.datetime.strptime(value, "%m-%d-%y").strftime(
                        "%Y-%m-%d"
                    )
                except:
                    try:
                        result = datetime.datetime.strptime(value, "%m/%d/%y").strftime(
                            "%Y-%m-%d"
                        )
                    except:
                        try:
                            result = datetime.datetime.strptime(
                                value, "%m/%d/%Y"
                            ).strftime("%Y-%m-%d")
                        except:
                            result = None
    elif column_name in ["shape", "latlng_bounds", "origin"]:
        result = value if value and len(value) > 0 else None
    else:
        result = value
    return result


def _grant_rights(connection, schema, ro_users, rw_users):
    """
    Grant rights to other users to the tables in the schema.

    Grant select only rights to ro_users and grant insert, delete, update rights to rw_users;
    """

    for db_user in ro_users:
        sql = f'GRANT SELECT ON ALL TABLES IN SCHEMA {schema} to "{db_user}"'
        _execute_sql(connection, sql)
        sql = f'GRANT USAGE ON SCHEMA {schema} to "{db_user}"'
        _execute_sql(connection, sql)
    for db_user in rw_users:
        sql = f'GRANT TRUNCATE, DELETE, TRIGGER, UPDATE, REFERENCES, SELECT, INSERT  ON ALL TABLES IN SCHEMA {schema} to "{db_user}"'
        _execute_sql(connection, sql)
        sql = f'GRANT ALL ON SCHEMA {schema} to "{db_user}"'
        _execute_sql(connection, sql)


def _add_referential_constraints(connection, schema, table_names):
    """Add referential integrity constraints to tables in the database of the connection."""

    # Construct map of foreign keys for each table
    foreign_key_map = {}
    for table_name in table_names:
        sql = f"SELECT column_name from information_schema.columns where table_schema='{schema}' and table_name='{table_name}'"
        column_names = [row.get("column_name") for row in _sql_query(connection, sql)]
        for column_name in column_names:
            if column_name in table_names:
                foreign_keys = foreign_key_map.get(table_name, [])
                foreign_keys.append(column_name)
                foreign_key_map[table_name] = foreign_keys

    # Check for any referential integrity constraints now
    verify_referential_integrity(connection, foreign_key_map)

    # Add foreign key constraints since we now know no violations exists
    for table_name in table_names:
        foreign_key_columns = foreign_key_map.get(table_name)
        if foreign_key_columns:
            for foreign_key_column in foreign_key_columns:
                constraint_name = f"{table_name}_{foreign_key_column}"
                sql = f"ALTER TABLE {table_name} DROP CONSTRAINT IF EXISTS {constraint_name};"
                _execute_sql(connection, sql)
                sql = f"ALTER TABLE {table_name} ADD CONSTRAINT {constraint_name} FOREIGN KEY ({foreign_key_column}) REFERENCES {foreign_key_column} (id);"
                _execute_sql(connection, sql)


def verify_referential_integrity(connection, foreign_key_map):
    """
    Verify foreign key relationships and display all violations of rows in the tables.
    Raises:
        ValueError:     if any violations are found.
    """

    found_error = False
    table_names = foreign_key_map.keys()
    for table_name in table_names:
        foreign_key_columns = foreign_key_map.get(table_name)
        for foreign_key_column in foreign_key_columns:
            # Use a SQL query to find foreign key violations for the foreign key column of a table
            sql = f"SELECT {table_name}.id, {table_name}.{foreign_key_column} from {table_name} left outer join {foreign_key_column} on {table_name}.{foreign_key_column} = {foreign_key_column}.id where {foreign_key_column}.id is null;"
            error_rows = _sql_query(connection, sql)
            for error_row in error_rows:
                row_id = error_row.get("id")
                invalid_value = error_row.get(foreign_key_column)
                print(
                    f"Invalid value '{invalid_value}' in column '{foreign_key_column}' in row '{row_id}' of table '{table_name}."
                )
                found_error = True
    if found_error:
        raise ValueError("Failed validation check")


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


def _execute_sql(connection, sql, data_values=None):
    """Execute a SQL statement on the database connection"""

    cur = connection.cursor()
    if data_values:
        cur.execute(sql, data_values)
    else:
        cur.execute(sql)


def drop_all_tables(model_path, schema):
    """Drop all the tables in the schema associated with a model CSV file."""

    connection = get_connection(schema)
    with connection:
        table_names = _get_list_of_tables_from_csv(model_path)
        for table_name in table_names:
            # Drop table before creating new table
            try:
                sql = f"DROP TABLE IF EXISTS {table_name} CASCADE;"
                _execute_sql(connection, sql)
            except Exception as e:
                raise ValueError(
                    f"Unable to drop table '{table_name}' while exporting to SQL."
                ) from e


def get_connection(schema: str) -> psycopg.connection:
    """Get the database connection of the data catalog database"""

    try:
        params = get_db_configuration(schema)
        params["autocommit"] = True
        params["options"] = f"-c search_path={schema}"
        params["row_factory"] = dict_row
        connection = psycopg.connect(**params)
        return connection
    except Exception as e:
        raise ValueError("Unable to connect to data_catalog database.") from e


def get_db_configuration(schema: str):
    """Read the database configuration options from a configuration file in protected user home directory."""

    result = {}
    if schema == "public":
        config_file = os.path.expanduser("~/.data_catalog/db_credentials_public")
    else:
        config_file = os.path.expanduser("~/.data_catalog/db_credentials_development")

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


def _get_list_of_tables(connection, schema) -> List[str]:
    """Get the list of table names in the data catalog model."""

    result = []
    # Get list of tables from database
    sql = f"select table_name from information_schema.tables where table_schema = '{schema}'"
    result = [row.get("table_name") for row in _sql_query(connection, sql)]

    if len(result) == 0:
        raise ValueError("Unable to get list of tables")
    return result


def _get_list_of_tables_from_csv(model_path) -> List[str]:
    """Get the list of table names in the data catalog model."""

    result = []
    if os.path.exists(f"{model_path}/data_catalog_entry.csv"):
        # Get list of tables from csv file of model
        for file_name in os.listdir(model_path):
            table_name = file_name.replace(".csv", "")
            table_name = table_name.strip()
            result.append(table_name)

    if len(result) == 0:
        raise ValueError("Unable to get list of tables")
    return result


def _get_columns_of_table(connection, schema, table_name) -> List[str]:
    """Get the names of the columns in the table from the csv file header."""

    column_names = []
    sql = f"SELECT column_name from information_schema.columns where table_schema='{schema}' and table_name='{table_name}'"
    column_names = [row.get("column_name") for row in _sql_query(connection, sql)]

    return column_names


def _get_columns_of_csv(model_path, table_name) -> List[str]:
    """Get the names of the columns in the table from the csv file header."""

    column_names = []
    csv_file = f"{model_path}/{table_name}.csv"
    if os.path.exists(csv_file):
        with open(csv_file, "r") as csv_file:
            rows = csv.reader(csv_file, delimiter=",")
            column_names = list(rows)[0]

    return column_names


def _get_row_values_from_csv(model_path, table_name) -> List[dict]:
    """Get the values of all the rows in the table as a list of dict from the csv file."""

    result = []
    csv_file = f"{model_path}/{table_name}.csv"
    if os.path.exists(csv_file):
        with open(csv_file, "r") as csv_file:
            column_names = _get_columns_of_csv(model_path, table_name)
            rows = csv.reader(csv_file, delimiter=",")
            for row_count, row in enumerate(list(rows)):
                if row_count > 0:
                    entry = {}
                    for index, column_name in enumerate(column_names):
                        try:
                            column_value = row[index]
                        except Exception as e:
                            raise ValueError(
                                f"Unable to load row '{row_count}' column '{column_name}' from table '{table_name}.csv."
                            ) from e
                        entry[column_name] = column_value
                    result.append(entry)
    return result


def _get_row_values_from_table(connection, table_name) -> List[dict]:
    """Get the values of all the rows in the table as a list of dict from the csv file."""

    result = []
    sql = f"SELECT * FROM {table_name}"
    result = _sql_query(connection, sql)
    return result


def _add_current_date_version(connection):
    """Add a entry to the version table with the current date if such a version does not exist yet."""

    current_date = datetime.datetime.now().strftime("%Y-%m-%d")
    sql = f"SELECT id from version WHERE id='{current_date}'"
    rows = _sql_query(connection, sql)
    if len(rows) == 0:
        # Insert new version with current date into version table before exporting to csv
        sql = f"INSERT INTO version (id,modified_by,comments) VALUES ('{current_date}', 'jenkins job', 'exported by jenkins job')"
        _execute_sql(connection, sql)


if __name__ == "__main__":
    main()
