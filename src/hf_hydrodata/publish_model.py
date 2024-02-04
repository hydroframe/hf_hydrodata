"""
    Publish a copy of the data catalog model to /hydrodata/data_catalog.
"""
# pylint: disable=E0401
import os
import shutil
import data_model_access
import sqlite3
import csv

def main():
    """
    Copy publish the model .csv files to /hydrodata/catalog.

    This does nothing if /hydrodata/data_catalog folder does not exist.
    This creates the remote data catalog version folder if it does not exist.
    This copies all the .csv files from the src/hf_hydrodata/model folder
    to the /hydrodata/data_catalog/<version> folder.
    """

    version = data_model_access.REMOTE_DATA_CATALOG_VERSION
    local_dir = "./model"
    dc_dir = "/hydrodata/data_catalog"
    target_dir = f"{dc_dir}/{version}"
    if os.path.exists(dc_dir):
        if not os.path.exists(target_dir):
            os.mkdir(target_dir)
            os.chmod(target_dir, 0o775)
        for f in os.listdir(local_dir):
            if f.endswith(".csv"):
                src = f"{local_dir}/{f}"
                dst = f"{target_dir}/{f}"
                shutil.copyfile(src, dst)
                os.chmod(dst, 0o664)
                print(f"Copied to {dst}")


def publish_to_sqlite():
    # Connect to SQLite database (creates a new one if it doesn't exist)
    db_path = "example.db"
    if os.path.exists(db_path):
        os.remove(db_path)
    conn = sqlite3.connect(db_path)

    # Create a cursor object to execute SQL queries
    cursor = conn.cursor()
    local_dir = "./model"
    for f in os.listdir(local_dir):
        if f.endswith(".csv"):
            table_name = f.replace(".csv", "")
            csv_path = os.path.join(local_dir, f)
            _populate_table(cursor, table_name, csv_path)

    # Commit the changes and close the connection
    conn.commit()
    conn.close()

def _populate_table(cursor, table_name, csv_path):
    with open(csv_path, "r") as csv_stream:
        rows = csv.reader(csv_stream, delimiter=",")
        insert_statement = ""
        for row_count, row in enumerate(list(rows)):
            if row_count == 0:
                column_names = row
                ddl = f"CREATE TABLE IF NOT EXISTS {table_name} ("
                ddl = ddl + f"{column_names[0]} TEXT PRIMARY KEY"
                value_options = "?"
                for i in range(1, len(column_names)):
                    ddl = ddl + f", {column_names[i]} TEXT"
                    value_options = value_options + ", ?"
                ddl = ddl + ");"
                cursor.execute(ddl)
                sql_column_names = ", ".join(column_names)
                insert_statement = f"INSERT INTO {table_name} ({sql_column_names}) VALUES ({value_options});"
            else:
                cursor.execute(insert_statement, row)

if __name__ == "__main__":
    publish_to_sqlite()