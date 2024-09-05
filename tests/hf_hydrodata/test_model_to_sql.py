"""
Unit tests for model_to_sql module.
"""

# pylint: disable=E0401, C0413,W1514,W0212
import sys
import os
import tempfile
import datetime
import pytest
import psycopg

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../utils")))
import model_to_sql


def test_import_and_export(mocker):
    """Test the ability to import and export model csv files into SQL"""

    # Skip this test if the developer does not have ./data_catalog DB credentials
    if not os.path.exists(os.path.expanduser("~/.data_catalog")):
        return

    # Verify we can export the schema into csv files in an empty directory correctly
    with tempfile.TemporaryDirectory() as temp_dir:
        assert not os.path.exists(f"{temp_dir}/version.csv")
        assert not os.path.exists(f"{temp_dir}/data_catalog_entry.csv")
        assert not os.path.exists(f"{temp_dir}/dataset.csv")
        testargs = ["model_to_sql", "export", "development", temp_dir]
        mocker.patch.object(sys, "argv", testargs)
        rc = model_to_sql.main()
        assert rc == 0, "Unexpected error occurred in import command."
        assert os.path.exists(f"{temp_dir}/version.csv")
        assert os.path.exists(f"{temp_dir}/data_catalog_entry.csv")
        assert os.path.exists(f"{temp_dir}/dataset.csv")
        with open(f"{temp_dir}/version.csv", "r") as fp:
            contents = fp.read()
            current_date = datetime.datetime.now().strftime("%Y-%m-%d")
            assert current_date in contents

        # Verify we can drop all files in schema
        model_path = temp_dir
        testargs = ["model_to_sql", "drop", "public_test", model_path]
        mocker.patch.object(sys, "argv", testargs)
        rc = model_to_sql.main()
        assert rc == 0, "Unexpected error occurred in import command."
        connection = model_to_sql.get_connection("public_test")
        with pytest.raises(psycopg.errors.UndefinedTable):
            model_to_sql._sql_query(connection, "select * from grid")

        # Then verify import csv files into a SQL DB
        testargs = ["model_to_sql", "import", "public_test", model_path]
        mocker.patch.object(sys, "argv", testargs)
        rc = model_to_sql.main()
        assert rc == 0, "Unexpected error occurred in import command."
        connection = model_to_sql.get_connection("public_test")
        rows = model_to_sql._sql_query(
            connection, "select * from grid where id='conus2'"
        )
        assert len(rows) == 1
