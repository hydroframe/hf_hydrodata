"""
Unit tests for public_release.py
"""

# pylint: disable=W0212,E0401,W0718,C0413

import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
import utils.public_release


def test_public_release():
    """Test the public_release program to copy development to public_test schema."""

    try:
        utils.model_to_sql.get_db_configuration("development")
    except Exception:
        # If this server has no DB configuration do not run this test
        return

    # First delete an entry in data_catalog_entry in the public_test schema
    connection = utils.public_release._get_connection("public_test")
    utils.public_release._execute_sql(
        connection, "delete from public_test.data_catalog_entry where id='213'"
    )
    rows = utils.public_release._sql_query(
        connection, "select * from public_test.data_catalog_entry where id='213'"
    )
    assert len(rows) == 0

    # Then run the publish schema program
    utils.public_release.publish_schema("development", "public_test")

    # Then verify that the entry is restored
    connection = utils.public_release._get_connection("public_test")
    rows = utils.public_release._sql_query(
        connection, "select * from public_test.data_catalog_entry where id='213'"
    )
    assert len(rows) == 1
    assert rows[0]["variable"] == "soil_moisture"
