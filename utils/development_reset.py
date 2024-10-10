"""
    Program to reset the development schema from the public schema.
    This should be called by a developer running locally from this workspace.

    This resets the data catalog development schema by copying the data from the public
    schema to the development schema. This fully replaces the contents of the development schema
    using the latest data from the public schema. This should be used by a developer prior to
    making manual changes to the development schema to prepare for new changes to the schema.

    If all changes to the public schema were made normally by publishing from the development schema
    then this program is not necessary. However, if manual changes were made to the public schema
    (not recommended) then this program resets the development schema so those changes are not lost.

    The account executing this prograrm must contain DB crendentials in the file:
        ~/.data_catalog/db_credentials_development
"""

# pylint: disable=C0301,W1514

import os
import datetime
import psycopg
from psycopg.rows import dict_row
import public_release


def main():
    """Copy tables from public schema to development schema using DDL file."""
    public_release.publish_schema("public", "development")

if __name__ == "__main__":
    main()
