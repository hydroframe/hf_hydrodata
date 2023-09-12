"""
    Generate the .vst markdown files for the read-the-docs documumenation of data catalog entries.
    Generate this these files into the directory docs/source with the gen_ prefix.
    These will be included bv the hand referenced .vst files in the same directory.
"""
# pylint: disable=E0401,C0413
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../src")))
from hf_hydrodata.data_model_access import load_data_model


def main():
    """
    Main function for the docs generator.
    """

    generate_datasets()


def generate_datasets():
    """Generate the documentation of datasets (not used now)"""
    data_model = load_data_model()
    data_catalog_entry_table = data_model.get_table("data_catalog_entry")
    dataset_ids = set()
    for row_id in data_catalog_entry_table.row_ids:
        row = data_catalog_entry_table.get_row(row_id)
        security_level = row["security_level"]
        if security_level >= "2":
            dataset_ids.add(str(row["dataset"]))


if __name__ == "__main__":
    main()
