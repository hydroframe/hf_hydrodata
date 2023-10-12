"""
Unit test for the data_model_access module.
"""
# pylint: disable=E0401,C0413
import sys
import os
import json

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../src")))
import hf_hydrodata.data_model_access


def test_load_model():
    """Test loading of CSV files using load_model."""

    data_model = hf_hydrodata.data_model_access.load_data_model(False)

    assert len(data_model.table_names) >= 14
    assert len(data_model.get_table("grid").row_ids) >= 6
    assert data_model.get_table("grid").get_row("conus2").get_value("shape")[1] == 3256
    assert data_model.get_table("grid").get_row("conus2")["shape"][1] == 3256


def test_unit_types():
    """Test loading unit types."""

    data_model = hf_hydrodata.data_model_access.load_data_model(False)
    units_table = data_model.get_table("units")
    assert units_table.get_row("m3/h")["unit_type"] == "volume_flux"

def test_export_model():
    """Test exporting the data model to a dict."""

    data_model = hf_hydrodata.data_model_access.load_data_model(False)
    model_dict = data_model.export_to_dict()
    entries = model_dict.get("data_catalog_entry")
    assert len(entries) > 300

def test_import_model():
    """Test importing the data model from a dict."""

    data_model = hf_hydrodata.data_model_access.load_data_model()
    model_dict = data_model.export_to_dict()
    data_model.import_from_dict(model_dict)
    data_catalog_entry_table = data_model.get_table("data_catalog_entry")
    assert len(data_catalog_entry_table.row_ids) > 300
    row = data_catalog_entry_table.rows["11"]
    assert row["dataset_type"] == "parflow"



