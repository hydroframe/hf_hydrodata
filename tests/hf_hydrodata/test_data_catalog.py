"""
Unit test for the data_catalog.py module
"""

# pylint: disable=C0301,C0103,W0632,W0702,W0101,C0302,W0105,E0401,C0413,R0903,W0613,R0912
import sys
import os
import pytest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../src")))

import hf_hydrodata as hf
import hf_hydrodata.gridded as gr


def test_get_citations():
    """Test get_citation"""

    result = hf.get_citations(dataset="conus1_domain")
    assert "gmd-8-923-2015" in result
    assert "10.5194" in result
    result = hf.get_citations("conus1_domain")
    assert "10.5194" in result
    result = hf.get_citations("CW3E")


def test_get_entries():
    """Test getting data_catalog_entries using filters."""

    gr.HYDRODATA = "/hydrodata"
    rows = hf.get_catalog_entries(dataset="NLDAS2", file_type="pfb", period="daily")
    assert len(rows) == 10
    for index, _ in enumerate(rows):
        row = rows[index]
        if row["variable"] == "air_temp":
            assert row.get_value("variable") == "air_temp"
            assert row.get_value("variable_type") == "atmospheric"
            assert row.get_value("dataset_type") == "forcing"
            assert row.get_value("grid") == "conus1"


def test_get_entry_filter():
    """Test getting single data_catalog_entry using filters."""

    gr.HYDRODATA = "/hydrodata"
    row = hf.get_catalog_entry(
        dataset="NLDAS2", file_type="pfb", period="daily", variable="precipitation"
    )
    assert row.get_value("id") == "130"

    with pytest.raises(ValueError):
        hf.get_catalog_entry(dataset="NLDAS2", file_type="pfb", period="daily")

    entry = hf.get_catalog_entry(
        dataset="NLDAS2xxx",
        file_type="pfb",
        period="daily",
        variable="precipitation",
    )
    assert entry is None


def test_get_table_row():
    """Test getting a single row from a table."""

    gr.HYDRODATA = "/hydrodata"
    entry = hf.get_table_row("grid", id="conus1")
    assert entry is not None

    entry = hf.get_table_row("grid", id="conus5")
    assert entry is None

    with pytest.raises(ValueError):
        entry = hf.get_table_row("variable_type", variable_type="atomspheric")


def test_register_api():
    """Test register and get an email pin stored in users home directory."""

    # Backup previous existing pin.json file so test is not destructive
    pin_file = os.path.expanduser("~/.hydrodata/pin.json")
    pin_file_backup = os.path.expanduser("~/.hydrodata/pin.json.backup")
    if os.path.exists(pin_file_backup):
        os.remove(pin_file_backup)
    if os.path.exists(pin_file):
        os.rename(pin_file, pin_file_backup)

    # Register a pin and verify it was registered
    hf.register_api_pin("dummy@email.com", "0000")
    email, pin = hf.get_registered_api_pin()
    assert pin == "0000"
    assert email == "dummy@email.com"

    # Put back pin file to original state
    os.remove(pin_file)
    if os.path.exists(pin_file_backup):
        os.rename(pin_file_backup, pin_file)


def test_dataset_version():
    """Test reading catalog entries with dataset_versions"""

    row = hf.get_catalog_entry(
        dataset="CW3E", period="hourly", variable="precipitation", dataset_version="0.9"
    )
    assert row["id"] == "167"

    row = hf.get_catalog_entry(
        dataset="CW3E",
        period="hourly",
        variable="precipitation",
        dataset_version="1.0",
    )
    assert row["id"] == "537"


def test_dataset_version_default():
    """Test the default dataset_version is the highest version number if not specified"""

    row = hf.get_catalog_entry(
        dataset="CW3E", period="hourly", variable="precipitation"
    )
    assert row["id"] == "537"


def test_catalog_preference_dataset_version():
    """Test get_catalog_entry() preference for dataset version."""

    option = {
        "dataset": "CW3E",
        "variable": "air_temp",
        "temporal_resolution": "hourly",
        "start_time": "2001-01-01",
    }
    entry = hf.get_catalog_entry(option)
    assert entry["aggregation"] == "-"
    assert entry["dataset_version"] == "1.0"

    option = {
        "dataset": "CW3E",
        "variable": "air_temp",
        "temporal_resolution": "hourly",
        "dataset_version": "0.9",
        "start_time": "2001-01-01",
    }
    entry = hf.get_catalog_entry(option)
    assert entry["aggregation"] == "-"
    assert entry["dataset_version"] == "0.9"


def test_catalog_preference_file_type():
    """Test get_catalog_entry() preference for file type."""

    option = {"dataset": "conus1_domain", "variable": "flow_direction"}
    entry = hf.get_catalog_entry(option)
    assert entry["file_type"] == "pfb"

    option = {
        "dataset": "conus1_domain",
        "variable": "flow_direction",
        "file_type": "tiff",
    }
    entry = hf.get_catalog_entry(option)
    assert entry["file_type"] == "tiff"


def test_catalog_preference_aggregation():
    """Test get_catalog_entry() preference for aggregation and dataset_version."""

    option = {
        "dataset": "CW3E",
        "variable": "air_temp",
        "temporal_resolution": "daily",
    }
    entry = hf.get_catalog_entry(option)

    assert entry["aggregation"] == "mean"
    assert entry["dataset_version"] == "1.0"

    option = {
        "dataset": "CW3E",
        "variable": "air_temp",
        "aggregation": "max",
        "temporal_resolution": "daily",
    }
    entry = hf.get_catalog_entry(option)
    assert entry["aggregation"] == "max"
    assert entry["dataset_version"] == "1.0"


def test_get_citations_usgs():
    """Test for get_citations function with return DataFrame."""
    citation = hf.get_citations(dataset="usgs_nwis")

    assert isinstance(citation, str)


def test_get_citations_ameriflux():
    """Test for get_citations function with return DataFrame."""
    citation = hf.get_citations(dataset="ameriflux")

    assert isinstance(citation, str)


def test_get_citations_jasechko():
    """Test citations for jasechko_2024 dataset."""
    t = hf.get_citations("jasechko_2024")
    assert "10.1038/s41586-023-06879-8" in t


def test_get_citations_fan():
    """Test citations for fan_2013 dataset."""
    t = hf.get_citations("fan_2013")
    assert "10.1126/science.1229881" in t


def test_get_citations_fail():
    """Test citations function exception for unknown dataset."""
    with pytest.raises(Exception) as exc:
        hf.get_citations("usgs")
    assert str(exc.value) == "No such dataset 'usgs'"
