"""
Unit test for the data_catalog.py module
"""

# pylint: disable=C0301,C0103,W0632,W0702,W0101,C0302,W0105,E0401,C0413,R0903,W0613,R0912
import sys
import os
import tempfile
import pytest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../src")))

import hf_hydrodata.generate_hydrodata_catalog_yaml
import hf_hydrodata as hf
import hf_hydrodata.gridded as gr


@pytest.fixture(autouse=True)
def patch_api(mocker):
    """Mock api call to load model from API. This allows tests to work with model from the git repo."""

    def mock_return_model(option):
        return None

    mocker.patch(
        "hf_hydrodata.data_model_access._load_model_from_api",
        side_effect=mock_return_model,
    )


def test_get_citations():
    """Test get_citation"""

    result = hf.get_citations(dataset="conus1_domain")
    #assert "10.5194" in result
    #result = hf.get_citations("conus1_domain")
    #assert "10.5194" in result
    #result = hf.get_citations("CW3E")



def test_get_entries():
    """Test getting data_catalog_entries using filters."""

    gr.HYDRODATA = "/hydrodata"
    rows = hf.get_catalog_entries(dataset="NLDAS2", file_type="pfb", period="daily")
    assert len(rows) == 10
    # TODO NOT FOR SQL assert len(rows[0].column_names()) >= 25
    for index in range(0, len(rows)):
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
    """Test register and and get and email pin stored in users home directory."""

    hf.register_api_pin("dummy@email.com", "0000")
    email, pin = hf.get_registered_api_pin()
    assert pin == "0000"
    assert email == "dummy@email.com"


def test_generate_hydrodata_catalog_yaml():
    """Test generate_hydrodata_catalog_yaml"""

    with tempfile.TemporaryDirectory() as tempdirname:
        hf.load_data_model(True)
        output_file = os.path.join(tempdirname, "foo.yaml")
        hf_hydrodata.generate_hydrodata_catalog_yaml.generate_yaml(output_file)
        assert os.path.exists(output_file)


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
        dataset_version="0.8",
    )
    assert row["id"] == "529"


def test_dataset_version_default():
    """Test the default dataset_version is the highest version number if not specified"""

    row = hf.get_catalog_entry(
        dataset="CW3E", period="hourly", variable="precipitation"
    )
    assert row["id"] == "167"

def test_catalog_preference():
    """Test get_catalog_entry() preference algorithm."""

    option = {
        "dataset": "CW3E",
        "variable": "air_temp",
        "temporal_resolution": "daily",
        "start_time": "2001-01-01"
    }
    entry = hf.get_catalog_entry(option)
    assert entry["aggregation"] == "mean"
    assert entry["dataset_version"] == "1.0" or entry["dataset_version"] == ""


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
