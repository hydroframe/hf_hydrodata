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
        "hf_hydrodata.data_model_access._load_model_from_api", side_effect=mock_return_model
    )

def test_get_citations():
    """Test get_citation"""

    result = hf.get_citations(dataset="conus1_domain")
    assert "10.5194" in result
    result = hf.get_citations("conus1_domain")
    assert "10.5194" in result
    result = hf.get_citations("CW3E")
    print(result)
    # result = hf.get_citations("CW3E")
    # print(result)


def test_get_entries():
    """Test getting data_catalog_entries using filters."""

    gr.HYDRODATA = "/hydrodata"
    rows = hf.get_catalog_entries(dataset="NLDAS2", file_type="pfb", period="daily")
    assert len(rows) == 10
    assert len(rows[0].column_names()) >= 25
    assert rows[4].get_value("variable") == "air_temp"
    assert rows[4].get_value("variable_type") == "atmospheric"
    assert rows[4].get_value("dataset_type") == "forcing"
    assert rows[4].get_value("aggregation") == "min"
    assert rows[4].get_value("grid") == "conus1"
    assert (
        rows[4].get_value("path")
        == "/hydrodata/forcing/processed_data/CONUS1/NLDAS2/daily/WY{wy}/NLDAS.Temp.daily.min.{wy_daynum:03d}.pfb"
    )


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


def test_get_table_rows():
    """Test getting rows from any table in the data model."""

    gr.HYDRODATA = "/hydrodata"
    rows = hf.get_table_rows("variable", variable_type="atmospheric")
    assert len(rows) >= 8

    rows = hf.get_table_rows("variable", variable_type="land_use")
    assert len(rows) == 0


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

    row = hf.get_catalog_entry(dataset="CW3E", period="hourly", variable="precipitation")
    hf_hydrodata.data_model_access.DATA_MODEL_CACHE = None
    assert row["id"] == "167"
