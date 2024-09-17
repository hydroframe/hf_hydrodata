"""
Unit test for the gridded module.
"""

# pylint: disable=C0301,C0103,W0632,W0702,W0101,C0302,W0105,E0401,C0413,R0903,W0613,R0912,W0212
import sys
import os
import datetime
import tempfile
import math
from unittest.mock import patch
import xarray as xr
import numpy as np
import pytest
import pytz
import rioxarray

from parflow import read_pfb_sequence

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../src")))

import hf_hydrodata as hf
import hf_hydrodata.gridded as gr

hf.data_model_access.DATA_MODEL_CACHE = None


@pytest.fixture(autouse=True)
def patch_api(mocker):
    """Mock api call to load model from API. This allows tests to work with model from the git repo."""

    def mock_return_model(option):
        return None

    mocker.patch(
        "hf_hydrodata.data_model_access._load_model_from_api",
        side_effect=mock_return_model,
    )


class MockResponse:
    """Mock the flask.request response."""

    def __init__(self):
        self.headers = {}
        self.status_code = 200
        self.content = b'{"email": "dummy@email.com", "jwt_token":"foo"}'
        self.checksum = ""


def mock_requests_get(url, headers=None, timeout=5):
    """Create a mock streaming response."""

    response = MockResponse()
    return response


def test_get_vegp():
    """Test ability to retreive vegp file."""

    gr.HYDRODATA = "/hydrodata"
    hf.get_raw_file(
        filepath="./vegp.dat",
        dataset="conus1_baseline_mod",
        file_type="vegp",
        variable="clm_run",
    )

    assert os.path.exists("./vegp.dat") is True
    os.remove("./vegp.dat")

    with patch(
        "requests.get",
        new=mock_requests_get,
    ):
        gr.HYDRODATA = "/empty"
        hf.get_raw_file(
            filepath="./vegp.dat",
            dataset="conus1_baseline_mod",
            file_type="vegp",
            variable="clm_run",
        )

        assert os.path.exists("./vegp.dat") is True
        os.remove("./vegp.dat")


def test_get_drv_clm():
    """Test ability to retreive drv_clm file."""

    gr.HYDRODATA = "/hydrodata"
    hf.get_raw_file(
        filepath="./vegp.dat",
        dataset="conus1_baseline_mod",
        file_type="drv_clm",
        variable="clm_run",
    )

    assert os.path.exists("./vegp.dat") is True
    os.remove("./vegp.dat")

    with patch(
        "requests.get",
        new=mock_requests_get,
    ):
        gr.HYDRODATA = "/empty"
        hf.get_raw_file(
            filepath="./vegp.dat",
            dataset="conus1_baseline_mod",
            file_type="vegp",
            variable="clm_run",
        )

        assert os.path.exists("./vegp.dat") is True
        os.remove("./vegp.dat")


def test_start_time_in_get_gridded_data():
    """Test ability to pass start_time in get_gridded_data method."""

    gr.HYDRODATA = "/hydrodata"

    start_time = datetime.datetime.strptime("2005-09-01", "%Y-%m-%d")
    end_time = start_time + datetime.timedelta(hours=96)
    data = gr.get_gridded_data(
        dataset="NLDAS2",
        file_type="pfb",
        period="hourly",
        variable="precipitation",
        start_time=start_time,
        end_time=end_time,
    )
    assert data.shape[0] == 96

    start_time = "2005-09-01"
    data = gr.get_gridded_data(
        dataset="NLDAS2",
        file_type="pfb",
        period="hourly",
        variable="precipitation",
        start_time=start_time,
        end_time=end_time,
    )
    assert data.shape[0] == 96


def test_get_paths_and_metadata():
    """Demonstrate getting water table depth files crossing a water year."""

    gr.HYDRODATA = "/hydrodata"
    options = {
        "variable": "water_table_depth",
        "dataset": "conus1_baseline_mod",
        "grid": "conus1",
        "file_type": "pfb",
        "period": "daily",
        "time": "2005-09-29",
        "start_time": "2005-09-29",
        "end_time": "2005-10-03",
        "level": "4",
    }
    row = hf.get_catalog_entry(options)
    paths = gr.get_file_paths(row, options)

    assert len(paths) == 4  # 4 files between 9/29/2005 and 10/3/2005
    assert row["dataset_var"] == "wtd"
    assert row["units"] == "m"
    assert row["file_type"] == "pfb"
    assert row["id"] == "10"
    assert (
        paths[3]
        == "/hydrodata/PFCLM/CONUS1_baseline/simulations/daily/WY2006/wtd.daily.mean.002.pfb"
    )


def test_paths_hourly_files():
    """Demonstrate getting water table depth files crossing a water year."""

    gr.HYDRODATA = "/hydrodata"
    options = {
        "variable": "pressure_head",
        "dataset": "conus1_baseline_mod",
        "grid": "conus1",
        "period": "hourly",
        "file_type": "pfb",
        "start_time": "2005-09-29",
        "end_time": "2005-10-03",
    }
    row = hf.get_catalog_entry(options)
    paths = gr.get_file_paths(row, options)

    assert len(paths) == 96  # 96 hour files between 9/29/2005 and 10/3/2005
    assert row["dataset_var"] == "Press"
    assert row["units"] == "m"
    assert row["file_type"] == "pfb"
    assert row["id"] == "52"
    assert row["period"] == "hourly"

    assert (
        paths[0]
        == "/hydrodata/PFCLM/CONUS1_baseline/simulations/2005/raw_outputs/pressure/CONUS.2005.out.press.08713.pfb"
    )
    assert (
        paths[95]
        == "/hydrodata/PFCLM/CONUS1_baseline/simulations/2006/raw_outputs/pressure/CONUS.2006.out.press.00048.pfb"
    )


def test_files_exist():
    """Test that the files found in every data_catalog_entry exist."""

    gr.HYDRODATA = "/hydrodata"
    if not os.path.exists("/hydrodata"):
        # Just skip test if this is run on a machine without /hydrodata access
        return

    rows = hf.get_catalog_entries()

    bad_row = False
    for row in rows:
        site_id = ""
        dataset = row["dataset"]
        site_type = row["site_type"]
        if dataset == "conus1_baseline_85":
            start_time = "1984-11-01"
        elif dataset == "conus1_baseline_mod":
            start_time = "2005-09-01"
        elif dataset == "NLDAS2":
            start_time = "2005-09-01"
        elif dataset == "conus1_domain":
            start_time = "2005-09-01"
        elif dataset == "nasa_smap":
            start_time = "2023-01-01"
        elif dataset == "noaa":
            start_time = "2023-01-01"
        elif site_type == "streamflow":
            site_id = "05490600"
        elif site_type == "groundwater":
            site_id = "351058106391002"
        elif dataset == "snotel":
            site_id = "348:UT:SNTL"
            start_time = "2002-01-01"
        elif dataset == "observations":
            site_id = "348:UT:SNTL"
            start_time = "2002-01-01"
        elif dataset == "scan":
            site_id = "2109:MS:SCAN"
        elif dataset == "ameriflux":
            site_id = "US-HB2"
        elif dataset == "obs_anomolies":
            start_time = "2002-01-01"
        elif dataset == "conus1_current_conditions":
            start_time = "2023-01-01"
        elif dataset == "NLDAS2_85":
            start_time = "2005-10-01"
            
        row_id = row["id"]
        if not row_id in [
            "206",
            "207",
            "208",
            "209",
            "210",
            "213",
            "253",
            "254",
            "522",
            "525",
            "528",
        ]:
            paths = gr.get_file_paths(
                row,
                start_time=start_time,
                level="4",
                site_type=site_type,
                site_id=site_id,
            )
            for path in paths:
                if not os.path.exists(path):
                    print(
                        f"Dataset '{dataset}' path not exist '{path}' for template '{row_id}'"
                    )
                    bad_row = True
                    break
    assert bad_row is False


def test_read_data_all_entries():
    """Test that can read data for all entries"""

    gr.HYDRODATA = "/hydrodata"
    if not os.path.exists("/hydrodata"):
        # Just skip test if this is run on a machine without /hydrodata access
        return

    # Skip this test for now because it takes more than 60 seconds to run (we will run it manually for now)
    return

    rows = hf.get_catalog_entries()

    bad_row = False
    for row in rows:
        dataset = row["dataset"]
        if dataset == "conus1_baseline_85":
            start_time = "1984-11-01"
        elif dataset == "conus1_baseline_mod":
            start_time = "2005-09-01"
        elif dataset == "NLDAS2":
            start_time = "2005-09-01"
        elif dataset == "NLDAS2_85":
            start_time = "1984-11-01"
        elif dataset == "conus1_domain":
            start_time = "2005-09-01"
        elif dataset == "CW3E":
            start_time = "2005-09-02"

        try:
            entry_id = row["id"]
            file_type = row["file_type"]
            # Zarr not supported yet
            # vegm is supported, but tested with a seperate unit test
            # vegm, pftxt, drv_clm, pftcl are parflow configuration files and not read as data
            # ID 1, 68, 69 are pfmetadata files referencing variables in model that are not in the pfmetadata file
            if file_type not in [
                "zarr",
                "pftxt",
                "drv_clm",
                "vegp",
                "vegm",
                "pftcl",
                "sql",
            ] and entry_id not in ["1", "68", "69"]:
                data = gr.get_ndarray(row, start_time=start_time, level=4)
                if data is None:
                    bad_row = True
        except:
            print(f"No data for {entry_id}")
            bad_row = True
    assert bad_row is False


def test_subsetting():
    """Test subsetting"""

    gr.HYDRODATA = "/hydrodata"
    if not os.path.exists("/hydrodata"):
        # Just skip test if this is run on a machine without /hydrodata access
        return
    options = {
        "variable": "pressure_head",
        "dataset": "conus1_baseline_mod",
        "grid": "conus1",
        "file_type": "pfb",
        "period": "hourly",
        "start_time": "2005-09-29",
        "end_time": "2005-10-03",
    }
    row = hf.get_catalog_entry(options)
    paths = gr.get_file_paths(row, options)

    # Read the data from the list of pfb files (5 days crossing a water year)
    # Subset the pfb files by x,y space constraints to an area of interest
    boundary_constraints = {
        "x": {"start": int(1076), "stop": int(1124)},
        "y": {"start": int(720), "stop": int(739)},
        "z": {"start": 0, "stop": 0},
    }
    data = read_pfb_sequence(paths, boundary_constraints)

    assert data.shape[0] == 96  # 96 hours
    assert data.shape[1] == 5  # 5 layers deep
    assert data.shape[2] == 19  # 19 y points
    assert data.shape[3] == 48  # 48 x points


def test_get_gridded_data_pfb_precipitation():
    """Test get_gridded_data of a NLDAS2 pfb precipitation variable sliced by bounds."""

    gr.HYDRODATA = "/hydrodata"
    if not os.path.exists("/hydrodata"):
        # Just skip test if this is run on a machine without /hydrodata access
        return

    bounds = [200, 200, 300, 250]
    latlng_bounds = [
        33.79169338210987,
        -114.34357566786298,
        34.41096361516614,
        -113.38485056306695,
    ]

    # Get a daily precipitation entry from data catalog
    entry = hf.get_catalog_entry(
        dataset="NLDAS2", file_type="pfb", period="daily", variable="precipitation"
    )

    # The data result has 4 days in the time dimension because end time is exclusive
    data = gr.get_gridded_data(
        dataset="NLDAS2",
        file_type="pfb",
        period="daily",
        variable="precipitation",
        start_time="2005-09-29",
        end_time="2005-10-03",
        grid_bounds=bounds,
    )
    assert data.shape == (4, 50, 100)

    # Select a single time value with a bounds
    data = gr.get_gridded_data(
        dataset="NLDAS2",
        file_type="pfb",
        period="daily",
        variable="precipitation",
        start_time="2005-09-29",
        grid_bounds=bounds,
    )
    assert data.shape == (1, 50, 100)

    # The data result has 4 days in the time dimension because end time is exclusive
    data = gr.get_gridded_data(
        dataset="NLDAS2",
        file_type="pfb",
        period="daily",
        variable="precipitation",
        start_time="2005-09-29",
        end_time="2005-10-03",
        latlng_bounds=latlng_bounds,
    )
    assert data.shape == (4, 50, 100)

    """
    gr.HYDRODATA = "/empty"
    # The data result has 4 days in the time dimension because end time is exclusive
    data = gr.get_gridded_data(
        dataset="NLDAS2", file_type="pfb", period="daily", variable="precipitation", start_time="2005-09-29", end_time="2005-10-03", grid_bounds=bounds
    )
    assert data.shape == (4, 50, 100)

    data = gr.get_gridded_data(
        dataset="NLDAS2", file_type="pfb", period="daily", variable="precipitation", start_time="2005-09-29", end_time="2005-10-03", latlng_bounds=latlng_bounds
    )
    assert data.shape == (4, 50, 100)

    # Select a single time value with a bounds
    data = gr.get_gridded_data(
        dataset="NLDAS2", file_type="pfb", period="daily", variable="precipitation", start_time="2005-09-29", grid_bounds=bounds
    )
    assert data.shape == (1, 50, 100)
    """


def test_get_gridded_data_pfb_precipitation_string_input():
    """Test get_gridded_data of a NLDAS2 pfb precipitation variable sliced by bounds."""

    gr.HYDRODATA = "/hydrodata"
    if not os.path.exists("/hydrodata"):
        # Just skip test if this is run on a machine without /hydrodata access
        return
    bounds = "[200, 200, 300, 250]"
    latlng_bounds = "[33.79169338210987, -114.34357566786298, 34.41096361516614, -113.38485056306695]"

    # The data result has 4 days in the time dimension because end time is exclusive
    data = gr.get_gridded_data(
        dataset="NLDAS2",
        file_type="pfb",
        period="daily",
        variable="precipitation",
        start_time="2005-09-29",
        end_time="2005-10-03",
        grid_bounds=bounds,
    )
    assert data.shape == (4, 50, 100)

    # The data result has 4 days in the time dimension because end time is exclusive
    data = gr.get_gridded_data(
        dataset="NLDAS2",
        file_type="pfb",
        period="daily",
        variable="precipitation",
        start_time="2005-09-29",
        end_time="2005-10-03",
        latlng_bounds=latlng_bounds,
    )
    assert data.shape == (4, 50, 100)

    # Select a single time value with a bounds
    data = gr.get_gridded_data(
        dataset="NLDAS2",
        file_type="pfb",
        period="daily",
        variable="precipitation",
        start_time="2005-09-29",
        grid_bounds=bounds,
    )
    assert data.shape == (1, 50, 100)

    """
    gr.HYDRODATA = "/empty"
    # The data result has 4 days in the time dimension because end time is exclusive
    
    data = gr.get_gridded_data(
        dataset="NLDAS2", file_type="pfb", period="daily", variable="precipitation", start_time="2005-09-29", end_time="2005-10-03", grid_bounds=bounds
    )
    assert data.shape == (4, 50, 100)

    # Select a single time value with a bounds
    data = gr.get_gridded_data(
        dataset="NLDAS2", file_type="pfb", period="daily", variable="precipitation", start_time="2005-09-29", grid_bounds=bounds
    )
    assert data.shape == (1, 50, 100)
    
    data = gr.get_gridded_data(
        dataset="NLDAS2", file_type="pfb", period="daily", variable="precipitation", start_time="2005-09-29", latlng_bounds=latlng_bounds
    )
    assert data.shape == (1, 50, 100)
    """


def test_get_nldas2_wind_pfb_hourly():
    """Test get_gridded_data of a NLDAS2 pfb wind variable sliced by bounds."""

    gr.HYDRODATA = "/hydrodata"
    if not os.path.exists("/hydrodata"):
        # Just skip test if this is run on a machine without /hydrodata access
        return
    bounds = [200, 200, 300, 250]

    entry = hf.get_catalog_entry(
        dataset="NLDAS2", file_type="pfb", period="hourly", variable="east_windspeed"
    )

    # The result has 5 days of 24 hours in the time dimension and sliced to x,y shape 100x50 at origin 200, 200 in the conus1 grid.
    data = gr.get_gridded_data(
        dataset="NLDAS2",
        file_type="pfb",
        period="hourly",
        variable="east_windspeed",
        start_time="2005-09-29",
        end_time="2005-10-04",
        grid_bounds=bounds,
    )
    assert data.shape == (120, 50, 100)

    # This result has only one day values but is hourly so will have one 24 hour time dimension in the result.
    data = gr.get_gridded_data(
        dataset="NLDAS2",
        file_type="pfb",
        period="hourly",
        variable="east_windspeed",
        start_time="2005-09-29",
        grid_bounds=bounds,
    )
    assert data.shape == (1, 50, 100)


def test_gridded_data_no_grid_bounds():
    """Test get ndarray without grid_bounds parameters."""

    gr.HYDRODATA = "/hydrodata"
    if not os.path.exists("/hydrodata"):
        # Just skip test if this is run on a machine without /hydrodata access
        return

    entry = hf.get_catalog_entry(
        dataset="NLDAS2", file_type="pfb", period="daily", variable="precipitation"
    )
    data = gr.get_gridded_data(
        dataset="NLDAS2",
        file_type="pfb",
        period="daily",
        variable="precipitation",
        start_time="2005-09-29",
        end_time="2005-10-03",
    )

    assert data.shape == (4, 1888, 3342)


def test_vegm():
    """Test reading vegm files."""

    # Skip this test for now because it takes more than 45 seconds to run
    return

    gr.HYDRODATA = "/hydrodata"
    if not os.path.exists("/hydrodata"):
        # Just skip test if this is run on a machine without /hydrodata access
        return

    grid_bounds = [10, 10, 50, 100]
    entry = hf.get_catalog_entry(
        dataset="conus1_baseline_85",
        file_type="vegm",
        variable="clm_run",
    )

    data = gr.get_ndarray(entry, grid_bounds=grid_bounds)

    # Shape is 18 vegitation types + lat + lnt + clay + sand + color = 23 attributes
    assert data.shape == (23, 90, 40)


def test_gridded_data_baseline85_pressure_head():
    """Test get_gridded_data from baseline85 dataset preasure_head variable."""

    gr.HYDRODATA = "/hydrodata"
    if not os.path.exists("/hydrodata"):
        # Just skip test if this is run on a machine without /hydrodata access
        return

    bounds = [200, 200, 300, 250]

    data = gr.get_gridded_data(
        dataset="conus1_baseline_85",
        file_type="pfb",
        period="daily",
        variable="pressure_head",
        start_time="1984-11-01",
        end_time="1984-11-03",
        grid_bounds=bounds,
    )

    assert data.shape == (2, 5, 50, 100)


def xxtest_gridded_data_baseline85_pressure_head_hourly():
    """Test get_gridded_data from baseline85 dataset preasure_head variable."""

    gr.HYDRODATA = "/hydrodata"
    if not os.path.exists("/hydrodata"):
        # Just skip test if this is run on a machine without /hydrodata access
        return

    bounds = [200, 200, 300, 250]

    data = gr.get_gridded_data(
        dataset="conus1_baseline_mod",
        file_type="pfb",
        period="hourly",
        variable="pressure_head",
        start_time="2005-10-01",
        end_time="2005-10-03",
        grid_bounds=bounds,
    )

    assert data.shape == (48, 5, 50, 100)


def test_gridded_data_baseline_mod_pressure_head():
    """Test get_gridded_data from baseline_mod dataset preasure_head variable."""

    gr.HYDRODATA = "/hydrodata"
    if not os.path.exists("/hydrodata"):
        # Just skip test if this is run on a machine without /hydrodata access
        return

    bounds = [200, 200, 300, 250]

    data = gr.get_gridded_data(
        dataset="conus1_baseline_mod",
        file_type="pfb",
        period="daily",
        variable="pressure_head",
        start_time="2005-09-01",
        end_time="2005-09-03",
        grid_bounds=bounds,
    )

    assert data.shape == (2, 5, 50, 100)


def test_gridded_data_conus1_domain_porosity():
    """Test get_gridded_data from conus1_domain dataset porosity variable."""

    gr.HYDRODATA = "/hydrodata"
    if not os.path.exists("/hydrodata"):
        # Just skip test if this is run on a machine without /hydrodata access
        return

    bounds = [200, 200, 300, 250]

    data = gr.get_gridded_data(
        dataset="conus1_domain",
        file_type="pfb",
        variable="porosity",
        grid_bounds=bounds,
    )

    assert data.shape == (5, 50, 100)


def test_gridded_data_pressure_hourly():
    """Test get_gridded_data from conus1_domain dataset porosity variable."""

    gr.HYDRODATA = "/hydrodata"
    if not os.path.exists("/hydrodata"):
        # Just skip test if this is run on a machine without /hydrodata access
        return

    bounds = [200, 200, 300, 250]

    # Get 1 day of one hour of pressure head
    start_time = "2005-01-01 11:00:00"
    data = gr.get_gridded_data(
        dataset="conus1_baseline_mod",
        file_type="pfb",
        variable="pressure_head",
        period="hourly",
        grid_bounds=bounds,
        start_time=start_time,
    )
    assert data.shape == (1, 5, 50, 100)

    start_time = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
    data = gr.get_gridded_data(
        dataset="conus1_baseline_mod",
        file_type="pfb",
        period="hourly",
        variable="pressure_head",
        grid_bounds=bounds,
        start_time=start_time,
    )
    assert data.shape == (1, 5, 50, 100)


def test_gridded_data_wind_hourly():
    """Test get_gridded_data from conus1_domain dataset north_windspeed variable with no z values."""

    gr.HYDRODATA = "/hydrodata"
    if not os.path.exists("/hydrodata"):
        # Just skip test if this is run on a machine without /hydrodata access
        return

    bounds = [200, 200, 300, 250]

    entry = hf.get_catalog_entry(
        dataset="NLDAS2",
        file_type="pfb",
        variable="north_windspeed",
        period="hourly",
    )

    # Get 1 day of one hour of pressure head
    start_time = "2005-01-01 11:00:00"
    data = gr.get_gridded_data(
        dataset="NLDAS2",
        file_type="pfb",
        variable="north_windspeed",
        period="hourly",
        grid_bounds=bounds,
        start_time=start_time,
    )
    assert data.shape == (1, 50, 100)

    start_time = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
    data = gr.get_gridded_data(
        dataset="NLDAS2",
        file_type="pfb",
        variable="north_windspeed",
        period="hourly",
        grid_bounds=bounds,
        start_time=start_time,
    )
    assert data.shape == (1, 50, 100)


def test_gridded_data_smap_daily():
    """Test get_gridded_data from conus1_domain dataset daily soil_moisture variable with no z values."""

    gr.HYDRODATA = "/hydrodata"
    if not os.path.exists("/hydrodata"):
        # Just skip test if this is run on a machine without /hydrodata access
        return

    bounds = [200, 200, 300, 250]

    # Get 1 day of smap
    start_time = "2022-08-01"
    data = gr.get_gridded_data(
        dataset="nasa_smap",
        variable="soil_moisture",
        period="daily",
        grid="smapgrid",
        grid_bounds=bounds,
        start_time=start_time,
    )
    assert data.shape == (1, 1, 50, 100)


def test_pfmetadata():
    """Test reading pfmetadata files"""

    gr.HYDRODATA = "/hydrodata"
    if not os.path.exists("/hydrodata"):
        # Just skip test if this is run on a machine without /hydrodata access
        return
    bounds = [200, 200, 300, 250]

    data = gr.get_gridded_data(
        dataset="conus1_domain",
        file_type="pfmetadata",
        variable="van_genuchten_n",
        grid_bounds=bounds,
    )

    # The result has 5 days in the time dimension and sliced to x,y shape 100x50 at origin 200, 200 in the conus1 grid.
    assert data.shape == (5, 50, 100)


def test_gridded_data_tiff():
    """Test get_gridded_data from a tiff file."""

    gr.HYDRODATA = "/hydrodata"
    if not os.path.exists("/hydrodata"):
        # Just skip test if this is run on a machine without /hydrodata access
        return

    bounds = [200, 200, 300, 250]

    data = gr.get_gridded_data(
        dataset="huc_mapping",
        file_type="tiff",
        variable="huc_map",
        grid="conus1",
        grid_bounds=bounds,
        level=4,
    )

    assert data.shape == (50, 100)

    data = gr.get_gridded_data(
        dataset="huc_mapping",
        file_type="tiff",
        variable="huc_map",
        grid="conus1",
        level=4,
    )

    assert data.shape == (1888, 3342)

    data = gr.get_gridded_data(
        dataset="huc_mapping",
        file_type="tiff",
        variable="huc_map",
        grid="conus2",
        level=4,
    )

    assert data.shape == (3256, 4442)


def test_gridded_data_latlng():
    """Test get_gridded_data from a latitude and longitude file."""

    gr.HYDRODATA = "/hydrodata"
    if not os.path.exists("/hydrodata"):
        # Just skip test if this is run on a machine without /hydrodata access
        return

    data = gr.get_gridded_data(
        dataset="conus1_domain", file_type="pfb", variable="latitude", grid="conus1"
    )
    assert data.shape == (1888, 3342)

    data = gr.get_gridded_data(
        dataset="conus2_domain", file_type="pfb", variable="latitude", grid="conus2"
    )
    assert data.shape == (3256, 4442)


def test_grid_to_latlng():
    """Test grid_to_latlng."""

    gr.HYDRODATA = "/hydrodata"
    (lat, lng) = hf.grid.to_latlon("conus1", 0, 0)
    assert round(lat, 2) == 31.65
    assert round(lng, 2) == -115.98
    bounds = hf.grid.to_latlon("conus1", *[0, 0, 3341, 1887])
    assert round(bounds[2], 2) == 49.1
    assert round(bounds[3], 2) == -76.11
    (lat, lng) = hf.grid.to_latlon("conus1", 10.5, 10.5)
    assert round(lat, 6) == 31.764588
    assert round(lng, 6) == -115.898577
    (lat, lng) = hf.grid.to_latlon("conus1", 10.0, 10.0)
    assert round(lat, 6) == 31.759219
    assert round(lng, 6) == -115.902573
    (lat, lng) = hf.grid.to_latlon("conus2", 0, 0)
    assert round(lat, 2) == 22.36
    assert round(lng, 2) == -117.85


def test_latlng_to_grid():
    """Test grid_to_latlng."""

    gr.HYDRODATA = "/hydrodata"
    (x, y) = hf.from_latlon("conus1", 31.759219, -115.902573)
    assert round(x) == 10
    assert round(y) == 10
    grid_bounds = hf.from_latlon("conus1", 31.65, -115.98, 31.759219, -115.902573)
    assert round(grid_bounds[0]) == 0
    assert round(grid_bounds[1]) == 0
    grid_bounds = hf.from_latlon("conus2", 31.65, -115.98, 31.759219, -115.902573)
    assert round(grid_bounds[0]) == 441
    assert round(grid_bounds[1]) == 970

    (x, y) = hf.from_latlon("conus1", 49.1423, -76.3369)
    assert round(x) == 3324
    assert round(y) == 1888


def test_get_huc_bbox_conus2():
    """Unit test for get_huc_bbox for conus2"""

    gr.HYDRODATA = "/hydrodata"
    bbox = hf.get_huc_bbox("conus2", ["101900"])
    assert bbox == [1439, 1573, 1844, 1851]
    bbox = hf.get_huc_bbox("conus2", ["1019"])
    assert bbox == [1439, 1573, 1844, 1851]
    bbox = hf.get_huc_bbox("conus2", ["10"])
    assert bbox == [948, 1353, 2741, 2783]
    bbox = hf.get_huc_bbox("conus2", ["15020018"])
    assert bbox == [940, 1333, 1060, 1422]

    # Check the bbox is correct for HUC 15 (this failed with old get_huc_box code)
    bbox = hf.get_huc_bbox("conus2", ["15"])
    assert bbox == [510, 784, 1226, 1763]

    # Check the bbox for HUC16 that is ajacent to the old failing HUC 15
    bbox = hf.get_huc_bbox("conus2", ["16"])
    assert bbox == [279, 1337, 1130, 2152]

    # Check the bbox passes for either the value from the old float32 tiffs or the new int32 tiffs
    bbox = hf.get_huc_bbox("conus2", ["1019000404"])
    assert (bbox == [1468, 1664, 1550, 1693]) or (bbox == [1504, 1670, 1550, 1687])
    bbox = hf.get_huc_bbox("conus2", ["10190004"])
    assert (bbox == [1468, 1664, 1550, 1693]) or (bbox == [1504, 1670, 1550, 1687])


def test_latlng_to_grid_out_of_bounds():
    """Unit tests for when latlng is out of bounds of conus1."""

    gr.HYDRODATA = "/hydrodata"
    with pytest.raises(ValueError):
        (_, _) = hf.from_latlon("conus1", 90, -180)


def test_gridded_data_no_entry_passed():
    """Test able to get and ndarray passing None for entry"""

    if not os.path.exists("/hydrodata"):
        # Just skip test if this is run on a machine without /hydrodata access
        return

    data = gr.get_gridded_data(
        dataset="NLDAS2",
        file_type="pfb",
        period="daily",
        variable="precipitation",
        start_time="2006-01-01",
    )
    assert data.shape == (1, 1888, 3342)

    data = gr.get_gridded_data(
        dataset="NLDAS2",
        file_type="pfb",
        period="daily",
        variable="precipitation",
        start_time="2006-01-01",
        end_time="2006-01-3",
    )
    assert data.shape == (2, 1888, 3342)

    data = gr.get_gridded_data(
        dataset="NLDAS2",
        file_type="pfb",
        period="hourly",
        variable="precipitation",
        start_time="2006-01-01",
        end_time="2006-01-03",
    )
    assert data.shape == (48, 1888, 3342)

    data = gr.get_gridded_data(
        dataset="conus1_baseline_85",
        file_type="pfb",
        period="hourly",
        variable="pressure_head",
        start_time="2006-01-01",
        end_time="2006-01-3",
    )
    assert data.shape == (48, 5, 1888, 3342)

    path = gr.get_file_path(
        None,
        dataset="NLDAS2",
        file_type="pfb",
        period="daily",
        variable="precipitation",
        start_time="2006-01-01",
    )
    assert "sum.093.pfb" in path


def test_get_gridded_data_monthly():
    """Test getting monthly files."""

    options = {
        "dataset": "NLDAS2",
        "variable": "air_temp",
        "file_type": "pfb",
        "period": "monthly",
        "start_time": "2006-01-31",
        "end_time": "2006-03-01",
    }
    data = gr.get_gridded_data(options)
    assert data.shape == (2, 1888, 3342)


def test_get_gridded_data_daily():
    """Test geting daily values from pfb"""
    options = {
        "dataset": "NLDAS2",
        "variable": "air_temp",
        "file_type": "pfb",
        "aggregation": "max",
        "period": "daily",
        "start_time": "2005-10-01",
        "end_time": "2005-10-04",
    }
    data = gr.get_gridded_data(options)
    assert data.shape == (3, 1888, 3342)


def xtest_get_numpy_nasa_smap_conus2():
    """Test geting daily values from pfb"""
    grid_bounds = [100, 100, 150, 300]
    options = {
        "dataset": "nasa_smap",
        "variable": "soil_moisture",
        "file_type": "pfb",
        "period": "daily",
        "start_time": "2023-08-01",
        "grid_bounds": grid_bounds,
    }
    data = gr.get_gridded_data(options)


def test_get_entry_with_multiple_file_types():
    """Test getting a catalog entry that has multiple file types."""

    options = {
        "dataset": "NLDAS2",
        "variable": "precipitation",
        "period": "daily",
        "start_time": "2005-08-01",
    }

    entries = hf.get_catalog_entries(options)
    assert len(entries) > 1
    entry = hf.get_catalog_entry(options)
    assert entry["file_type"] == "pfb"


def test_get_point_anomalies():
    """Unit test for point observation anomalies."""
    options = {
        "site_type": "streamflow",
        "dataset": "obs_anomalies",
        "variable": "anomaly",
        "period": "daily",
    }
    options["start_time"] = "2002-03-01"
    data = gr.get_gridded_data(options)
    data = gr.get_gridded_data(
        site_type="streamflow",
        dataset="obs_anomalies",
        start_time="2002-05-01",
        variable="anomaly",
        period="daily",
        site_id="1013500",
    )
    assert data.shape[0] > 8000


def test_filter_point_obs_by_time():
    """UNit test for stream flow filters."""
    data = gr.get_gridded_data(
        site_type="streamflow",
        dataset="observations",
        start_time="1978-08-01",
        end_time="1978-08-04",
        variable="anomaly",
        period="daily",
        site_id="06787000",
    )
    assert data.shape[0] == 3
    data = gr.get_gridded_data(
        site_type="streamflow",
        dataset="observations",
        start_time="1978-08-01",
        end_time="1978-08-15",
        variable="anomaly",
        period="weekly",
        site_id="06787000",
    )
    assert data.shape[0] == 2


def test_timezone():
    """Test with timezone in start_time/end_time"""

    gr.HYDRODATA = "/hydrodata"
    if not os.path.exists("/hydrodata"):
        # Just skip test if this is run on a machine without /hydrodata access
        return

    bounds = [375, 239, 487, 329]
    start = "2005-10-07"
    time_zone = "EST"
    start_date = datetime.datetime.strptime(start, "%Y-%m-%d")
    if time_zone != "UTC":
        start_date = (
            start_date.replace(tzinfo=pytz.timezone(time_zone))
            .astimezone(pytz.UTC)
            .replace(tzinfo=None)
        )
    end_date = start_date + datetime.timedelta(hours=7)


    data = gr.get_gridded_data(
                dataset="NLDAS2",
        variable="air_temp",
        grid="conus1",
        file_type="pfb",
        period="hourly",
        start_time=start_date,
        end_time=end_date,
        grid_bounds=bounds,
    )
    assert data.shape[0] == 7


def test_get_date_range():
    """Test get_date_range."""

    options = {
        "dataset": "NLDAS2",
        "variable": "precipitation",
        "period": "daily",
        "start_time": "2005-08-01",
    }

    (low, high) = hf.get_date_range(options)
    assert low.strftime("%Y-%m-%d") == "2002-10-01"
    assert high.strftime("%Y-%m-%d") == "2006-09-30"


def test_ambiguous_filter():
    """Test ambiguous filter request."""
    options = {
        "dataset": "NLDAS2",
        "period": "daily",
        "start_time": "2005-08-01",
    }

    with pytest.raises(ValueError) as info:
        hf.get_catalog_entry(options)
    assert "variable = '" in str(info.value) and "'downward_longwave" in str(info.value)


def test_get_huc_from_point():
    """Unit test for get_huc_from_latlon and get_huc_from_xy"""

    grid = "conus1"
    (lat, lng) = hf.grid.to_latlon("conus1", 1078, 722)
    huc_id = hf.get_huc_from_latlon(grid, 10, lat, lng)
    assert huc_id == "1019000404"

    huc_id = hf.get_huc_from_xy(grid, 10, 1078, 722)
    assert huc_id == "1019000404"

    huc_id = hf.get_huc_from_xy(grid, 10, 1078, 1999)
    assert huc_id is None


def test_get_huc_bbox_conus1():
    """Unit test for get_huc_bbox for conus1"""

    with pytest.raises(ValueError):
        hf.get_huc_bbox("bad grid", ["1019000404"])
    with pytest.raises(ValueError):
        hf.get_huc_bbox("conus1", ["1019000404", "123"])

    bbox = hf.get_huc_bbox("conus1", ["1019000404"])
    assert bbox == [1076, 720, 1124, 739]

    bbox = hf.get_huc_bbox("conus1", ["1102001002", "1102001003"])
    assert bbox == [1088, 415, 1132, 453]


def test_getndarray_site_id():
    """Test for a bug using get_gridded_data and site_id variable."""

    data = gr.get_gridded_data(        site_type="streamflow",
        dataset="obs_anomalies",
        variable="site_id",
        period="daily", start_time="2002-03-1")
    assert data.shape[0] >= 8626


def test_filter_errors():
    """Unit test to check for filter error messages."""
    gr.HYDRODATA = "/hydrodata"
    if not os.path.exists("/hydrodata"):
        # Just skip test if this is run on a machine without /hydrodata access
        return

    options = {
        "dataset": "NLDAS2",
        "file_type": "pfb",
        "variable": "north_windspeed",
        "period": "hourly",
        "start_time": "2005-01-01 11:00:00",
        "grid_bounds": [5000, 200, 50010, 300],
    }
    with pytest.raises(ValueError) as info:
        gr.get_gridded_data(options)
    assert "is outside the grid shape 3342, 1888" in str(info.value)

    options = {
        "dataset": "NLDAS2",
        "file_type": "pfb",
        "variable": "north_windspeed",
        "period": "hourly",
        "start_time": "2005-01-01 11:00:00",
        "grid_point": [200, 2000],
    }
    with pytest.raises(ValueError) as info:
        gr.get_gridded_data(options)
    assert "is outside the grid shape 3342, 1888" in str(info.value)


def test_get_datasets():
    """Test get_datasets."""

    datasets = hf.get_datasets()
    assert len(datasets) >= 21
    assert datasets[0] == "CW3E"

    datasets = hf.get_datasets(variable="air_temp")
    assert len(datasets) >= 9
    assert datasets[0] == "CW3E"

    datasets = hf.get_datasets(grid="conus2")
    assert len(datasets) == 6
    assert datasets[0] == "CW3E"

    options = {"variable": "air_temp", "grid": "conus1"}
    datasets = hf.get_datasets(options)
    print(datasets)
    assert len(datasets) >= 4
    assert datasets[0] == "NLDAS2"


def test_get_variables():
    """Test get_variables."""

    variables = hf.get_variables()
    assert len(variables) == 70
    assert variables[0] == "air_temp"
    variables = hf.get_variables(dataset="CW3E")
    assert len(variables) == 8
    assert variables[0] == "air_temp"
    variables = hf.get_variables(grid="conus2")
    assert len(variables) == 33
    assert variables[0] == "air_temp"

    options = {"dataset": "NLDAS2", "grid": "conus1"}
    variables = hf.get_variables(options)
    assert len(variables) == 8
    assert variables[0] == "air_temp"


def test_get_catalog_bug():
    """Test bug in getting catalog entries with typo."""

    bounds = [100, 100, 200, 200]
    entries = hf.get_catalog_entries(
        dataset="conus1_baseline_85",
        file_type="pfb",
        period="hourly",
        variable="precipitation",
    )
    assert len(entries) == 0

    start_time = datetime.datetime.strptime("2005-09-01", "%Y-%m-%d")
    end_time = start_time + datetime.timedelta(hours=24)
    with pytest.raises(ValueError) as info:
        gr.get_gridded_data(
            start_time=start_time,
            end_time=end_time,
            grid_bounds=bounds,
        )
    assert "Ambiguous filter" in str(info.value)


def test_temporal_resolution():
    """Test that temporal_resolution and period are both returned from data_catalog."""

    entry = hf.get_catalog_entry(
        dataset="NLDAS2", file_type="pfb", period="hourly", variable="precipitation"
    )
    assert entry["temporal_resolution"] == "hourly"
    assert entry["period"] == "hourly"
    entry = hf.get_catalog_entry(
        dataset="NLDAS2",
        file_type="pfb",
        temporal_resolution="hourly",
        variable="precipitation",
    )
    assert entry["temporal_resolution"] == "hourly"
    assert entry["period"] == "hourly"


def test_get_gridded_files_pfb():
    """Unit test for get_gridded_files not passing variables list."""

    cd = os.getcwd()
    with tempfile.TemporaryDirectory() as tempdirname:
        os.chdir(tempdirname)
        grid_bounds = [10, 10, 20, 20]
        options = {
            "dataset": "NLDAS2",
            "temporal_resolution": "hourly",
            "variable": "atmospheric_pressure",
            "grid_bounds": grid_bounds,
            "start_time": "2005-10-02",
            "end_time": "2005-10-3",
        }
        assert not os.path.exists("NLDAS2.Press.000001_to_000024.pfb")
        gr.get_gridded_files(options)
        assert os.path.exists("NLDAS2.Press.000001_to_000024.pfb")

        with pytest.raises(ValueError):
            options["dataset"] = "error"
            gr.get_gridded_files(options)
    os.chdir(cd)


def test_get_gridded_files_variables():
    """Unit test for get_gridded_files with variables list."""

    cd = os.getcwd()
    with tempfile.TemporaryDirectory() as tempdirname:
        os.chdir(tempdirname)
        variables = ["air_temp", "precipitation"]
        grid_bounds = [10, 10, 20, 20]
        options = {
            "dataset": "NLDAS2",
            "temporal_resolution": "hourly",
            "grid_bounds": grid_bounds,
            "start_time": "2005-10-02",
            "end_time": "2005-10-3",
        }
        assert not os.path.exists("NLDAS2.Temp.000001_to_000024.pfb")
        assert not os.path.exists("NLDAS2.APCP.000001_to_000024.pfb")
        gr.get_gridded_files(options, variables=variables)
        assert os.path.exists("NLDAS2.Temp.000001_to_000024.pfb")
        assert os.path.exists("NLDAS2.APCP.000001_to_000024.pfb")

    os.chdir(cd)


def test_get_gridded_files_3d():
    """Unit test for get_gridded_files with 3d variable."""

    cd = os.getcwd()
    with tempfile.TemporaryDirectory() as tempdirname:
        os.chdir(tempdirname)
        grid_bounds = [10, 10, 20, 20]
        options = {
            "dataset": "conus1_baseline_mod",
            "variable": "pressure_head",
            "temporal_resolution": "hourly",
            "grid_bounds": grid_bounds,
            "start_time": "2005-10-01",
            "end_time": "2005-10-02",
        }
        assert not os.path.exists("CONUS.2006.out.press.00025.pfb")
        assert not os.path.exists("CONUS.2006.out.press.00024.pfb")
        gr.get_gridded_files(
            options, filename_template="CONUS.{wy}.out.press.{wy_hour:05d}.pfb"
        )
        assert os.path.exists("CONUS.2006.out.press.00001.pfb")
        assert os.path.exists("CONUS.2006.out.press.00024.pfb")
    os.chdir(cd)


def test_get_gridded_files_netcdf():
    """Unit test for get_gridded_files to netcdf file."""

    cd = os.getcwd()
    with tempfile.TemporaryDirectory() as tempdirname:
        os.chdir(tempdirname)
        variables = ["ground_heat", "pressure_head"]
        grid_bounds = [10, 10, 14, 20]
        options = {
            "dataset": "conus1_baseline_mod",
            "temporal_resolution": "hourly",
            "grid_bounds": grid_bounds,
            "start_time": "2005-09-29",
            "end_time": "2005-10-04",
        }
        assert not os.path.exists("NLDAS2.2006.nc")
        gr.get_gridded_files(
            options, variables=variables, filename_template="NLDAS2.{wy}.nc"
        )
        assert os.path.exists("NLDAS2.2006.nc")
        assert os.path.exists("NLDAS2.2005.nc")
        ds = xr.open_dataset("NLDAS2.2006.nc")
        assert len(ds.keys()) == 2
        ground_heat = ds["eflx_soil_grnd"]
        assert ground_heat.shape == (8760, 10, 4)
        pressure_head = ds["Press"]
        assert pressure_head.shape == (8760, 5, 10, 4)
        lat_coord = ds["latitude"]
        assert lat_coord.shape == (10, 4)

        # Check it does nothing if file already exists
        gr.get_gridded_files(
            options, variables=variables, filename_template="NLDAS2.{wy}.nc"
        )

    os.chdir(cd)


def test_get_gridded_files_tiff():
    """Unit test for get_gridded_files to tiff file."""

    cd = os.getcwd()
    with tempfile.TemporaryDirectory() as tempdirname:
        os.chdir(tempdirname)
        variables = ["ground_heat", "pressure_head"]
        variables = ["ground_heat"]
        grid_bounds = [10, 10, 14, 20]
        options = {
            "dataset": "conus1_baseline_mod",
            "temporal_resolution": "hourly",
            "grid_bounds": grid_bounds,
            "start_time": "2005-09-29",
            "end_time": "2005-10-04",
        }
        assert not os.path.exists("conus1_baseline_mod.ground_heat.tiff")
        gr.get_gridded_files(
            options, variables=variables, filename_template="{dataset}.{variable}.tiff"
        )
        assert os.path.exists("conus1_baseline_mod.ground_heat.tiff")
        luc = rioxarray.open_rasterio("conus1_baseline_mod.ground_heat.tiff")
        assert 'standard_parallel_1",33' in str(luc.rio.crs)
    os.chdir(cd)


def test_entry_without_dataset():
    """Test that we can get gridded data with options without a dataset"""

    options = {
        "grid": "conus1",
        "variable": "latitude",
        "file_type": "pfb",
        "grid_bounds": [50, 50, 52, 55],
    }
    entries = hf.get_catalog_entries(options)
    assert len(entries) == 1
    data = hf.get_gridded_data(options)
    assert data.shape == (5, 2)


def test_multiple_aggregations():
    """Test that we can get_gridded_files with multiple aggregations."""

    cd = os.getcwd()
    with tempfile.TemporaryDirectory() as tempdirname:
        os.chdir(tempdirname)

        variables = ["precipitation", "air_temp", "downward_shortwave"]
        bounds = [500, 500, 502, 502]
        start_time = datetime.datetime(1991, 8, 4)
        end_time = start_time + datetime.timedelta(days=2)
        options = {
            "dataset": "CW3E",
            "grid_bounds": bounds,
            "temporal_resolution": "daily",
            "start_time": start_time,
            "end_time": end_time,
        }
        assert not os.path.exists("foo.nc")
        gr.get_gridded_files(options, variables=variables, filename_template="foo.nc")
        assert os.path.exists("foo.nc")
        ds = xr.open_dataset("foo.nc")
        da = ds["APCP"]
        assert da.shape == (365, 2, 2)
        da = ds["DSWR"]
        assert da.shape == (365, 2, 2)
        da = ds["Temp_mean"]
        assert da.shape == (365, 2, 2)
        da = ds["Temp_min"]
        assert da.shape == (365, 2, 2)
        da = ds["Temp_max"]
        assert da.shape == (365, 2, 2)
    os.chdir(cd)


def test_huc_mask():
    """Test that mask is applied when HUC is provided."""
    start_time = datetime.datetime(1991, 8, 4)
    options = {
        "dataset": "CW3E",
        "huc_id": "18060006",
        "temporal_resolution": "daily",
        "start_time": start_time,
        "variable": "precipitation",
        "mask": "true",
    }
    data = gr.get_gridded_data(options)
    assert math.isnan(data[0, 0, 0])


def test_huc_border():
    """Test that mask is applied at coastal areas with a grid."""
    start_time = datetime.datetime(1991, 8, 4)
    bounds = [56, 1433, 155, 1643]
    options = {
        "dataset": "CW3E",
        "grid_bounds": bounds,
        "temporal_resolution": "daily",
        "start_time": start_time,
        "variable": "precipitation",
    }
    data = gr.get_gridded_data(options)
    assert math.isnan(data[0, 0, 0])


def test_get_wtd():
    """Unit test reading the 3 resolution of water table depth files."""

    # Test the 1000 meter resolution version
    x = 1500
    y = 1500
    grid_bound_x_width = 2
    grid_bounds_y_height = 2
    bounds = [x, y, x + grid_bound_x_width, y + grid_bounds_y_height]
    options = {
        "dataset": "conus2_current_conditions",
        "grid_bounds": bounds,
        "variable": "water_table_depth",
        "grid": "conus2_wtd",
    }
    data = hf.get_gridded_data(options)
    assert (
        hf.get_path(options)
        == "/hydrodata/temp/high_resolution_data/WTD_estimates/30m/remapped_data/wtd_mean_estimate_RF_additional_inputs_dummy_drop0LP_1000m_CONUS2_m_1s_remapped.tif"
    )

    assert data.shape == (2, 2)
    assert str(round(data[0, 0], 5)) == "52.86004"
    assert str(round(data[0, 1], 5)) == "43.37404"
    assert str(round(data[1, 0], 5)) == "27.75672"
    assert str(round(data[1, 1], 5)) == "36.64674"

    # Test the 100 meter resolution version
    # Same points, but values are not exactly the same as 1000 because of aggregation in resolutions
    x = 1500 * 10
    y = 1500 * 10
    bounds = [x, y, x + grid_bound_x_width, y + grid_bounds_y_height]
    options = {
        "dataset": "conus2_current_conditions",
        "grid_bounds": bounds,
        "variable": "water_table_depth",
        "grid": "conus2_wtd.100",
    }
    data = hf.get_gridded_data(options)
    assert (
        hf.get_path(options)
        == "/hydrodata/temp/high_resolution_data/WTD_estimates/30m/remapped_data/wtd_mean_estimate_RF_additional_inputs_dummy_drop0LP_100m_CONUS2_m_1s_remapped.tif"
    )
    assert data.shape == (2, 2)
    assert str(round(data[0, 0], 5)) == "58.01496"
    assert str(round(data[0, 1], 5)) == "54.30452"
    assert str(round(data[1, 0], 5)) == "48.61397"
    assert str(round(data[1, 1], 5)) == "49.04675"

    # Test the 30 meter resolution version
    # Same points, but values are not exactly the same as 1000 because of aggregation in resolutions
    x = int((1500 * 1000) / 30)
    y = int((1500 * 1000) / 30)
    bounds = [x, y, x + grid_bound_x_width, y + grid_bounds_y_height]
    options = {
        "dataset": "conus2_current_conditions",
        "grid_bounds": bounds,
        "variable": "water_table_depth",
        "grid": "conus2_wtd.30",
    }
    data = hf.get_gridded_data(options)
    assert (
        hf.get_path(options)
        == "/hydrodata/temp/high_resolution_data/WTD_estimates/30m/compressed_data/wtd_mean_estimate_RF_additional_inputs_dummy_drop0LP_1s_CONUS2_m_remapped_unflip_compressed.tif"
    )

    assert data.shape == (2, 2)
    assert str(round(data[0, 0], 5)) == "77.19169"
    assert str(round(data[0, 1], 5)) == "78.74432"
    assert str(round(data[1, 0], 5)) == "78.69136"
    assert str(round(data[1, 1], 5)) == "78.74432"


def test_wtd_1000m_north():
    """Unit test edge condition found during integration testing."""
    bounds = [1593, 1724, 3420, 3484]
    options = {
        "dataset": "conus2_current_conditions",
        "grid_bounds": bounds,
        "variable": "water_table_depth",
        "grid": "conus2_wtd",
    }
    data = hf.get_gridded_data(options)
    assert data.shape == ((1760, 1827))


def test_noaa_precip():
    """Unit test NOAA precip"""
    bounds = [1000, 1000, 1005, 1005]
    options = {
        "dataset": "noaa",
        "grid_bounds": bounds,
        "start_time": "2023-03-01",
        "end_time": "2023-03-03",
        "variable": "precipitation",
    }
    data = hf.get_gridded_data(options)
    assert data.shape == (2, 5, 5)
    assert round(data[0, 0, 0], 3) == 0.653


def test_noaa_temp():
    """Unit test NOAA air_temp data."""
    bounds = [1000, 1000, 1005, 1005]
    options = {
        "dataset": "noaa",
        "grid_bounds": bounds,
        "start_time": "2023-03-01",
        "end_time": "2023-03-03",
        "variable": "air_temp",
        "aggregation": "min",
    }
    data = hf.get_gridded_data(options)
    assert data.shape == (2, 5, 5)
    assert round(data[0, 0, 0], 3) == 257.5

    options = {
        "dataset": "noaa",
        "grid_bounds": bounds,
        "start_time": "2023-03-01",
        "end_time": "2023-03-03",
        "variable": "air_temp",
        "aggregation": "max",
    }
    data = hf.get_gridded_data(options)
    assert data.shape == (2, 5, 5)
    assert round(data[0, 0, 0], 3) == 278.25

    options = {
        "dataset": "noaa",
        "grid_bounds": bounds,
        "start_time": "2023-03-01",
        "end_time": "2023-03-03",
        "variable": "air_temp",
        "aggregation": "mean",
    }
    data = hf.get_gridded_data(options)
    assert data.shape == (2, 5, 5)
    assert round(data[0, 0, 0], 3) == 265.25


def test_topographic_index():
    """Unit test topographic_index variable."""
    cd = os.getcwd()
    with tempfile.TemporaryDirectory() as tempdirname:
        os.chdir(tempdirname)

        bounds = [1000, 1000, 1005, 1005]
        options = {
            "dataset": "conus1_domain",
            "grid_bounds": bounds,
            "variable": "topographic_index",
        }
        hf.get_gridded_files(options, filename_template="foo.nc")
        ds = xr.open_dataset("foo.nc")
        da = ds["topographic_index"]
        assert da.shape == (5, 5)
    os.chdir(cd)


def test_gridded_files_default_temporal_resolution():
    """Test reading gridded files without specifing temporal resolution."""

    cd = os.getcwd()
    with tempfile.TemporaryDirectory() as tempdirname:
        os.chdir(tempdirname)
        options = {
            "dataset": "conus1_current_conditions",
            "variable": "soil_moisture",
            "grid_bounds": [100, 100, 104, 104],
            "start_time": "2024-03-01",
            "end_time": "2024-03-03",
        }
        variables = ["soil_moisture"]
        gr.get_gridded_files(options, filename_template="foo.nc", variables=variables)
        assert os.path.exists("foo.nc")
    os.chdir(cd)

    assert gr._get_temporal_resolution_from_catalog(options) == "daily"

    with pytest.raises(ValueError):
        gr._get_temporal_resolution_from_catalog({"dataset": "CW3E"})


def test_flow_direction():
    """Test reading flow_direction"""

    options = {
        "dataset": "conus1_domain",
        "variable": "flow_direction",
        "grid_bounds": [0, 0, 5, 5],
        "nomask": "true",
        "file_type": "pfb",
    }
    data = hf.get_gridded_data(options)
    assert data[0, 0] == 4.0
    assert data[0, 3] == 1.0

    options = {
        "dataset": "conus1_domain",
        "variable": "flow_direction",
        "grid_bounds": [0, 0, 5, 5],
        "nomask": "true",
        "file_type": "tiff",
    }
    data = hf.get_gridded_data(options)
    assert data[0, 0] == 4.0
    assert data[0, 3] == 1.0

    options = {
        "dataset": "conus1_domain",
        "variable": "flow_direction",
        "grid_bounds": [100, 100, 104, 104],
        "nomask": "true",
    }
    data = hf.get_gridded_data(options)
    assert data[0, 0] == 1.0
    assert data[0, 1] == 2.0

    # Validate conus2 flow_direction
    options = {
        "dataset": "conus2_domain",
        "variable": "flow_direction",
        "grid_bounds": [900, 900, 910, 910],
        "file_type": "pfb",
    }
    data = hf.get_gridded_data(options)
    assert data[0, 0] == 1.0
    assert data[0, 1] == 4.0

    options = {
        "dataset": "conus2_domain",
        "variable": "flow_direction",
        "grid_bounds": [900, 900, 910, 910],
        "file_type": "tiff",
        "grid": "conus2",
    }
    data = hf.get_gridded_data(options)
    assert data[0, 0] == 1.0
    assert data[0, 1] == 4.0


def test_smap_current_conditions():
    """Test a bug that happened when trying to read too many pfb files in a single call to get_gridded_data"""
    x = 2387
    y = 1673
    st_dt = "2023-08-01"
    options = {
        "data_catalog_entry_id": "213",
        "start_time": st_dt,
        "end_time": "2024-02-01",
        "x": x,
        "y": y,
    }
    data = hf.get_gridded_data(options)
    # This used to fail before fixing a bug to call read_pfb_sequence with a limited block size
    assert (data[62] - 0.3409) <= 0.001, "Data for long block length not read properly"


def test_cw3e_version():
    """Test request for CW3E dataset using dataset_version parameter."""
    options = {
        "dataset": "CW3E",
        "variable": "air_temp",
        "temporal_resolution": "hourly",
        "start_time": "2002-10-01",
        "end_time": "2002-10-02",
        "grid": "conus2",
        "grid_bounds": [1000, 1000, 1002, 1002],
    }

    options_version09 = options.copy()
    options_version09["dataset_version"] = "0.9"
    cw3e_version09 = hf.get_gridded_data(options_version09)
    assert cw3e_version09[0, 0, 0] - 299.78436 <= 0.00001

    options_version08 = options.copy()
    options_version08["dataset_version"] = "0.8"
    cw3e_version08 = hf.get_gridded_data(options_version08)
    assert cw3e_version08[0, 0, 0] - 299.04806 <= 0.00001

    cw3e_default = hf.get_gridded_data(options)
    np.testing.assert_array_equal(cw3e_default, cw3e_version09)


if __name__ == "__main__":
    pytest.main([__file__])
