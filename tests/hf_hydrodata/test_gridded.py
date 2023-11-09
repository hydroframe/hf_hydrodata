"""
Unit test for the gridded module.
"""

# pylint: disable=C0301,C0103,W0632,W0702,W0101,C0302,W0105,E0401,C0413,R0903,W0613,R0912
import sys
import os
import datetime
from unittest.mock import patch
import pytest
import pytz

from parflow import read_pfb_sequence

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../src")))

import hf_hydrodata.gridded

@pytest.fixture(autouse=True)
def patch_api(mocker):
    mocker.patch("hf_hydrodata.data_model_access._load_model_from_api", return_value=None)
    
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

    hf_hydrodata.gridded.HYDRODATA = "/hydrodata"
    hf_hydrodata.gridded.get_raw_file(
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
        hf_hydrodata.gridded.HYDRODATA = "/empty"
        hf_hydrodata.gridded.get_raw_file(
            filepath="./vegp.dat",
            dataset="conus1_baseline_mod",
            file_type="vegp",
            variable="clm_run",
        )

        assert os.path.exists("./vegp.dat") is True
        os.remove("./vegp.dat")


def test_get_drv_clm():
    """Test ability to retreive drv_clm file."""

    hf_hydrodata.gridded.HYDRODATA = "/hydrodata"
    hf_hydrodata.gridded.get_raw_file(
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
        hf_hydrodata.gridded.HYDRODATA = "/empty"
        hf_hydrodata.gridded.get_raw_file(
            filepath="./vegp.dat",
            dataset="conus1_baseline_mod",
            file_type="vegp",
            variable="clm_run",
        )

        assert os.path.exists("./vegp.dat") is True
        os.remove("./vegp.dat")


def test_start_time_in_get_ndarray():
    """Test ability to pass start_time in get_ndarray method."""

    hf_hydrodata.gridded.HYDRODATA = "/hydrodata"
    entry = hf_hydrodata.gridded.get_catalog_entry(
        dataset="NLDAS2", file_type="pfb", period="hourly", variable="precipitation"
    )

    start_time = datetime.datetime.strptime("2005-09-01", "%Y-%m-%d")
    end_time = start_time + datetime.timedelta(hours=96)
    data = hf_hydrodata.gridded.get_ndarray(
        entry, start_time=start_time, end_time=end_time
    )
    assert data.shape[0] == 96

    start_time = "2005-09-01"
    data = hf_hydrodata.gridded.get_ndarray(
        entry, start_time=start_time, end_time=end_time
    )
    assert data.shape[0] == 96


def test_get_numpy():
    """Test the get_numpy helper method"""

    data = hf_hydrodata.gridded.get_numpy(
        dataset="NLDAS2",
        file_type="pfb",
        period="hourly",
        variable="precipitation",
        start_time="2005-09-01 00:00:00",
    )
    assert data.shape == (1, 1888, 3342)


def test_get_date_dimension_pfb():
    """Test get_date_dimension function for pfb data"""

    hf_hydrodata.gridded.HYDRODATA = "/hydrodata"
    entry = hf_hydrodata.gridded.get_catalog_entry(
        dataset="NLDAS2", file_type="pfb", period="hourly", variable="precipitation"
    )

    start_time = datetime.datetime.strptime("2005-09-01", "%Y-%m-%d")
    end_time = start_time + datetime.timedelta(hours=96)
    dates = []
    hf_hydrodata.gridded.get_ndarray(
        entry, start_time=start_time, end_time=end_time, time_values=dates
    )

    assert len(dates) == 4
    assert dates[1] == "2005-09-02"

    """
    # test remote execution of get_ndarray()
    hf_hydrodata.gridded.HYDRODATA = "/empty"
    entry = hf_hydrodata.gridded.get_catalog_entry(
        dataset="NLDAS2", file_type="pfb", period="hourly", variable="precipitation"
    )
    start_time = datetime.datetime.strptime("2005-09-01", "%Y-%m-%d")
    end_time = start_time + datetime.timedelta(hours=24)
    dates = []
    hf_hydrodata.gridded.get_ndarray(
        entry, start_time=start_time, end_time=end_time, time_values=dates
    )

    assert len(dates) == 1
    assert dates[0] == "2005-09-01"
    """


def test_get_date_dimension_netcdf():
    """Test get_date_dimension function for netcdf data"""

    hf_hydrodata.gridded.HYDRODATA = "/hydrodata"
    entry = hf_hydrodata.gridded.get_catalog_entry(
        dataset="NLDAS2", file_type="pfb", period="hourly", variable="precipitation"
    )

    start_time = datetime.datetime.strptime("2005-09-01", "%Y-%m-%d")
    end_time = start_time + datetime.timedelta(hours=96)
    dates = []
    hf_hydrodata.gridded.get_ndarray(
        entry, start_time=start_time, end_time=end_time, time_values=dates
    )
    assert len(dates) == 4
    assert dates[1] == "2005-09-02"


def test_get_entries():
    """Test getting data_catalog_entries using filters."""

    hf_hydrodata.gridded.HYDRODATA = "/hydrodata"
    rows = hf_hydrodata.gridded.get_catalog_entries(
        dataset="NLDAS2", file_type="pfb", period="daily"
    )
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

    hf_hydrodata.gridded.HYDRODATA = "/hydrodata"
    row = hf_hydrodata.gridded.get_catalog_entry(
        dataset="NLDAS2", file_type="pfb", period="daily", variable="precipitation"
    )
    assert row.get_value("id") == "130"

    with pytest.raises(ValueError):
        hf_hydrodata.gridded.get_catalog_entry(
            dataset="NLDAS2", file_type="pfb", period="daily"
        )

    entry = hf_hydrodata.gridded.get_catalog_entry(
        dataset="NLDAS2xxx",
        file_type="pfb",
        period="daily",
        variable="precipitation",
    )
    assert entry is None


def test_get_table_rows():
    """Test getting rows from any table in the data model."""

    hf_hydrodata.gridded.HYDRODATA = "/hydrodata"
    rows = hf_hydrodata.gridded.get_table_rows("variable", variable_type="atmospheric")
    assert len(rows) >= 8

    rows = hf_hydrodata.gridded.get_table_rows("variable", variable_type="land_use")
    assert len(rows) == 0


def test_get_table_row():
    """Test getting a single row from a table."""

    hf_hydrodata.gridded.HYDRODATA = "/hydrodata"
    entry = hf_hydrodata.gridded.get_table_row("grid", id="conus1")
    assert entry is not None

    entry = hf_hydrodata.gridded.get_table_row("grid", id="conus5")
    assert entry is None

    with pytest.raises(ValueError):
        entry = hf_hydrodata.gridded.get_table_row(
            "variable_type", variable_type="atomspheric"
        )


def test_get_paths_and_metadata():
    """Demonstrate getting water table depth files crossing a water year."""

    hf_hydrodata.gridded.HYDRODATA = "/hydrodata"
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
    row = hf_hydrodata.gridded.get_catalog_entry(options)
    paths = hf_hydrodata.gridded.get_file_paths(row, options)

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

    hf_hydrodata.gridded.HYDRODATA = "/hydrodata"
    options = {
        "variable": "pressure_head",
        "dataset": "conus1_baseline_mod",
        "grid": "conus1",
        "period": "hourly",
        "file_type": "pfb",
        "start_time": "2005-09-29",
        "end_time": "2005-10-03",
    }
    row = hf_hydrodata.gridded.get_catalog_entry(options)
    paths = hf_hydrodata.gridded.get_file_paths(row, options)

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

    hf_hydrodata.gridded.HYDRODATA = "/hydrodata"
    if not os.path.exists("/hydrodata"):
        # Just skip test if this is run on a machine without /hydrodata access
        return

    rows = hf_hydrodata.gridded.get_catalog_entries()

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
        elif site_type == "streamflow":
            site_id = "05490600"
        elif site_type == "groundwater":
            site_id = "351058106391002"
        elif site_type == "snotel":
            site_id = "348:UT:SNTL"
        elif dataset == "obs_anomolies":
            start_time = "2002-01-01"
        elif dataset == "conus1_current_conditions":
            start_time = "2023-01-01"

        row_id = row["id"]
        if not row_id in ["206", "207", "208", "209", "210", "213", "253", "254"]:
            paths = hf_hydrodata.gridded.get_file_paths(
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

    hf_hydrodata.gridded.HYDRODATA = "/hydrodata"
    if not os.path.exists("/hydrodata"):
        # Just skip test if this is run on a machine without /hydrodata access
        return

    # Skip this test for now because it takes more than 60 seconds to run (we will run it manually for now)
    return

    rows = hf_hydrodata.gridded.get_catalog_entries()

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
            ] and entry_id not in ["1", "68", "69"]:
                data = hf_hydrodata.gridded.get_ndarray(
                    row, start_time=start_time, level=4
                )
                if data is None:
                    bad_row = True
        except:
            print(f"No data for {entry_id}")
            bad_row = True
    assert bad_row is False


def test_get__tables():
    """Test get_table_names."""

    hf_hydrodata.gridded.HYDRODATA = "/hydrodata"
    table_names = hf_hydrodata.gridded.get_table_names()
    assert len(table_names) >= 14


def test_subsetting():
    """Test subsetting"""

    hf_hydrodata.gridded.HYDRODATA = "/hydrodata"
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
    row = hf_hydrodata.gridded.get_catalog_entry(options)
    paths = hf_hydrodata.gridded.get_file_paths(row, options)

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


def test_get_ndarray_pfb_precipitation():
    """Test get_ndarray of a NLDAS2 pfb precipitation variable sliced by bounds."""

    hf_hydrodata.gridded.HYDRODATA = "/hydrodata"
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
    entry = hf_hydrodata.gridded.get_catalog_entry(
        dataset="NLDAS2", file_type="pfb", period="daily", variable="precipitation"
    )

    # The data result has 4 days in the time dimension because end time is exclusive
    data = hf_hydrodata.gridded.get_ndarray(
        entry, start_time="2005-09-29", end_time="2005-10-03", grid_bounds=bounds
    )
    assert data.shape == (4, 50, 100)

    # Select a single time value with a bounds
    data = hf_hydrodata.gridded.get_ndarray(
        entry, start_time="2005-09-29", grid_bounds=bounds
    )
    assert data.shape == (1, 50, 100)

    # The data result has 4 days in the time dimension because end time is exclusive
    data = hf_hydrodata.gridded.get_ndarray(
        entry,
        start_time="2005-09-29",
        end_time="2005-10-03",
        latlng_bounds=latlng_bounds,
    )
    assert data.shape == (4, 50, 100)

    """
    hf_hydrodata.gridded.HYDRODATA = "/empty"
    # The data result has 4 days in the time dimension because end time is exclusive
    data = hf_hydrodata.gridded.get_ndarray(
        entry, start_time="2005-09-29", end_time="2005-10-03", grid_bounds=bounds
    )
    assert data.shape == (4, 50, 100)

    data = hf_hydrodata.gridded.get_ndarray(
        entry, start_time="2005-09-29", end_time="2005-10-03", latlng_bounds=latlng_bounds
    )
    assert data.shape == (4, 50, 100)

    # Select a single time value with a bounds
    data = hf_hydrodata.gridded.get_ndarray(
        entry, start_time="2005-09-29", grid_bounds=bounds
    )
    assert data.shape == (1, 50, 100)
    """


def test_get_ndarray_pfb_precipitation_string_input():
    """Test get_ndarray of a NLDAS2 pfb precipitation variable sliced by bounds."""

    hf_hydrodata.gridded.HYDRODATA = "/hydrodata"
    if not os.path.exists("/hydrodata"):
        # Just skip test if this is run on a machine without /hydrodata access
        return
    bounds = "[200, 200, 300, 250]"
    latlng_bounds = "[33.79169338210987, -114.34357566786298, 34.41096361516614, -113.38485056306695]"

    # Get a daily precipitation entry from data catalog
    entry = hf_hydrodata.gridded.get_catalog_entry(
        dataset="NLDAS2", file_type="pfb", period="daily", variable="precipitation"
    )

    # The data result has 4 days in the time dimension because end time is exclusive
    data = hf_hydrodata.gridded.get_ndarray(
        entry, start_time="2005-09-29", end_time="2005-10-03", grid_bounds=bounds
    )
    assert data.shape == (4, 50, 100)

    # The data result has 4 days in the time dimension because end time is exclusive
    data = hf_hydrodata.gridded.get_ndarray(
        entry,
        start_time="2005-09-29",
        end_time="2005-10-03",
        latlng_bounds=latlng_bounds,
    )
    assert data.shape == (4, 50, 100)

    # Select a single time value with a bounds
    data = hf_hydrodata.gridded.get_ndarray(
        entry, start_time="2005-09-29", grid_bounds=bounds
    )
    assert data.shape == (1, 50, 100)

    """
    hf_hydrodata.gridded.HYDRODATA = "/empty"
    # The data result has 4 days in the time dimension because end time is exclusive
    
    data = hf_hydrodata.gridded.get_ndarray(
        entry, start_time="2005-09-29", end_time="2005-10-03", grid_bounds=bounds
    )
    assert data.shape == (4, 50, 100)

    # Select a single time value with a bounds
    data = hf_hydrodata.gridded.get_ndarray(
        entry, start_time="2005-09-29", grid_bounds=bounds
    )
    assert data.shape == (1, 50, 100)
    
    data = hf_hydrodata.gridded.get_ndarray(
        entry, start_time="2005-09-29", latlng_bounds=latlng_bounds
    )
    assert data.shape == (1, 50, 100)
    """


def test_get_nldas2_wind_pfb_hourly():
    """Test get_ndarray of a NLDAS2 pfb wind variable sliced by bounds."""

    hf_hydrodata.gridded.HYDRODATA = "/hydrodata"
    if not os.path.exists("/hydrodata"):
        # Just skip test if this is run on a machine without /hydrodata access
        return
    bounds = [200, 200, 300, 250]

    entry = hf_hydrodata.gridded.get_catalog_entry(
        dataset="NLDAS2", file_type="pfb", period="hourly", variable="east_windspeed"
    )

    # The result has 5 days of 24 hours in the time dimension and sliced to x,y shape 100x50 at origin 200, 200 in the conus1 grid.
    data = hf_hydrodata.gridded.get_ndarray(
        entry, start_time="2005-09-29", end_time="2005-10-04", grid_bounds=bounds
    )
    assert data.shape == (120, 50, 100)

    # This result has only one day values but is hourly so will have one 24 hour time dimension in the result.
    data = hf_hydrodata.gridded.get_ndarray(
        entry, start_time="2005-09-29", grid_bounds=bounds
    )
    assert data.shape == (1, 50, 100)


def test_get_ndarray_no_grid_bounds():
    """Test get ndarray without grid_bounds parameters."""

    hf_hydrodata.gridded.HYDRODATA = "/hydrodata"
    if not os.path.exists("/hydrodata"):
        # Just skip test if this is run on a machine without /hydrodata access
        return

    entry = hf_hydrodata.gridded.get_catalog_entry(
        dataset="NLDAS2", file_type="pfb", period="daily", variable="precipitation"
    )
    data = hf_hydrodata.gridded.get_ndarray(
        entry, start_time="2005-09-29", end_time="2005-10-03"
    )

    assert data.shape == (4, 1888, 3342)


def test_vegm():
    """Test reading vegm files."""

    # Skip this test for now because it takes more than 45 seconds to run
    return

    hf_hydrodata.gridded.HYDRODATA = "/hydrodata"
    if not os.path.exists("/hydrodata"):
        # Just skip test if this is run on a machine without /hydrodata access
        return

    grid_bounds = [10, 10, 50, 100]
    entry = hf_hydrodata.gridded.get_catalog_entry(
        dataset="conus1_baseline_85",
        file_type="vegm",
        variable="clm_run",
    )

    data = hf_hydrodata.gridded.get_ndarray(entry, grid_bounds=grid_bounds)

    # Shape is 18 vegitation types + lat + lnt + clay + sand + color = 23 attributes
    assert data.shape == (23, 90, 40)


def test_get_ndarray_baseline85_pressure_head():
    """Test get_ndarray from baseline85 dataset preasure_head variable."""

    hf_hydrodata.gridded.HYDRODATA = "/hydrodata"
    if not os.path.exists("/hydrodata"):
        # Just skip test if this is run on a machine without /hydrodata access
        return

    bounds = [200, 200, 300, 250]

    entry = hf_hydrodata.gridded.get_catalog_entry(
        dataset="conus1_baseline_85",
        file_type="pfb",
        period="daily",
        variable="pressure_head",
    )

    data = hf_hydrodata.gridded.get_ndarray(
        entry, start_time="1984-11-01", end_time="1984-11-03", grid_bounds=bounds
    )

    assert data.shape == (2, 5, 50, 100)


def xxtest_get_ndarray_baseline85_pressure_head_hourly():
    """Test get_ndarray from baseline85 dataset preasure_head variable."""

    hf_hydrodata.gridded.HYDRODATA = "/hydrodata"
    if not os.path.exists("/hydrodata"):
        # Just skip test if this is run on a machine without /hydrodata access
        return

    bounds = [200, 200, 300, 250]

    entry = hf_hydrodata.gridded.get_catalog_entry(
        dataset="conus1_baseline_mod",
        file_type="pfb",
        period="hourly",
        variable="pressure_head",
    )

    data = hf_hydrodata.gridded.get_ndarray(
        entry, start_time="2005-10-01", end_time="2005-10-03", grid_bounds=bounds
    )

    assert data.shape == (48, 5, 50, 100)


def test_get_ndarray_baseline_mod_pressure_head():
    """Test get_ndarray from baseline_mod dataset preasure_head variable."""

    hf_hydrodata.gridded.HYDRODATA = "/hydrodata"
    if not os.path.exists("/hydrodata"):
        # Just skip test if this is run on a machine without /hydrodata access
        return

    bounds = [200, 200, 300, 250]

    entry = hf_hydrodata.gridded.get_catalog_entry(
        dataset="conus1_baseline_mod",
        file_type="pfb",
        period="daily",
        variable="pressure_head",
    )

    data = hf_hydrodata.gridded.get_ndarray(
        entry, start_time="2005-09-01", end_time="2005-09-03", grid_bounds=bounds
    )

    assert data.shape == (2, 5, 50, 100)


def test_get_ndarray_conus1_domain_porosity():
    """Test get_ndarray from conus1_domain dataset porosity variable."""

    hf_hydrodata.gridded.HYDRODATA = "/hydrodata"
    if not os.path.exists("/hydrodata"):
        # Just skip test if this is run on a machine without /hydrodata access
        return

    bounds = [200, 200, 300, 250]

    entry = hf_hydrodata.gridded.get_catalog_entry(
        dataset="conus1_domain", file_type="pfb", variable="porosity"
    )

    data = hf_hydrodata.gridded.get_ndarray(entry, grid_bounds=bounds)

    assert data.shape == (5, 50, 100)


def test_get_ndarray_pressure_hourly():
    """Test get_ndarray from conus1_domain dataset porosity variable."""

    hf_hydrodata.gridded.HYDRODATA = "/hydrodata"
    if not os.path.exists("/hydrodata"):
        # Just skip test if this is run on a machine without /hydrodata access
        return

    bounds = [200, 200, 300, 250]

    entry = hf_hydrodata.gridded.get_catalog_entry(
        dataset="conus1_baseline_mod",
        file_type="pfb",
        variable="pressure_head",
        period="hourly",
    )

    # Get 1 day of one hour of pressure head
    start_time = "2005-01-01 11:00:00"
    data = hf_hydrodata.gridded.get_ndarray(
        entry, grid_bounds=bounds, start_time=start_time
    )
    assert data.shape == (1, 5, 50, 100)

    start_time = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
    data = hf_hydrodata.gridded.get_ndarray(
        entry, grid_bounds=bounds, start_time=start_time
    )
    assert data.shape == (1, 5, 50, 100)


def test_get_ndarray_wind_hourly():
    """Test get_ndarray from conus1_domain dataset north_windspeed variable with no z values."""

    hf_hydrodata.gridded.HYDRODATA = "/hydrodata"
    if not os.path.exists("/hydrodata"):
        # Just skip test if this is run on a machine without /hydrodata access
        return

    bounds = [200, 200, 300, 250]

    entry = hf_hydrodata.gridded.get_catalog_entry(
        dataset="NLDAS2",
        file_type="pfb",
        variable="north_windspeed",
        period="hourly",
    )

    # Get 1 day of one hour of pressure head
    start_time = "2005-01-01 11:00:00"
    data = hf_hydrodata.gridded.get_ndarray(
        entry, grid_bounds=bounds, start_time=start_time
    )
    assert data.shape == (1, 50, 100)

    start_time = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
    data = hf_hydrodata.gridded.get_ndarray(
        entry, grid_bounds=bounds, start_time=start_time
    )
    assert data.shape == (1, 50, 100)


def test_get_ndarray_smap_daily():
    """Test get_ndarray from conus1_domain dataset daily soil_moisture variable with no z values."""

    hf_hydrodata.gridded.HYDRODATA = "/hydrodata"
    if not os.path.exists("/hydrodata"):
        # Just skip test if this is run on a machine without /hydrodata access
        return

    bounds = [200, 200, 300, 250]

    entry = hf_hydrodata.gridded.get_catalog_entry(
        dataset="nasa_smap",
        variable="soil_moisture",
        period="daily",
        grid="smapgrid",
    )
    # Get 1 day of smap
    start_time = "2022-08-01"
    data = hf_hydrodata.gridded.get_ndarray(
        entry, grid_bounds=bounds, start_time=start_time
    )
    assert data.shape == (1, 1, 50, 100)


def test_pfmetadata():
    """Test reading pfmetadata files"""

    hf_hydrodata.gridded.HYDRODATA = "/hydrodata"
    if not os.path.exists("/hydrodata"):
        # Just skip test if this is run on a machine without /hydrodata access
        return
    bounds = [200, 200, 300, 250]

    entry = hf_hydrodata.gridded.get_catalog_entry(
        dataset="conus1_domain", file_type="pfmetadata", variable="van_genuchten_n"
    )

    data = hf_hydrodata.gridded.get_ndarray(entry, grid_bounds=bounds)

    # The result has 5 days in the time dimension and sliced to x,y shape 100x50 at origin 200, 200 in the conus1 grid.
    assert data.shape == (5, 50, 100)


def test_get_ndarray_tiff():
    """Test get_ndarray from a tiff file."""

    hf_hydrodata.gridded.HYDRODATA = "/hydrodata"
    if not os.path.exists("/hydrodata"):
        # Just skip test if this is run on a machine without /hydrodata access
        return

    bounds = [200, 200, 300, 250]

    entry = hf_hydrodata.gridded.get_catalog_entry(
        dataset="huc_mapping", file_type="tiff", variable="huc_map", grid="conus1"
    )

    data = hf_hydrodata.gridded.get_ndarray(entry, grid_bounds=bounds, level=4)

    assert data.shape == (50, 100)

    data = hf_hydrodata.gridded.get_ndarray(entry, level=4)

    assert data.shape == (1888, 3342)

    entry = hf_hydrodata.gridded.get_catalog_entry(
        dataset="huc_mapping", file_type="tiff", variable="huc_map", grid="conus2"
    )

    data = hf_hydrodata.gridded.get_ndarray(entry, level=4)

    assert data.shape == (3256, 4442)


def test_get_ndarray_latlng():
    """Test get_ndarray from a latitude and longitude file."""

    hf_hydrodata.gridded.HYDRODATA = "/hydrodata"
    if not os.path.exists("/hydrodata"):
        # Just skip test if this is run on a machine without /hydrodata access
        return

    entry = hf_hydrodata.gridded.get_catalog_entry(
        dataset="conus1_domain", file_type="pfb", variable="latitude", grid="conus1"
    )

    data = hf_hydrodata.gridded.get_ndarray(entry)
    assert data.shape == (1888, 3342)

    entry = hf_hydrodata.gridded.get_catalog_entry(
        dataset="conus2_domain", file_type="pfb", variable="latitude", grid="conus2"
    )
    data = hf_hydrodata.gridded.get_ndarray(entry)
    assert data.shape == (3256, 4442)


def test_grid_to_latlng():
    """Test grid_to_latlng."""

    hf_hydrodata.gridded.HYDRODATA = "/hydrodata"
    (lat, lng) = hf_hydrodata.grid.to_latlon("conus1", 0, 0)
    assert round(lat, 2) == 31.65
    assert round(lng, 2) == -115.98
    bounds = hf_hydrodata.grid.to_latlon("conus1", *[0, 0, 3341, 1887])
    assert round(bounds[2], 2) == 49.1
    assert round(bounds[3], 2) == -76.11
    (lat, lng) = hf_hydrodata.grid.to_latlon("conus1", 10.5, 10.5)
    assert round(lat, 6) == 31.764588
    assert round(lng, 6) == -115.898577
    (lat, lng) = hf_hydrodata.grid.to_latlon("conus1", 10.0, 10.0)
    assert round(lat, 6) == 31.759219
    assert round(lng, 6) == -115.902573
    (lat, lng) = hf_hydrodata.grid.to_latlon("conus2", 0, 0)
    assert round(lat, 2) == 22.36
    assert round(lng, 2) == -117.85


def test_latlng_to_grid():
    """Test grid_to_latlng."""

    hf_hydrodata.gridded.HYDRODATA = "/hydrodata"
    (x, y) = hf_hydrodata.grid.from_latlon("conus1", 31.759219, -115.902573)
    assert round(x) == 10
    assert round(y) == 10
    grid_bounds = hf_hydrodata.grid.from_latlon(
        "conus1", 31.65, -115.98, 31.759219, -115.902573
    )
    assert round(grid_bounds[0]) == 0
    assert round(grid_bounds[1]) == 0
    grid_bounds = hf_hydrodata.grid.from_latlon(
        "conus2", 31.65, -115.98, 31.759219, -115.902573
    )
    assert round(grid_bounds[0]) == 441
    assert round(grid_bounds[1]) == 970

    (x, y) = hf_hydrodata.grid.from_latlon("conus1", 49.1423, -76.3369)
    assert round(x) == 3324
    assert round(y) == 1888


def test_get_huc_from_point():
    """Unit test for get_huc_from_latlng and get_huc_from_xy"""

    hf_hydrodata.gridded.HYDRODATA = "/hydrodata"
    grid = "conus1"
    (lat, lng) = hf_hydrodata.grid.to_latlon("conus1", 1078, 722)
    huc_id = hf_hydrodata.grid.get_huc_from_latlon(grid, 10, lat, lng)
    assert huc_id == "1019000404"

    huc_id = hf_hydrodata.grid.get_huc_from_xy(grid, 10, 1078, 722)
    assert huc_id == "1019000404"

    huc_id = hf_hydrodata.grid.get_huc_from_xy(grid, 10, 1078, 1999)
    assert huc_id is None


def test_get_huc_bbox_conus1():
    """Unit test for get_huc_bbox for conus1"""

    hf_hydrodata.gridded.HYDRODATA = "/hydrodata"
    with pytest.raises(ValueError):
        hf_hydrodata.gridded.get_huc_bbox("bad grid", ["1019000404"])
    with pytest.raises(ValueError):
        hf_hydrodata.gridded.get_huc_bbox("conus1", ["1019000404", "123"])

    bbox = hf_hydrodata.gridded.get_huc_bbox("conus1", ["1019000404"])
    assert bbox == (1076, 720, 1124, 739)

    bbox = hf_hydrodata.gridded.get_huc_bbox("conus1", ["1102001002", "1102001003"])
    assert bbox == (1088, 415, 1132, 453)


def test_get_huc_bbox_conus2():
    """Unit test for get_huc_bbox for conus2"""

    hf_hydrodata.gridded.HYDRODATA = "/hydrodata"
    bbox = hf_hydrodata.grid.get_huc_bbox("conus2", ["1019000404"])
    assert bbox == (1468, 1664, 1550, 1693)


def test_latlng_to_grid_out_of_bounds():
    """Unit tests for when latlng is out of bounds of conus1."""

    hf_hydrodata.gridded.HYDRODATA = "/hydrodata"
    with pytest.raises(ValueError):
        (_, _) = hf_hydrodata.grid.from_latlon("conus1", 90, -180)


def test_get_ndarray_no_entry_passed():
    """Test able to get and ndarray passing None for entry"""

    if not os.path.exists("/hydrodata"):
        # Just skip test if this is run on a machine without /hydrodata access
        return

    data = hf_hydrodata.gridded.get_ndarray(
        None,
        dataset="NLDAS2",
        file_type="pfb",
        period="daily",
        variable="precipitation",
        start_time="2006-01-01",
    )
    assert data.shape == (1, 1888, 3342)

    data = hf_hydrodata.gridded.get_ndarray(
        None,
        dataset="NLDAS2",
        file_type="pfb",
        period="daily",
        variable="precipitation",
        start_time="2006-01-01",
        end_time="2006-01-3",
    )
    assert data.shape == (2, 1888, 3342)

    data = hf_hydrodata.gridded.get_ndarray(
        None,
        dataset="NLDAS2",
        file_type="pfb",
        period="hourly",
        variable="precipitation",
        start_time="2006-01-01",
        end_time="2006-01-03",
    )
    assert data.shape == (48, 1888, 3342)

    data = hf_hydrodata.gridded.get_ndarray(
        None,
        dataset="conus1_baseline_85",
        file_type="pfb",
        period="hourly",
        variable="pressure_head",
        start_time="2006-01-01",
        end_time="2006-01-3",
    )
    assert data.shape == (48, 5, 1888, 3342)

    path = hf_hydrodata.gridded.get_file_path(
        None,
        dataset="NLDAS2",
        file_type="pfb",
        period="daily",
        variable="precipitation",
        start_time="2006-01-01",
    )
    assert "sum.093.pfb" in path


def test_get_numpy_monthly():
    """Test getting monthly files."""

    options = {
        "dataset": "NLDAS2",
        "variable": "air_temp",
        "file_type": "pfb",
        "period": "monthly",
        "start_time": "2006-01-31",
        "end_time": "2006-03-01",
    }
    data = hf_hydrodata.gridded.get_numpy(options)
    assert data.shape == (2, 1888, 3342)


def test_get_numpy_daily():
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
    data = hf_hydrodata.gridded.get_numpy(options)
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
    data = hf_hydrodata.gridded.get_numpy(options)
    print(data.shape)


def test_get_entry_with_multiple_file_types():
    """Test getting a catalog entry that has multiple file types."""

    options = {
        "dataset": "NLDAS2",
        "variable": "precipitation",
        "period": "daily",
        "start_time": "2005-08-01",
    }

    entries = hf_hydrodata.gridded.get_catalog_entries(options)
    assert len(entries) > 1
    entry = hf_hydrodata.gridded.get_catalog_entry(options)
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
    data = hf_hydrodata.gridded.get_numpy(options)
    data = hf_hydrodata.gridded.get_numpy(
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
    data = hf_hydrodata.gridded.get_numpy(
        site_type="streamflow",
        dataset="observations",
        start_time="1978-08-01",
        end_time="1978-08-04",
        variable="anomaly",
        period="daily",
        site_id="06787000",
    )
    assert data.shape[0] == 3
    data = hf_hydrodata.gridded.get_numpy(
        site_type="streamflow",
        dataset="observations",
        start_time="1978-08-01",
        end_time="1978-08-15",
        variable="anomaly",
        period="weekly",
        site_id="06787000",
    )
    assert data.shape[0] == 2


def test_register_api():
    """Test register and and get and email pin stored in users home directory."""

    hf_hydrodata.gridded.register_api_pin("dummy@email.com", "0000")
    email, pin = hf_hydrodata.gridded.get_registered_api_pin()
    assert pin == "0000"
    assert email == "dummy@email.com"


def test_timezone():
    """Test with timezone in start_time/end_time"""

    hf_hydrodata.gridded.HYDRODATA = "/hydrodata"
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

    entry = hf_hydrodata.gridded.get_catalog_entry(
        dataset="NLDAS2",
        variable="air_temp",
        grid="conus1",
        file_type="pfb",
        period="hourly",
    )

    data = hf_hydrodata.gridded.get_ndarray(
        entry,
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

    (low, high) = hf_hydrodata.gridded.get_date_range(options)
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
        hf_hydrodata.gridded.get_catalog_entry(options)
    assert "variable = 'precipitation' or 'downward_longwave" in str(info.value)

def test_get_huc_from_point():
    """Unit test for get_huc_from_latlon and get_huc_from_xy"""

    grid = "conus1"
    (lat, lng) = hf_hydrodata.grid.to_latlon("conus1", 1078, 722)
    huc_id = hf_hydrodata.gridded.get_huc_from_latlon(grid, 10, lat, lng)
    assert huc_id == "1019000404"

    huc_id = hf_hydrodata.gridded.get_huc_from_xy(grid, 10, 1078, 722)
    assert huc_id == "1019000404"

    huc_id = hf_hydrodata.gridded.get_huc_from_xy(grid, 10, 1078, 1999)
    assert huc_id is None


def test_get_huc_bbox_conus1():
    """Unit test for get_huc_bbox for conus1"""

    with pytest.raises(ValueError):
        hf_hydrodata.gridded.get_huc_bbox("bad grid", ["1019000404"])
    with pytest.raises(ValueError):
        hf_hydrodata.gridded.get_huc_bbox("conus1", ["1019000404", "123"])

    bbox = hf_hydrodata.gridded.get_huc_bbox("conus1", ["1019000404"])
    assert bbox == (1076, 720, 1124, 739)

    bbox = hf_hydrodata.gridded.get_huc_bbox("conus1", ["1102001002", "1102001003"])
    assert bbox == (1088, 415, 1132, 453)


def test_get_huc_bbox_conus2():
    """Unit test for get_huc_bbox for conus2"""

    bbox = hf_hydrodata.gridded.get_huc_bbox("conus2", ["1019000404"])
    assert bbox == (1468, 1664, 1550, 1693)

if __name__ == "__main__":
    pytest.main([__file__])
