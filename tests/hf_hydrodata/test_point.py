"""Unit test for the /point."""

# pylint: disable=W0613,C0301,R0903,E0401,C0302,W0212,C0413,C0121
import sys
import os
import io
import pytest
import pandas as pd
import numpy as np

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../src")))

from hf_hydrodata import point
from hf_hydrodata.data_catalog import MaintenanceError

REMOTE_TEST_DATA_DIR = "/hydrodata/national_obs/tools/test_data"
LOCAL_TEST_DATA_DIR = os.path.join(os.path.dirname(__file__), "test_data")


class MockResponseMetadata:
    """Mock the flask.request response."""

    def __init__(self):
        data = {
            "headers": ["site_id", "2020-01-01", "2020-01-02"],
            "0": ["01019001", "18.39500", "18.36670"],
            "1": ["01027200", "4.92420", "4.64120"],
            "2": ["01029500", "35.09200", "33.67700"],
        }

        # Create a DataFrame with specified column names
        df = pd.DataFrame(data)
        buffer = io.BytesIO()
        df.to_pickle(buffer)
        data_bytes = buffer.getvalue()

        self.headers = {}
        self.status_code = 200
        self.content = data_bytes
        self.text = None
        self.checksum = ""


class MockResponse:
    """Mock the flask.request response."""

    def __init__(self):
        data = {
            "headers": ["site_id", "2020-01-01", "2020-01-02"],
            "0": ["01019000", "18.39500", "18.36670"],
            "1": ["01027200", "4.92420", "4.64120"],
            "2": ["01029500", "35.09200", "33.67700"],
        }

        # Create a DataFrame with specified column names
        df = pd.DataFrame(data)
        buffer = io.BytesIO()
        df.to_pickle(buffer)
        data_bytes = buffer.getvalue()

        self.headers = {}
        self.status_code = 200
        self.content = data_bytes
        self.text = None
        self.checksum = ""


class MockResponseSecurity:
    """Mock the flask.request response."""

    def __init__(self):
        data = b'{"email":"dummy@email.com","expires":"2130/10/14 18:31:11 GMT-0000","groups":["demo"],"jwt_token":"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJkdW1teSIsImVtYWlsIjoiZHVtbXlAZW1haWwuY29tIiwiZ3JvdXBzIjpbImRlbW8iXSwiZXhwIjoxNjk3MzA4MjcxfQ.Z6YJHZOlo3OdzdmuLHAqdaRIraH1Z-WzoKtXQSbh92w","user_id":"dummy"}'

        self.headers = {}
        self.status_code = 200
        self.content = data
        self.text = None
        self.checksum = ""


def mock_requests_get(point_data_url, headers, timeout=180):
    """Create a mock csv response."""

    if headers is None:
        response = MockResponseSecurity()
    else:
        response = MockResponse()

    return response


def mock_requests_get_metadata(point_data_url, headers, timeout=180):
    """Create a mock csv response."""

    if headers is None:
        response = MockResponseSecurity()
    else:
        response = MockResponseMetadata()

    return response


def test_get_dataframe():
    """Test code that allows api to access metadata remotely, with api
    calls mocked out."""

    # This test is removed and will be added later to be tested on remote server


def test_get_meta_dataframe():
    """Test code that allows api to access metadata remotely, with api
    calls mocked out."""

    # This test is removed and will be added later to be tested on remote server


def test_check_inputs():
    """Confirm utils.check_inputs fails for expected cases."""
    # Parameter provided for variable not in supported list (typo).
    point.HYDRODATA = "/hydrodata"
    with pytest.raises(Exception):
        point._check_inputs(
            dataset="usgs_nwis",
            variable="steamflow",
            temporal_resolution="daily",
            aggregation="mean",
        )

    # Parameter provided for temporal_resolution not in supported list.
    with pytest.raises(Exception):
        point._check_inputs(
            dataset="usgs_nwis",
            variable="streamflow",
            temporal_resolution="monthly",
            aggregation="mean",
        )

    # Variable requested is soil moisture but no depth level provided.
    with pytest.raises(Exception):
        point._check_inputs(
            dataset="scan",
            variable="soil_moisture",
            temporal_resolution="daily",
            aggregation="sod",
        )

    # Variable requested is soil moisture with unsupported depth level provided.
    with pytest.raises(Exception):
        point._check_inputs(
            dataset="scan",
            variable="soil_moisture",
            temporal_resolution="daily",
            aggregation="sod",
            depth_level=6,
        )


def test_filter_min_num_obs():
    """Test functionality for filtering DataFrame on minimum non-NaN values."""
    df = pd.DataFrame(
        {
            "site1": [1, 5, 3, 4],
            "site2": [np.nan, 4, 2, 9],
            "site3": [np.nan, 9, 2, np.nan],
        }
    )

    df1 = point._filter_min_num_obs(df, 1)
    assert list(df1.columns) == ["site1", "site2", "site3"]
    df2 = point._filter_min_num_obs(df, 2)
    assert list(df2.columns) == ["site1", "site2", "site3"]
    df3 = point._filter_min_num_obs(df, 3)
    assert list(df3.columns) == ["site1", "site2"]
    df4 = point._filter_min_num_obs(df, 4)
    assert list(df4.columns) == ["site1"]


def test_no_sites_error_message():
    """Test that error gets raised if not sites fit filters"""
    with pytest.raises(Exception):
        point.get_point_data(
            dataset="usgs_nwis",
            variable="water_table_depth",
            temporal_resolution="hourly",
            aggregation="mean",
            date_start="2002-01-01",
            date_end="2002-01-05",
            latitude_range=(49, 50),
            longitude_range=(-75, -74),
        )


def test_get_data_streamflow_daily():
    """Test for daily streamflow data"""
    df = point.get_point_data(
        dataset="usgs_nwis",
        variable="streamflow",
        temporal_resolution="daily",
        aggregation="mean",
        date_start="2002-01-01",
        date_end="2002-01-05",
        latitude_range=(47, 50),
        longitude_range=(-75, -50),
    )
    assert len(df) == 5
    assert "01011000" in df.columns


def test_get_data_streamflow_daily_dict():
    """Test for daily streamflow data using input dictionary"""
    df = point.get_point_data(
        {
            "dataset": "usgs_nwis",
            "variable": "streamflow",
            "temporal_resolution": "daily",
            "aggregation": "mean",
            "date_start": "2002-01-01",
            "date_end": "2002-01-05",
            "latitude_range": (47, 50),
            "longitude_range": (-75, -50),
        }
    )
    assert len(df) == 5
    assert "01011000" in df.columns


def test_get_data_streamflow_hourly():
    """Test for hourly streamflow data"""
    df = point.get_point_data(
        dataset="usgs_nwis",
        variable="streamflow",
        temporal_resolution="hourly",
        aggregation="mean",
        date_start="2002-01-01",
        date_end="2002-01-05",
        latitude_range=(45, 50),
        longitude_range=(-75, -50),
    )
    assert len(df) == 97
    assert "01011000" in df.columns


def test_get_data_wtd_daily():
    """Test for daily wtd data"""
    df = point.get_point_data(
        dataset="usgs_nwis",
        variable="water_table_depth",
        temporal_resolution="daily",
        aggregation="mean",
        date_start="2002-01-01",
        date_end="2002-01-05",
        latitude_range=(45, 50),
        longitude_range=(-75, -50),
    )
    assert len(df) == 5
    assert "453629068531801" in df.columns


def test_get_data_wtd_hourly():
    """Test for hourly wtd data"""
    df = point.get_point_data(
        dataset="usgs_nwis",
        variable="water_table_depth",
        temporal_resolution="hourly",
        aggregation="mean",
        date_start="2002-01-01",
        date_end="2002-01-05",
        latitude_range=(30, 40),
        longitude_range=(-120, -110),
    )
    assert len(df) == 97
    assert "343409111511101" in df.columns


def test_get_data_wtd_instantaneous():
    """Test for instantaneous wtd data"""
    df = point.get_point_data(
        dataset="usgs_nwis",
        variable="water_table_depth",
        temporal_resolution="instantaneous",
        aggregation="-",
        date_start="2002-01-01",
        date_end="2002-01-01",
        latitude_range=(30, 35),
        longitude_range=(-120, -115),
    )
    assert len(df) >= 42
    assert len(df) < 100
    assert "323709080324809" not in list(df["site_id"])
    assert "340722116260301" in list(df["site_id"])
    assert set(list(df["date"])) == {"2002-01-01"}


def test_get_metadata_wtd_instantaneous():
    """Test for instantaneous wtd metadata"""
    df = point.get_point_data(
        dataset="usgs_nwis",
        variable="water_table_depth",
        temporal_resolution="instantaneous",
        aggregation="-",
        date_start="2002-01-01",
        date_end="2002-01-01",
        latitude_range=(30, 35),
        longitude_range=(-120, -115),
    )

    metadata_df = point.get_point_metadata(
        dataset="usgs_nwis",
        variable="water_table_depth",
        temporal_resolution="instantaneous",
        aggregation="-",
        date_start="2002-01-01",
        date_end="2002-01-01",
        latitude_range=(30, 35),
        longitude_range=(-120, -115),
    )
    assert len(df["site_id"].unique()) == len(metadata_df)
    assert "323709080324809" not in list(metadata_df["site_id"])
    assert "340722116260301" in list(metadata_df["site_id"])


def test_get_data_swe_daily():
    """Test for daily swe data"""
    df = point.get_point_data(
        dataset="snotel",
        variable="swe",
        temporal_resolution="daily",
        aggregation="sod",
        date_start="2022-01-01",
        date_end="2022-01-04",
        latitude_range=(39, 40),
        longitude_range=(-120, -119),
    )
    assert len(df) == 4
    assert "340:NV:SNTL" in df.columns


def test_get_data_precipitation_daily():
    """Tests for daily precipitation data"""
    accumulated_df = point.get_point_data(
        dataset="snotel",
        variable="precipitation",
        temporal_resolution="daily",
        aggregation="accumulated",
        date_start="2022-01-01",
        date_end="2022-01-04",
        latitude_range=(39, 40),
        longitude_range=(-120, -119),
    )
    assert len(accumulated_df) == 4
    assert "340:NV:SNTL" in accumulated_df.columns

    total_df = point.get_point_data(
        dataset="snotel",
        variable="precipitation",
        temporal_resolution="daily",
        aggregation="sum",
        date_start="2022-01-01",
        date_end="2022-01-04",
        latitude_range=(39, 40),
        longitude_range=(-120, -119),
    )
    assert len(total_df) == 4
    assert "340:NV:SNTL" in total_df.columns
    assert (accumulated_df["340:NV:SNTL"] > total_df["340:NV:SNTL"]).all() == True

    total_adj_df = point.get_point_data(
        dataset="snotel",
        variable="precipitation",
        temporal_resolution="daily",
        aggregation="sum_snow_adjusted",
        date_start="2022-01-01",
        date_end="2022-01-04",
        latitude_range=(39, 40),
        longitude_range=(-120, -119),
    )
    assert len(total_adj_df) == 4
    assert "340:NV:SNTL" in total_adj_df.columns
    assert (total_adj_df["340:NV:SNTL"] >= total_df["340:NV:SNTL"]).all() == True


def test_get_data_temperature_daily():
    """Tests for daily temperature data"""
    min_df = point.get_point_data(
        dataset="snotel",
        variable="air_temp",
        temporal_resolution="daily",
        aggregation="min",
        date_start="2022-01-01",
        date_end="2022-01-04",
        latitude_range=(39, 40),
        longitude_range=(-120, -119),
    )
    assert len(min_df) == 4
    assert "340:NV:SNTL" in min_df.columns

    max_df = point.get_point_data(
        dataset="snotel",
        variable="air_temp",
        temporal_resolution="daily",
        aggregation="max",
        date_start="2022-01-01",
        date_end="2022-01-04",
        latitude_range=(39, 40),
        longitude_range=(-120, -119),
    )
    assert len(max_df) == 4
    assert "340:NV:SNTL" in max_df.columns

    mean_df = point.get_point_data(
        dataset="snotel",
        variable="air_temp",
        temporal_resolution="daily",
        aggregation="mean",
        date_start="2022-01-01",
        date_end="2022-01-04",
        latitude_range=(39, 40),
        longitude_range=(-120, -119),
    )
    assert len(mean_df) == 4
    assert "340:NV:SNTL" in mean_df.columns

    assert (min_df["340:NV:SNTL"] <= max_df["340:NV:SNTL"]).all() == True
    assert (min_df["340:NV:SNTL"] <= mean_df["340:NV:SNTL"]).all() == True
    assert (mean_df["340:NV:SNTL"] <= max_df["340:NV:SNTL"]).all() == True


def test_get_soil_moisture_fail():
    """Make sure failure if required depth_level parameter not supplied."""
    with pytest.raises(Exception):
        point.get_point_data(
            dataset="scan",
            variable="soil_moisture",
            temporal_resolution="daily",
            aggregation="sod",
            date_start="2002-01-01",
            date_end="2002-01-05",
            latitude_range=(49, 50),
            longitude_range=(-75, -74),
        )


def test_get_data_soil_moisture_daily():
    """Tests for daily soil moisture data"""
    df_2 = point.get_point_data(
        dataset="snotel",
        variable="soil_moisture",
        temporal_resolution="daily",
        aggregation="sod",
        depth_level=2,
        date_start="2022-01-01",
        date_end="2022-01-03",
        latitude_range=(39, 40),
        longitude_range=(-120, -119),
    )
    assert len(df_2) == 3
    assert "1242:NV:SNTL" in df_2.columns

    df_4 = point.get_point_data(
        dataset="scan",
        variable="soil_moisture",
        temporal_resolution="daily",
        aggregation="sod",
        depth_level=4,
        date_start="2022-01-01",
        date_end="2022-01-03",
        latitude_range=(30, 40),
        longitude_range=(-120, -119),
    )
    assert len(df_4) == 3
    assert "2189:CA:SCAN" in df_4.columns

    df_8 = point.get_point_data(
        dataset="snotel",
        variable="soil_moisture",
        temporal_resolution="daily",
        aggregation="sod",
        depth_level=8,
        date_start="2022-01-01",
        date_end="2022-01-03",
        latitude_range=(39, 40),
        longitude_range=(-120, -119),
    )
    assert len(df_8) == 3
    assert "1242:NV:SNTL" in df_8.columns

    df_20 = point.get_point_data(
        dataset="snotel",
        variable="soil_moisture",
        temporal_resolution="daily",
        aggregation="sod",
        depth_level=20,
        date_start="2022-01-01",
        date_end="2022-01-03",
        latitude_range=(39, 40),
        longitude_range=(-120, -119),
    )
    assert len(df_20) == 3
    assert "1242:NV:SNTL" in df_20.columns

    df_40 = point.get_point_data(
        dataset="scan",
        variable="soil_moisture",
        temporal_resolution="daily",
        aggregation="sod",
        depth_level=40,
        date_start="2022-01-01",
        date_end="2022-01-03",
        latitude_range=(30, 40),
        longitude_range=(-120, -119),
    )
    assert len(df_40) == 3
    assert "2189:CA:SCAN" in df_40.columns


def test_get_data_latent_heat_flux():
    """Tests for hourly latent heat flux data"""
    df = point.get_point_data(
        dataset="ameriflux",
        variable="latent_heat",
        temporal_resolution="hourly",
        aggregation="sum",
        date_start="2022-01-01",
        date_end="2022-01-02",
        latitude_range=(30, 40),
        longitude_range=(-120, -119),
    )
    assert len(df) == 25
    assert "US-xSJ" in df.columns


def test_get_data_sensible_heat_flux():
    """Tests for hourly sensible heat flux data"""
    df = point.get_point_data(
        dataset="ameriflux",
        variable="sensible_heat",
        temporal_resolution="hourly",
        aggregation="sum",
        date_start="2022-01-01",
        date_end="2022-01-02",
        latitude_range=(30, 40),
        longitude_range=(-120, -119),
    )
    assert len(df) == 25
    assert "US-xSJ" in df.columns


def test_get_data_sradiation():
    """Tests for hourly shortwave radiation data"""
    df = point.get_point_data(
        dataset="ameriflux",
        variable="downward_shortwave",
        temporal_resolution="hourly",
        aggregation="mean",
        date_start="2022-01-01",
        date_end="2022-01-02",
        latitude_range=(30, 40),
        longitude_range=(-120, -119),
    )
    assert len(df) == 25
    assert "US-xSJ" in df.columns


def test_get_data_lradiation():
    """Tests for hourly longwave radiation data"""
    df = point.get_point_data(
        dataset="ameriflux",
        variable="downward_longwave",
        temporal_resolution="hourly",
        aggregation="mean",
        date_start="2022-01-01",
        date_end="2022-01-02",
        latitude_range=(30, 40),
        longitude_range=(-120, -119),
    )
    assert len(df) == 25
    assert "US-xSJ" in df.columns


def test_get_data_vpd():
    """Tests for hourly vapor pressure deficit data"""
    df = point.get_point_data(
        dataset="ameriflux",
        variable="vapor_pressure_deficit",
        temporal_resolution="hourly",
        aggregation="mean",
        date_start="2022-01-01",
        date_end="2022-01-02",
        latitude_range=(30, 40),
        longitude_range=(-120, -119),
    )
    assert len(df) == 25
    assert "US-xSJ" in df.columns


def test_get_data_temperature_fluxnet():
    """Tests for hourly temperature data"""
    df = point.get_point_data(
        dataset="ameriflux",
        variable="air_temp",
        temporal_resolution="hourly",
        aggregation="mean",
        date_start="2022-01-01",
        date_end="2022-01-02",
        latitude_range=(30, 40),
        longitude_range=(-120, -119),
    )
    assert len(df) == 25
    assert "US-xSJ" in df.columns


def test_get_data_wind_speed():
    """Tests for hourly wind speed data"""
    df = point.get_point_data(
        dataset="ameriflux",
        variable="wind_speed",
        temporal_resolution="hourly",
        aggregation="mean",
        date_start="2022-01-01",
        date_end="2022-01-02",
        latitude_range=(30, 40),
        longitude_range=(-120, -119),
    )
    assert len(df) == 25
    assert "US-xSJ" in df.columns


def test_get_metadata_streamflow():
    """Test for streamflow metadata"""
    metadata_df = point.get_point_metadata(
        dataset="usgs_nwis",
        variable="streamflow",
        temporal_resolution="daily",
        aggregation="mean",
        date_start="2002-01-01",
        date_end="2002-01-05",
        latitude_range=(47, 50),
        longitude_range=(-75, -50),
    )
    assert len(metadata_df) == 4
    assert len(metadata_df.columns) == 25
    assert "01011000" in list(metadata_df["site_id"])


def test_get_metadata_streamflow_dict():
    """Test for streamflow metadata using dictionary input"""
    metadata_df = point.get_point_metadata(
        {
            "dataset": "usgs_nwis",
            "variable": "streamflow",
            "temporal_resolution": "daily",
            "aggregation": "mean",
            "date_start": "2002-01-01",
            "date_end": "2002-01-05",
            "latitude_range": (47, 50),
            "longitude_range": (-75, -50),
        }
    )
    assert len(metadata_df) == 4
    assert len(metadata_df.columns) == 25
    assert "01011000" in list(metadata_df["site_id"])


def test_get_metadata_streamflow_hourly():
    """Test for streamflow metadata, hourly"""
    metadata_df = point.get_point_metadata(
        dataset="usgs_nwis",
        variable="streamflow",
        temporal_resolution="hourly",
        aggregation="mean",
        date_start="2002-01-01",
        date_end="2002-01-05",
        latitude_range=(47, 50),
        longitude_range=(-75, -50),
    )
    assert len(metadata_df) == 4
    assert len(metadata_df.columns) == 25
    assert "01011000" in list(metadata_df["site_id"])


def test_get_metadata_wtd():
    """Test for wtd metadata"""
    metadata_df = point.get_point_metadata(
        dataset="usgs_nwis",
        variable="water_table_depth",
        temporal_resolution="daily",
        aggregation="mean",
        date_start="2002-01-01",
        date_end="2002-01-05",
        latitude_range=(47, 50),
        longitude_range=(-75, -50),
    )
    assert len(metadata_df) == 1
    assert len(metadata_df.columns) == 25
    assert "471457068353001" in list(metadata_df["site_id"])


def test_get_metadata_swe():
    """Test for swe metadata"""
    metadata_df = point.get_point_metadata(
        dataset="snotel",
        variable="swe",
        temporal_resolution="daily",
        aggregation="sod",
        date_start="2002-01-01",
        date_end="2002-01-05",
        latitude_range=(39, 40),
        longitude_range=(-120, -119),
    )
    assert len(metadata_df) == 3
    assert len(metadata_df.columns) == 20
    assert "340:NV:SNTL" in list(metadata_df["site_id"])


def test_get_metadata_flux():
    """Test for ameriflux metadata"""
    metadata_df = point.get_point_metadata(
        dataset="ameriflux",
        variable="latent_heat",
        temporal_resolution="hourly",
        aggregation="sum",
        date_start="2022-01-01",
        date_end="2022-01-02",
        latitude_range=(30, 40),
        longitude_range=(-120, -119),
    )
    assert len(metadata_df) == 3
    assert len(metadata_df.columns) == 37
    assert "US-xSJ" in list(metadata_df["site_id"])


def test_get_data_state_filter():
    """Test for using state filter"""
    df = point.get_point_data(
        dataset="usgs_nwis",
        variable="streamflow",
        temporal_resolution="daily",
        aggregation="mean",
        date_start="2002-01-01",
        date_end="2002-01-05",
        state="ME",
    )
    assert len(df) == 5
    assert "01011000" in df.columns
    assert len(df.columns) >= 64


def test_get_data_site_filter_list():
    """Test for using site_ids filter with a list"""
    df = point.get_point_data(
        dataset="usgs_nwis",
        variable="streamflow",
        temporal_resolution="daily",
        aggregation="mean",
        date_start="2002-01-01",
        date_end="2002-01-05",
        site_ids=["01011000", "01013500"],
    )
    assert len(df) == 5
    assert list(df.columns == ["date", "01011000", "01013500"])


def test_get_data_site_filter_str():
    """Test for using site_ids filter with a string"""
    df = point.get_point_data(
        dataset="usgs_nwis",
        variable="streamflow",
        temporal_resolution="daily",
        aggregation="mean",
        date_start="2002-01-01",
        date_end="2002-01-05",
        site_ids="01011000",
    )
    assert len(df) == 5
    assert list(df.columns == ["date", "01011000"])


def test_get_data_site_filter_fail():
    """Test for using site_ids filter with numeric fails"""
    with pytest.raises(Exception):
        point.get_point_data(
            dataset="usgs_nwis",
            variable="streamflow",
            temporal_resolution="daily",
            aggregation="mean",
            date_start="2002-01-01",
            date_end="2002-01-05",
            site_ids=1011000,
        )


def test_site_networks_filter_list():
    """Test for using site_networks filter as a list"""
    data_df = point.get_point_data(
        dataset="usgs_nwis",
        variable="streamflow",
        temporal_resolution="daily",
        aggregation="mean",
        date_start="2002-01-01",
        date_end="2002-01-05",
        state="NJ",
        latitude_range=(40, 41),
        longitude_range=(-75, -74),
        site_networks=["gagesii"],
    )
    assert len(data_df) == 5
    assert "01377500" in data_df.columns

    metadata_df = point.get_point_metadata(
        dataset="usgs_nwis",
        variable="streamflow",
        temporal_resolution="daily",
        aggregation="mean",
        date_start="2002-01-01",
        date_end="2002-01-05",
        state="NJ",
        latitude_range=(40, 41),
        longitude_range=(-75, -74),
        site_networks=["gagesii"],
    )
    assert len(metadata_df) == 60


def test_site_networks_filter_nwm():
    """Test for using site_networks filter with nwm list"""
    nwm_sites_metadata = point.get_point_metadata(
        dataset="usgs_nwis",
        variable="streamflow",
        temporal_resolution="daily",
        aggregation="mean",
        date_start="2002-01-01",
        date_end="2002-01-05",
        state="NJ",
        latitude_range=(40, 41),
        longitude_range=(-75, -74),
        site_networks=["nwm"],
    )
    assert len(nwm_sites_metadata) == 60

    all_sites_metadata = point.get_point_metadata(
        dataset="usgs_nwis",
        variable="streamflow",
        temporal_resolution="daily",
        aggregation="mean",
        date_start="2002-01-01",
        date_end="2002-01-05",
        state="NJ",
        latitude_range=(40, 41),
        longitude_range=(-75, -74),
    )
    assert len(all_sites_metadata) == 65


def test_site_networks_filter_list_wtd():
    """Test for using site_networks filter as a list with water table depth variable"""
    data_df = point.get_point_data(
        dataset="usgs_nwis",
        variable="water_table_depth",
        temporal_resolution="daily",
        aggregation="mean",
        date_start="2002-01-01",
        date_end="2002-01-05",
        state="NJ",
        latitude_range=(40, 41),
        longitude_range=(-75, -74),
        site_networks=["climate_response_network"],
    )
    assert len(data_df) == 5
    assert "400232074213201" in data_df.columns

    metadata_df = point.get_point_metadata(
        dataset="usgs_nwis",
        variable="water_table_depth",
        temporal_resolution="daily",
        aggregation="mean",
        date_start="2002-01-01",
        date_end="2002-01-05",
        state="NJ",
        latitude_range=(40, 41),
        longitude_range=(-75, -74),
        site_networks=["climate_response_network"],
    )
    assert len(metadata_df) == 9


def test_site_networks_filter_str():
    """Test for using site_networks filter as a str"""
    data_df = point.get_point_data(
        dataset="usgs_nwis",
        variable="streamflow",
        temporal_resolution="daily",
        aggregation="mean",
        date_start="2002-01-01",
        date_end="2002-01-05",
        state="NJ",
        latitude_range=(40, 41),
        longitude_range=(-75, -74),
        site_networks="gagesii",
    )
    assert len(data_df) == 5
    assert "01377500" in data_df.columns

    metadata_df = point.get_point_metadata(
        dataset="usgs_nwis",
        variable="streamflow",
        temporal_resolution="daily",
        aggregation="mean",
        date_start="2002-01-01",
        date_end="2002-01-05",
        state="NJ",
        latitude_range=(40, 41),
        longitude_range=(-75, -74),
        site_networks="gagesii",
    )
    assert len(metadata_df) == 60


def test_site_networks_filter_str_wtd():
    """Test for using site_networks filter as a string with water table depth variable"""
    data_df = point.get_point_data(
        dataset="usgs_nwis",
        variable="water_table_depth",
        temporal_resolution="daily",
        aggregation="mean",
        date_start="2002-01-01",
        date_end="2002-01-05",
        state="NJ",
        latitude_range=(40, 41),
        longitude_range=(-75, -74),
        site_networks="climate_response_network",
    )
    assert len(data_df) == 5
    assert "400232074213201" in data_df.columns

    metadata_df = point.get_point_metadata(
        dataset="usgs_nwis",
        variable="water_table_depth",
        temporal_resolution="daily",
        aggregation="mean",
        date_start="2002-01-01",
        date_end="2002-01-05",
        state="NJ",
        latitude_range=(40, 41),
        longitude_range=(-75, -74),
        site_networks="climate_response_network",
    )
    assert len(metadata_df) == 9


def test_get_data_min_num_obs_filter():
    """Test for using min_num_obs filter"""
    df = point.get_point_data(
        dataset="usgs_nwis",
        variable="streamflow",
        temporal_resolution="daily",
        aggregation="mean",
        date_start="2002-01-01",
        date_end="2002-01-05",
        site_ids=["01377500", "01378500", "01445000"],
        min_num_obs=5,
    )
    assert list(df.columns) == ["date", "01377500", "01378500"]

    df = point.get_point_data(
        dataset="usgs_nwis",
        variable="streamflow",
        temporal_resolution="daily",
        aggregation="mean",
        date_start="2002-01-01",
        date_end="2002-01-05",
        site_ids=["01377500", "01378500", "01445000"],
        min_num_obs=1,
    )
    assert list(df.columns) == ["date", "01377500", "01378500"]

    # If no min_num_obs filter supplied, all three sites returned
    df = point.get_point_data(
        dataset="usgs_nwis",
        variable="streamflow",
        temporal_resolution="daily",
        aggregation="mean",
        date_start="2002-01-01",
        date_end="2002-01-05",
        site_ids=["01377500", "01378500", "01445000"],
    )
    assert list(df.columns) == ["date", "01377500", "01378500", "01445000"]


def test_polygon_filter_data_remote():
    """Test data filter for accepting a shapefile when the file is local to an end user and remote
    to /hydrodata and the API."""
    df = point.get_point_data(
        dataset="usgs_nwis",
        variable="streamflow",
        temporal_resolution="daily",
        aggregation="mean",
        date_start="2002-01-01",
        date_end="2002-01-05",
        polygon=f"{str(LOCAL_TEST_DATA_DIR)}/raritan_watershed.shp",
        polygon_crs="""GEOGCS["GCS_North_American_1983",
                        DATUM["D_North_American_1983",
                        SPHEROID["GRS_1980",6378137.0,298.257222101]],
                        PRIMEM["Greenwich",0.0],
                        UNIT["Degree",0.0174532925199433]]""",
    )
    assert len(df) == 5
    assert len(df.columns) >= 25
    assert "01401000" in df.columns


def test_polygon_filter_metadata_remote():
    """Test metadata filter for accepting a shapefile when the file is local to an end user and remote
    to /hydrodata and the API."""
    metadata_df = point.get_point_metadata(
        dataset="usgs_nwis",
        variable="streamflow",
        temporal_resolution="daily",
        aggregation="mean",
        date_start="2002-01-01",
        date_end="2002-01-05",
        polygon=f"{str(LOCAL_TEST_DATA_DIR)}/raritan_watershed.shp",
        polygon_crs="""GEOGCS["GCS_North_American_1983",
                        DATUM["D_North_American_1983",
                        SPHEROID["GRS_1980",6378137.0,298.257222101]],
                        PRIMEM["Greenwich",0.0],
                        UNIT["Degree",0.0174532925199433]]""",
    )
    assert len(metadata_df) >= 24
    assert "01401000" in list(metadata_df["site_id"])


def test_polygon_filter_fail():
    """Ensure polygon processing fails if no polygon_crs provided"""
    with pytest.raises(Exception):
        point.get_point_data(
            dataset="usgs_nwis",
            variable="streamflow",
            temporal_resolution="daily",
            aggregation="mean",
            date_start="2002-01-01",
            date_end="2002-01-05",
            polygon=f"{str(LOCAL_TEST_DATA_DIR)}/raritan_watershed.shp",
        )


def test_get_variables_lat_lon():
    """Test get_site_variables function with lat/lon filter"""

    df = point.get_site_variables(latitude_range=(47, 50), longitude_range=(-75, -60))

    # Bounds are flexible for if more sites are added
    assert (len(df) >= 36) & (len(df) <= 85)
    assert "01011000" in list(df["site_id"])
    assert "stream gauge" in list(df["site_type"])
    assert "groundwater well" in list(df["site_type"])


def test_get_variables_lat_lon_dict():
    """Test get_site_variables function with lat/lon filter"""
    query_parameters = {"latitude_range": "(47, 50)", "longitude_range": "(-75, -60)"}

    df = point.get_site_variables(query_parameters)

    # Bounds are flexible for if more sites are added
    assert (len(df) >= 36) & (len(df) <= 85)
    assert "01011000" in list(df["site_id"])
    assert "stream gauge" in list(df["site_type"])
    assert "groundwater well" in list(df["site_type"])


def test_get_variables_variable_filter():
    """Test get_site_variables function with variable filter"""
    df = point.get_site_variables(
        latitude_range=(47, 50), longitude_range=(-75, -60), variable="streamflow"
    )

    # Bounds are flexible for if more sites are added
    assert (len(df) >= 10) & (len(df) <= 15)
    assert "01011000" in list(df["site_id"])
    assert "stream gauge" in list(df["site_type"])
    assert "groundwater well" not in list(df["site_type"])


def test_get_variables_temporal_filter():
    """Test get_site_variables function with temporal_resolution filter"""
    df = point.get_site_variables(
        latitude_range=(47, 50),
        longitude_range=(-75, -60),
        variable="streamflow",
        temporal_resolution="daily",
    )

    # Bounds are flexible for if more sites are added
    assert (len(df) >= 6) & (len(df) <= 9)
    assert "01011000" in list(df["site_id"])
    assert "stream gauge" in list(df["site_type"])
    assert "groundwater well" not in list(df["site_type"])
    assert "Daily average streamflow" in list(df["variable_name"])
    assert "Hourly average streamflow" not in list(df["variable_name"])


def test_get_variables_source_filter():
    """Test get_site_variables function with dataset filter"""
    df = point.get_site_variables(
        latitude_range=(39, 40), longitude_range=(-120, -119), dataset="snotel"
    )

    # Bounds are flexible for if more sites are added
    assert (len(df) >= 40) & (len(df) <= 50)
    assert "340:NV:SNTL" in list(df["site_id"])
    assert "SNOTEL station" in list(df["site_type"])
    assert "groundwater well" not in list(df["site_type"])


def test_get_variables_aggregation_filter():
    """Test get_site_variables function with aggregation filter"""
    df = point.get_site_variables(
        latitude_range=(39, 40),
        longitude_range=(-120, -119),
        dataset="snotel",
        variable="air_temp",
        aggregation="mean",
    )

    # Bounds are flexible for if more sites are added
    assert (len(df) >= 4) & (len(df) <= 8)
    assert "340:NV:SNTL" in list(df["site_id"])
    assert "Daily average temperature" in list(df["variable_name"])
    assert "Daily minimum temperature" not in list(df["variable_name"])


def test_get_variables_date_filter():
    """Test get_site_variables function with date filters"""
    df = point.get_site_variables(
        latitude_range=(47, 50),
        longitude_range=(-75, -60),
        date_start="2015-01-01",
        date_end="2015-12-31",
    )

    # Bounds are flexible for if more sites are added
    assert (len(df) >= 11) & (len(df) <= 16)
    assert "01011000" in list(df["site_id"])
    assert "stream gauge" in list(df["site_type"])
    assert "groundwater well" in list(df["site_type"])


def test_get_variables_site_filter_list():
    """Test get_site_variables function with site_ids filter as a list"""
    df = point.get_site_variables(site_ids=["01011000", "01013500"])

    # Bounds are flexible for if more sites are added
    assert len(df) == 4
    assert "01011000" in list(df["site_id"])
    assert "01013500" in list(df["site_id"])
    assert "Daily average streamflow" in list(df["variable_name"])
    assert "Hourly average streamflow" in list(df["variable_name"])


def test_get_variables_site_filter_str():
    """Test get_site_variables function with site_ids filter as a string"""
    df = point.get_site_variables(site_ids="01011000")

    # Bounds are flexible for if more sites are added
    assert len(df) == 2
    assert "01011000" in list(df["site_id"])
    assert "Daily average streamflow" in list(df["variable_name"])
    assert "Hourly average streamflow" in list(df["variable_name"])


def test_get_variables_state_filter():
    """Test get_site_variables function with state filter"""
    df = point.get_site_variables(
        latitude_range=(45, 50), longitude_range=(-75, -60), state="VT"
    )

    # Bounds are flexible for if more sites are added
    assert len(df) == 4
    assert "04293430" in list(df["site_id"])
    assert "04294300" in list(df["site_id"])
    assert "Daily average streamflow" in list(df["variable_name"])
    assert "Hourly average streamflow" in list(df["variable_name"])


def test_get_variables_networks_filter_fail():
    """Test failure code returned if not enough information provided with site_networks filter"""
    with pytest.raises(Exception):
        point.get_site_variables(state="NJ", site_networks="gagesii")


def test_get_variables_networks_filter_list():
    """Test get_site_variables function with site_networks filter as a list"""
    df = point.get_site_variables(
        state="NJ",
        dataset="usgs_nwis",
        variable="streamflow",
        temporal_resolution="daily",
        site_networks=["gagesii"],
    )

    assert len(df) == 121
    assert "01367800" in list(df["site_id"])


def test_get_variables_networks_filter_str():
    """Test get_site_variables function with site_networks filter as a string"""
    df = point.get_site_variables(
        state="NJ",
        dataset="usgs_nwis",
        variable="streamflow",
        temporal_resolution="daily",
        site_networks="gagesii",
    )

    assert len(df) == 121
    assert "01367800" in list(df["site_id"])


def test_get_variables_polygon_filter():
    """Test get_variables filter for accepting a shapefile when the file is local to an end user and remote
    to /hydrodata and the API."""
    df = point.get_site_variables(
        state="NJ",
        variable="streamflow",
        temporal_resolution="daily",
        polygon=f"{str(LOCAL_TEST_DATA_DIR)}/raritan_watershed.shp",
        polygon_crs="""GEOGCS["GCS_North_American_1983",
                        DATUM["D_North_American_1983",
                        SPHEROID["GRS_1980",6378137.0,298.257222101]],
                        PRIMEM["Greenwich",0.0],
                        UNIT["Degree",0.0174532925199433]]""",
    )
    assert len(df) >= 56
    assert "01396091" in list(df["site_id"])


def test_fail_data_parameter_missing():
    """Test that error gets raised if required parameters are not included"""
    with pytest.raises(Exception):
        point.get_point_data(
            variable="water_table_depth",
            temporal_resolution="hourly",
            aggregation="mean",
            date_start="2002-01-01",
            date_end="2002-01-05",
            latitude_range=(49, 50),
            longitude_range=(-75, -74),
        )


def test_fail_metadata_parameter_missing():
    """Test that error gets raised if required parameters are not included"""
    with pytest.raises(Exception):
        point.get_point_metadata(
            dataset="usgs_nwis",
            variable="water_table_depth",
            aggregation="mean",
            date_start="2002-01-01",
            date_end="2002-01-05",
            latitude_range=(49, 50),
            longitude_range=(-75, -74),
        )


def test_fail_unsupported_parameter1():
    """Test that error gets raised if unrecognized parameter supplied."""
    with pytest.raises(Exception):
        point.get_point_metadata(
            dataset="usgs_nwis",
            variable="water_table_depth",
            aggregation="mean",
            date_start="2002-01-01",
            date_end="2002-01-05",
            latituderange=(49, 50),
            longituderange=(-75, -74),
        )


def test_fail_unsupported_parameter2():
    """Test that error gets raised if unrecognized parameter supplied."""
    with pytest.raises(Exception):
        point.get_point_metadata(
            dataset="usgs_nwis",
            varaible="water_table_depth",
            aggregation="mean",
            date_start="2002-01-01",
            date_end="2002-01-05",
            latitude_range=(49, 50),
            longitude_range=(-75, -74),
        )


def test_fail_unsupported_parameter3():
    """Test that error gets raised if unrecognized parameter supplied."""
    with pytest.raises(Exception):
        point.get_point_metadata(
            dataset="usgs_nwis",
            variable="water_table_depth",
            aggregation="mean",
            date_start="2002-01-01",
            date_end="2002-01-05",
            latitude_range=(49, 50),
            longitude_range=(-75, -74),
            min_num_obsv=1,
        )


def test_grid_bounds_conus1_list():
    """Test for using grid_bounds filter with a list"""
    df = point.get_point_data(
        dataset="usgs_nwis",
        variable="streamflow",
        temporal_resolution="daily",
        aggregation="mean",
        date_start="2002-01-01",
        date_end="2002-01-05",
        grid="conus1",
        grid_bounds=[1000, 450, 1200, 650],
    )
    assert df.shape[1] >= 27
    assert df.shape[1] <= 35
    assert "08249000" in df.columns
    assert "07093700" in df.columns

    metadata_df = point.get_point_metadata(
        dataset="usgs_nwis",
        variable="streamflow",
        temporal_resolution="daily",
        aggregation="mean",
        date_start="2002-01-01",
        date_end="2002-01-05",
        grid="conus1",
        grid_bounds=[1000, 450, 1200, 650],
    )
    assert metadata_df.shape[0] >= 26
    assert metadata_df.shape[0] <= 34
    assert "08249000" in list(metadata_df["site_id"])
    assert "07093700" in list(metadata_df["site_id"])


def test_grid_bounds_conus1_dict():
    """Test for using grid_bounds filter as a dictionary parameter"""
    df = point.get_point_data(
        {
            "dataset": "usgs_nwis",
            "variable": "streamflow",
            "temporal_resolution": "daily",
            "aggregation": "mean",
            "date_start": "2002-01-01",
            "date_end": "2002-01-05",
            "grid": "conus1",
            "grid_bounds": [1000, 450, 1200, 650],
        }
    )
    assert df.shape[1] >= 27
    assert df.shape[1] <= 35
    assert "08249000" in df.columns
    assert "07093700" in df.columns

    metadata_df = point.get_point_metadata(
        {
            "dataset": "usgs_nwis",
            "variable": "streamflow",
            "temporal_resolution": "daily",
            "aggregation": "mean",
            "date_start": "2002-01-01",
            "date_end": "2002-01-05",
            "grid": "conus1",
            "grid_bounds": [1000, 450, 1200, 650],
        }
    )
    assert metadata_df.shape[0] >= 26
    assert metadata_df.shape[0] <= 34
    assert "08249000" in list(metadata_df["site_id"])
    assert "07093700" in list(metadata_df["site_id"])


def test_grid_bounds_conus2_list():
    """Test for using grid_bounds filter with a list"""
    df = point.get_point_data(
        dataset="usgs_nwis",
        variable="streamflow",
        temporal_resolution="daily",
        aggregation="mean",
        date_start="2002-01-01",
        date_end="2002-01-05",
        grid="conus2",
        grid_bounds=[1500, 1300, 1700, 1500],
    )
    assert df.shape[1] >= 15
    assert df.shape[1] <= 100
    assert "07119500" in df.columns
    assert "07208500" in df.columns

    metadata_df = point.get_point_metadata(
        dataset="usgs_nwis",
        variable="streamflow",
        temporal_resolution="daily",
        aggregation="mean",
        date_start="2002-01-01",
        date_end="2002-01-05",
        grid="conus2",
        grid_bounds=[1500, 1300, 1700, 1500],
    )
    assert metadata_df.shape[0] >= 15
    assert metadata_df.shape[0] <= 100
    assert "07119500" in list(metadata_df["site_id"])
    assert "07208500" in list(metadata_df["site_id"])


def test_grid_bounds_conus2_dict():
    """Test for using grid_bounds filter as a dictionary parameter"""
    df = point.get_point_data(
        {
            "dataset": "usgs_nwis",
            "variable": "streamflow",
            "temporal_resolution": "daily",
            "aggregation": "mean",
            "date_start": "2002-01-01",
            "date_end": "2002-01-05",
            "grid": "conus2",
            "grid_bounds": [1500, 1300, 1700, 1500],
        }
    )
    assert df.shape[1] >= 15
    assert df.shape[1] <= 100
    assert "07119500" in df.columns
    assert "07208500" in df.columns

    metadata_df = point.get_point_metadata(
        {
            "dataset": "usgs_nwis",
            "variable": "streamflow",
            "temporal_resolution": "daily",
            "aggregation": "mean",
            "date_start": "2002-01-01",
            "date_end": "2002-01-05",
            "grid": "conus2",
            "grid_bounds": [1500, 1300, 1700, 1500],
        }
    )
    assert metadata_df.shape[0] >= 15
    assert metadata_df.shape[0] <= 100
    assert "07119500" in list(metadata_df["site_id"])
    assert "07208500" in list(metadata_df["site_id"])


def test_fail_no_grid_get_data():
    """
    Test to ensure proper failure if grid_bounds parameter supplied
    but no grid parameter supplied.
    """
    with pytest.raises(ValueError) as exc:
        point.get_point_data(
            dataset="usgs_nwis",
            variable="streamflow",
            temporal_resolution="daily",
            aggregation="mean",
            date_start="2002-01-01",
            date_end="2002-01-05",
            grid_bounds=[1500, 1300, 1700, 1500],
        )
    assert (
        str(exc.value)
        == "When providing the parameter `grid_bounds`, please also provide the parameter `grid` as either 'conus1' or 'conus2'."
    )


def test_fail_no_grid_get_metadata():
    """
    Test to ensure proper failure if grid_bounds parameter supplied
    but no grid parameter supplied.
    """
    with pytest.raises(ValueError) as exc:
        point.get_point_metadata(
            dataset="usgs_nwis",
            variable="streamflow",
            temporal_resolution="daily",
            aggregation="mean",
            date_start="2002-01-01",
            date_end="2002-01-05",
            grid_bounds=[1500, 1300, 1700, 1500],
        )
    assert (
        str(exc.value)
        == "When providing the parameter `grid_bounds`, please also provide the parameter `grid` as either 'conus1' or 'conus2'."
    )


def test_fail_no_sites_get_data():
    """
    Test to ensure proper failure if filtering on the supplied grid
    and grid_bounds results in zero sites.
    """
    with pytest.raises(Exception) as exc:
        point.get_point_data(
            dataset="usgs_nwis",
            variable="streamflow",
            temporal_resolution="daily",
            aggregation="mean",
            date_start="2002-01-01",
            date_end="2002-01-05",
            grid="conus2",
            grid_bounds=[1500, 1300, 1501, 1301],
        )
    assert str(exc.value) == "There are no sites within the provided grid_bounds."


def test_fail_no_sites_get_metadata():
    """
    Test to ensure proper failure if filtering on the supplied grid
    and grid_bounds results in zero sites.
    """
    with pytest.raises(Exception) as exc:
        point.get_point_metadata(
            dataset="usgs_nwis",
            variable="streamflow",
            temporal_resolution="daily",
            aggregation="mean",
            date_start="2002-01-01",
            date_end="2002-01-05",
            grid="conus2",
            grid_bounds=[1500, 1300, 1501, 1301],
        )
    assert str(exc.value) == "There are no sites within the provided grid_bounds."


def test_get_variables_grid_bounds_conus1_list():
    """Test for using grid_bounds filter with a list"""
    df = point.get_site_variables(
        dataset="usgs_nwis",
        variable="streamflow",
        temporal_resolution="daily",
        aggregation="mean",
        date_start="2002-01-01",
        date_end="2002-01-05",
        grid="conus1",
        grid_bounds=[1000, 450, 1200, 650],
    )
    assert df.shape[0] >= 26
    assert df.shape[0] <= 34
    assert "08249000" in list(df["site_id"])
    assert "07093700" in list(df["site_id"])


def test_get_variables_grid_bounds_conus1_dict():
    """Test for using grid_bounds filter as a dictionary parameter"""
    df = point.get_site_variables(
        {
            "dataset": "usgs_nwis",
            "variable": "streamflow",
            "temporal_resolution": "daily",
            "aggregation": "mean",
            "date_start": "2002-01-01",
            "date_end": "2002-01-05",
            "grid": "conus1",
            "grid_bounds": [1000, 450, 1200, 650],
        }
    )
    assert df.shape[0] >= 26
    assert df.shape[0] <= 34
    assert "08249000" in list(df["site_id"])
    assert "07093700" in list(df["site_id"])


def test_get_variables_grid_bounds_conus2_list():
    """Test for using grid_bounds filter with a list"""
    df = point.get_site_variables(
        dataset="usgs_nwis",
        variable="streamflow",
        temporal_resolution="daily",
        aggregation="mean",
        date_start="2002-01-01",
        date_end="2002-01-05",
        grid="conus2",
        grid_bounds=[1500, 1300, 1700, 1500],
    )
    assert df.shape[0] >= 15
    assert df.shape[0] <= 100
    assert "07119500" in list(df["site_id"])
    assert "07208500" in list(df["site_id"])


def test_get_variables_grid_bounds_conus2_dict():
    """Test for using grid_bounds filter as a dictionary parameter"""
    df = point.get_site_variables(
        {
            "dataset": "usgs_nwis",
            "variable": "streamflow",
            "temporal_resolution": "daily",
            "aggregation": "mean",
            "date_start": "2002-01-01",
            "date_end": "2002-01-05",
            "grid": "conus2",
            "grid_bounds": [1500, 1300, 1700, 1500],
        }
    )
    assert df.shape[0] >= 15
    assert df.shape[0] <= 100
    assert "07119500" in list(df["site_id"])
    assert "07208500" in list(df["site_id"])


def test_fail_no_grid_get_site_variables():
    """
    Test to ensure proper failure if grid_bounds parameter supplied
    but no grid parameter supplied.
    """
    with pytest.raises(ValueError) as exc:
        point.get_site_variables(
            dataset="usgs_nwis",
            variable="streamflow",
            temporal_resolution="daily",
            aggregation="mean",
            date_start="2002-01-01",
            date_end="2002-01-05",
            grid_bounds=[1500, 1300, 1700, 1500],
        )
    assert (
        "When providing the parameter `grid_bounds`, please also provide the parameter `grid`"
        in str(exc.value)
    )


def test_fail_no_sites_get_site_variables():
    """
    Test to ensure proper failure if filtering on the supplied grid
    and grid_bounds results in zero sites.
    """
    with pytest.raises(Exception) as exc:
        point.get_site_variables(
            dataset="usgs_nwis",
            variable="streamflow",
            temporal_resolution="daily",
            aggregation="mean",
            date_start="2002-01-01",
            date_end="2002-01-05",
            grid="conus2",
            grid_bounds=[1500, 1300, 1501, 1301],
        )
    assert "There are no sites within the provided grid_bounds." in str(exc.value)


def test_get_data_jasechko():
    """Test getting data for Jasechko dataset."""
    # No date range
    df = point.get_point_data(
        dataset="jasechko_2024",
        variable="water_table_depth",
        temporal_resolution="yearly",
        aggregation="median",
        site_ids=["1000000106"],
    )
    assert len(df) == 30

    # Date range
    df = point.get_point_data(
        dataset="jasechko_2024",
        variable="water_table_depth",
        temporal_resolution="yearly",
        aggregation="median",
        date_start="2000-01-01",
        date_end="2002-01-01",
        site_ids=["1000000106"],
    )
    assert len(df) == 2


def test_get_metadata_jasechko():
    """Test getting metadata for Jasechko dataset."""
    metadata_df = point.get_point_metadata(
        dataset="jasechko_2024",
        variable="water_table_depth",
        temporal_resolution="yearly",
        aggregation="median",
        site_ids=["1000000106"],
    )
    assert len(metadata_df) == 1
    assert "1000000106" in list(metadata_df["site_id"])

    metadata_df = point.get_point_metadata(
        dataset="jasechko_2024",
        variable="water_table_depth",
        temporal_resolution="yearly",
        aggregation="median",
        date_start="2021-01-01",
        date_end="2022-12-31",
        state="CO",
    )
    assert len(metadata_df) == 281
    assert "usgs_site" in metadata_df.columns


def test_get_site_variables_jasechko():
    """Test for get_site_variables with jasechko dataset."""
    df = point.get_site_variables(
        dataset="jasechko_2024",
        variable="water_table_depth",
        date_start="2021-01-01",
        date_end="2022-12-31",
        state="CO",
    )
    assert len(df) == 281


def test_get_data_fan():
    """Test getting data for Fan dataset."""
    df = point.get_point_data(
        dataset="fan_2013",
        variable="water_table_depth",
        temporal_resolution="long_term",
        aggregation="mean",
        grid="conus2",
        grid_bounds=[1500, 1300, 1550, 1500],
    )
    assert len(df) == 187


def test_get_data_fan_date_range():
    """Test getting data for Fan dataset with a date range provided.
    This dataset is one record per site as a long-term average, so
    as long as the date range overlaps with the data availiability range,
    no sites should get excluded."""
    df = point.get_point_data(
        dataset="fan_2013",
        variable="water_table_depth",
        temporal_resolution="long_term",
        aggregation="mean",
        date_start="2008-01-01",
        date_end="2010-12-31",
        grid="conus2",
        grid_bounds=[1500, 1300, 1550, 1500],
    )
    assert len(df) == 187


def test_get_metadata_fan():
    """Test getting metadata for Fan dataset."""
    metadata_df = point.get_point_metadata(
        dataset="fan_2013",
        variable="water_table_depth",
        temporal_resolution="long_term",
        aggregation="mean",
        grid="conus2",
        grid_bounds=[1500, 1300, 1550, 1500],
    )
    assert len(metadata_df) == 187


def test_get_site_variables_fan():
    """Test for get_site_variables with Fan dataset."""
    df = point.get_site_variables(
        dataset="fan_2013",
        variable="water_table_depth",
        date_start="2000-01-01",
        date_end="2022-12-31",
        state="TX",
        latitude_range=(27, 28),
    )
    assert len(df) == 22


def test_fail_fan_outside_timerange():
    """
    Test to ensure proper failure if filtering on dates outside of the
    date range which the Fan dataset is available for (1927-2009).
    """
    with pytest.raises(Exception) as exc:
        point.get_point_data(
            dataset="fan_2013",
            variable="water_table_depth",
            temporal_resolution="long_term",
            aggregation="mean",
            date_start="2010-01-01",
            date_end="2010-12-31",
        )
    assert str(exc.value) == "There are zero sites that satisfy the given parameters."


def test_get_metadata_by_huc():
    """Test for get_point_metadata with HUC filter."""
    df = point.get_point_metadata(
        dataset="usgs_nwis",
        variable="streamflow",
        temporal_resolution="daily",
        aggregation="mean",
        date_start="2002-01-01",
        date_end="2002-01-05",
        huc_id=["02040106"],
        grid="conus2",
    )
    assert len(df) == 15
    # site ID that is in bbox but not in huc
    assert "01470736" not in list(df["site_id"])


def test_get_data_by_huc():
    """Test for get_point_data with HUC filter."""
    df = point.get_point_data(
        dataset="usgs_nwis",
        variable="streamflow",
        temporal_resolution="daily",
        aggregation="mean",
        date_start="2002-01-01",
        date_end="2002-01-05",
        huc_id=["02040106"],
        grid="conus2",
    )
    assert df.shape[1] == 16
    # site ID that is in bbox but not in huc
    assert "01470736" not in list(df.columns)


def test_get_site_variables():
    """Test for get_site_varaibles with HUC filter."""
    df = point.get_site_variables(
        huc_id=["02040106"], grid="conus2", dataset="usgs_nwis"
    )
    assert len(df) >= 2800 & len(df) <= 3000


def test_huc_no_grid_fail():
    """Test data access for a huc fails when no grid is specified."""
    with pytest.raises(Exception) as exc:
        point.get_point_data(
            dataset="usgs_nwis",
            variable="streamflow",
            temporal_resolution="daily",
            aggregation="mean",
            date_start="2002-01-01",
            date_end="2002-01-05",
            huc_id=["02040106"],
        )
    assert (
        str(exc.value)
        == "When providing the parameter `huc_id`, please also provide the parameter `grid` as either 'conus1' or 'conus2'."
    )


def test_huc_list():
    """Test using a list of multiple huc ids."""
    df = point.get_point_metadata(
        dataset="usgs_nwis",
        variable="streamflow",
        temporal_resolution="daily",
        aggregation="mean",
        date_start="2002-01-01",
        date_end="2002-01-05",
        huc_id=["02040106", "02040106"],
        grid="conus2",
    )
    assert df.shape[1] == 25


def test_depth_level_provided_not_sm():
    """Test for if depth_level parameter is provided by the user
    for a variable other than soil moisture."""
    with pytest.raises(Exception) as exc:
        point.get_point_data(
            dataset="usgs_nwis",
            variable="streamflow",
            temporal_resolution="daily",
            aggregation="mean",
            date_start="2002-01-01",
            date_end="2002-01-05",
            latitude_range=(47, 50),
            longitude_range=(-75, -50),
            depth_level=2,
        )
    assert (
        str(exc.value)
        == "Parameter depth_level is only supported when variable=='soil_moisture'."
    )


def test_maintenance_error_point_fail(monkeypatch):
    """
    Test MaintenanceError is raised when get_point_data raises an Error
    during the maintenance window.
    """
    # Artifically set the maintenance window to always be True
    monkeypatch.setattr(
        "hf_hydrodata.data_catalog._is_maintenance_window", lambda: True
    )

    with pytest.raises(MaintenanceError):
        point.get_point_data(
            dataset="dummy",
            variable="dummy",
            temporal_resolution="daily",
            aggregation="dummy",
        )


def test_maintenance_error_point_pass(monkeypatch):
    """
    Test MaintenanceError is not raised when get_point_data does
    not raise an Error, even if it's during the maintenance window.
    """
    # Artifically set the maintenance window to always be True
    monkeypatch.setattr(
        "hf_hydrodata.data_catalog._is_maintenance_window", lambda: True
    )

    df = point.get_point_data(
        dataset="usgs_nwis",
        variable="streamflow",
        temporal_resolution="daily",
        aggregation="mean",
        date_start="2002-01-01",
        date_end="2002-01-05",
        latitude_range=(47, 50),
        longitude_range=(-75, -50),
    )
    assert len(df) == 5


if __name__ == "__main__":
    pytest.main()
