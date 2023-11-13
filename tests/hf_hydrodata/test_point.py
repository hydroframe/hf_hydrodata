"""Unit test for the /point."""

# pylint: disable=W0613,C0301,R0903
import sys
import os
import io
from unittest import mock
import pytest
import pandas as pd
import numpy as np

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../src")))

from hf_hydrodata import point  # noqa

REMOTE_TEST_DATA_DIR = '/hydrodata/national_obs/tools/test_data'
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
        print("The dataframe is:", df)
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

    with mock.patch(
        "requests.get",
        new=mock_requests_get,
    ):
        point.HYDRODATA = "/empty"
        data_df = point.get_data(
            "usgs_nwis",
            "streamflow",
            "daily",
            "average",
            date_start="2020-01-01",
            date_end="2020-01-03",
            latitude_range=(45, 46),
            longitude_range=(-110, -108),
        )

        assert (data_df.loc[0, "0"]) == "01019000"


def test_get_meta_dataframe():
    """Test code that allows api to access metadata remotely, with api
    calls mocked out."""

    with mock.patch(
        "requests.get",
        new=mock_requests_get_metadata,
    ):
        point.HYDRODATA = "/empty"
        data_df = point.get_metadata(
            "usgs_nwis",
            "streamflow",
            "daily",
            "average",
            date_start="2020-01-01",
            date_end="2020-01-03",
            latitude_range=(45, 46),
            longitude_range=(-110, -108),
        )

        assert (data_df.loc[0, "0"]) == "01019001"


def test_check_inputs():
    """Confirm utils.check_inputs fails for expected cases."""
    # Parameter provided for variable not in supported list (typo).
    point.HYDRODATA = "/hydrodata"
    with pytest.raises(Exception):
        point.check_inputs(
            data_source="usgs_nwis",
            variable="steamflow",
            temporal_resolution="daily",
            aggregation="average",
        )

    # Parameter provided for temporal_resolution not in supported list.
    with pytest.raises(Exception):
        point.check_inputs(
            data_source="usgs_nwis",
            variable="streamflow",
            temporal_resolution="monthly",
            aggregation="average",
        )

    # Variable requested is soil moisture but no depth level provided.
    with pytest.raises(Exception):
        point.check_inputs(
            data_source="usda_nrcs",
            variable="soil moisture",
            temporal_resolution="daily",
            aggregation="start-of-day",
        )

    # Variable requested is soil moisture with unsupported depth level provided.
    with pytest.raises(Exception):
        point.check_inputs(
            data_source="usda_nrcs",
            variable="soil moisture",
            temporal_resolution="daily",
            aggregation="start-of-day",
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
    assert list(df1.columns) == ['site1', 'site2', 'site3']
    df2 = point._filter_min_num_obs(df, 2)
    assert list(df2.columns) == ['site1', 'site2', 'site3']
    df3 = point._filter_min_num_obs(df, 3)
    assert list(df3.columns) == ['site1', 'site2']
    df4 = point._filter_min_num_obs(df, 4)
    assert list(df4.columns) == ['site1']


def test_no_sites_error_message():
    """Test that error gets raised if not sites fit filters"""
    with pytest.raises(Exception):
        df = point.get_data(
            "usgs_nwis",
            "wtd",
            "hourly",
            "average",
            date_start="2002-01-01",
            date_end="2002-01-05",
            latitude_range=(49, 50),
            longitude_range=(-75, -74)
        )


def test_get_data_streamflow_daily():
    """Test for daily streamflow data"""
    df = point.get_data(
        "usgs_nwis",
        "streamflow",
        "daily",
        "average",
        date_start="2002-01-01",
        date_end="2002-01-05",
        latitude_range=(47, 50),
        longitude_range=(-75, -50)
    )
    assert len(df) == 5
    assert '01011000' in df.columns


def test_get_data_streamflow_daily_dict():
    """Test for daily streamflow data using input dictionary"""
    df = point.get_data(
        "usgs_nwis",
        "streamflow",
        "daily",
        "average",
        {"date_start": "2002-01-01",
         "date_end": "2002-01-05",
         "latitude_range": (47, 50),
         "longitude_range": (-75, -50)}
    )
    assert len(df) == 5
    assert '01011000' in df.columns


def test_get_data_streamflow_hourly():
    """Test for hourly streamflow data"""
    df = point.get_data(
        "usgs_nwis",
        "streamflow",
        "hourly",
        "average",
        date_start="2002-01-01",
        date_end="2002-01-05",
        latitude_range=(45, 50),
        longitude_range=(-75, -50)
    )
    assert len(df) == 97
    assert '01011000' in df.columns


def test_get_data_wtd_daily():
    """Test for daily wtd data"""
    df = point.get_data(
        "usgs_nwis",
        "wtd",
        "daily",
        "average",
        date_start="2002-01-01",
        date_end="2002-01-05",
        latitude_range=(45, 50),
        longitude_range=(-75, -50)
    )
    assert len(df) == 5
    assert '453629068531801' in df.columns


def test_get_data_wtd_hourly():
    """Test for hourly wtd data"""
    df = point.get_data(
        "usgs_nwis",
        "wtd",
        "hourly",
        "average",
        date_start="2002-01-01",
        date_end="2002-01-05",
        latitude_range=(30, 40),
        longitude_range=(-120, -110)
    )
    assert len(df) == 97
    assert '343409111511101' in df.columns


def test_get_data_wtd_instantaneous():
    """Test for instantaneous wtd data"""
    df = point.get_data(
        "usgs_nwis",
        "wtd",
        "instantaneous",
        "instantaneous",
        date_start="2002-01-01",
        date_end="2002-01-01",
        latitude_range=(30, 35),
        longitude_range=(-120, -115)
    )
    assert len(df) >= 300
    assert '323709080324809' in list(df['site_id'])
    assert set(list(df['date'])) == {'2002-01-01'}


def test_get_data_swe_daily():
    """Test for daily swe data"""
    df = point.get_data(
        "usda_nrcs",
        "swe",
        "daily",
        "start-of-day",
        date_start="2022-01-01",
        date_end="2022-01-04",
        latitude_range=(39, 40),
        longitude_range=(-120, -119)
    )
    assert len(df) == 4
    assert '340:NV:SNTL' in df.columns


def test_get_data_precipitation_daily():
    """Tests for daily precipitation data"""
    accumulated_df = point.get_data(
        "usda_nrcs",
        "precipitation",
        "daily",
        "accumulated",
        date_start="2022-01-01",
        date_end="2022-01-04",
        latitude_range=(39, 40),
        longitude_range=(-120, -119)
    )
    assert len(accumulated_df) == 4
    assert '340:NV:SNTL' in accumulated_df.columns

    total_df = point.get_data(
        "usda_nrcs",
        "precipitation",
        "daily",
        "total",
        date_start="2022-01-01",
        date_end="2022-01-04",
        latitude_range=(39, 40),
        longitude_range=(-120, -119)
    )
    assert len(total_df) == 4
    assert '340:NV:SNTL' in total_df.columns
    assert (accumulated_df['340:NV:SNTL'] > total_df['340:NV:SNTL']).all() == True

    total_adj_df = point.get_data(
        "usda_nrcs",
        "precipitation",
        "daily",
        "total, snow-adjusted",
        date_start="2022-01-01",
        date_end="2022-01-04",
        latitude_range=(39, 40),
        longitude_range=(-120, -119)
    )
    assert len(total_adj_df) == 4
    assert '340:NV:SNTL' in total_adj_df.columns
    assert (total_adj_df['340:NV:SNTL'] >= total_df['340:NV:SNTL']).all() == True


def test_get_data_temperature_daily():
    """Tests for daily temperature data"""
    min_df = point.get_data(
        "usda_nrcs",
        "temperature",
        "daily",
        "minimum",
        date_start="2022-01-01",
        date_end="2022-01-04",
        latitude_range=(39, 40),
        longitude_range=(-120, -119)
    )
    assert len(min_df) == 4
    assert '340:NV:SNTL' in min_df.columns

    max_df = point.get_data(
        "usda_nrcs",
        "temperature",
        "daily",
        "maximum",
        date_start="2022-01-01",
        date_end="2022-01-04",
        latitude_range=(39, 40),
        longitude_range=(-120, -119)
    )
    assert len(max_df) == 4
    assert '340:NV:SNTL' in max_df.columns

    mean_df = point.get_data(
        "usda_nrcs",
        "temperature",
        "daily",
        "average",
        date_start="2022-01-01",
        date_end="2022-01-04",
        latitude_range=(39, 40),
        longitude_range=(-120, -119)
    )
    assert len(mean_df) == 4
    assert '340:NV:SNTL' in mean_df.columns

    assert (min_df['340:NV:SNTL'] <= max_df['340:NV:SNTL']).all() == True
    assert (min_df['340:NV:SNTL'] <= mean_df['340:NV:SNTL']).all() == True
    assert (mean_df['340:NV:SNTL'] <= max_df['340:NV:SNTL']).all() == True


def test_get_soil_moisture_fail():
    """Make sure failure if required depth_level parameter not supplied."""
    with pytest.raises(Exception):
        df = point.get_data(
            "usda_nrcs",
            "soil moisture",
            "daily",
            "start-of-day",
            date_start="2002-01-01",
            date_end="2002-01-05",
            latitude_range=(49, 50),
            longitude_range=(-75, -74)
        )


def test_get_data_soil_moisture_daily():
    """Tests for daily soil moisture data"""
    df_2 = point.get_data(
        "usda_nrcs",
        "soil moisture",
        "daily",
        "start-of-day",
        depth_level=2,
        date_start="2022-01-01",
        date_end="2022-01-03",
        latitude_range=(39, 40),
        longitude_range=(-120, -119)
    )
    assert len(df_2) == 3
    assert '1242:NV:SNTL' in df_2.columns

    df_4 = point.get_data(
        "usda_nrcs",
        "soil moisture",
        "daily",
        "start-of-day",
        depth_level=4,
        date_start="2022-01-01",
        date_end="2022-01-03",
        latitude_range=(30, 40),
        longitude_range=(-120, -119)
    )
    assert len(df_4) == 3
    assert '2189:CA:SCAN' in df_4.columns

    df_8 = point.get_data(
        "usda_nrcs",
        "soil moisture",
        "daily",
        "start-of-day",
        depth_level=8,
        date_start="2022-01-01",
        date_end="2022-01-03",
        latitude_range=(39, 40),
        longitude_range=(-120, -119)
    )
    assert len(df_8) == 3
    assert '1242:NV:SNTL' in df_8.columns

    df_20 = point.get_data(
        "usda_nrcs",
        "soil moisture",
        "daily",
        "start-of-day",
        depth_level=20,
        date_start="2022-01-01",
        date_end="2022-01-03",
        latitude_range=(39, 40),
        longitude_range=(-120, -119)
    )
    assert len(df_20) == 3
    assert '1242:NV:SNTL' in df_20.columns

    df_40 = point.get_data(
        "usda_nrcs",
        "soil moisture",
        "daily",
        "start-of-day",
        depth_level=40,
        date_start="2022-01-01",
        date_end="2022-01-03",
        latitude_range=(30, 40),
        longitude_range=(-120, -119)
    )
    assert len(df_40) == 3
    assert '2189:CA:SCAN' in df_40.columns


def test_get_data_latent_heat_flux():
    """Tests for hourly latent heat flux data"""
    df = point.get_data(
        "ameriflux",
        "latent heat flux",
        "hourly",
        "total",
        date_start="2022-01-01",
        date_end="2022-01-02",
        latitude_range=(30, 40),
        longitude_range=(-120, -119)
    )
    assert len(df) == 25
    assert 'US-xSJ' in df.columns


def test_get_data_sensible_heat_flux():
    """Tests for hourly sensible heat flux data"""
    df = point.get_data(
        "ameriflux",
        "sensible heat flux",
        "hourly",
        "total",
        date_start="2022-01-01",
        date_end="2022-01-02",
        latitude_range=(30, 40),
        longitude_range=(-120, -119)
    )
    assert len(df) == 25
    assert 'US-xSJ' in df.columns


def test_get_data_sradiation():
    """Tests for hourly shortwave radiation data"""
    df = point.get_data(
        "ameriflux",
        "shortwave radiation",
        "hourly",
        "average",
        date_start="2022-01-01",
        date_end="2022-01-02",
        latitude_range=(30, 40),
        longitude_range=(-120, -119)
    )
    assert len(df) == 25
    assert 'US-xSJ' in df.columns


def test_get_data_lradiation():
    """Tests for hourly longwave radiation data"""
    df = point.get_data(
        "ameriflux",
        "longwave radiation",
        "hourly",
        "average",
        date_start="2022-01-01",
        date_end="2022-01-02",
        latitude_range=(30, 40),
        longitude_range=(-120, -119)
    )
    assert len(df) == 25
    assert 'US-xSJ' in df.columns


def test_get_data_vpd():
    """Tests for hourly vapor pressure deficit data"""
    df = point.get_data(
        "ameriflux",
        "vapor pressure deficit",
        "hourly",
        "average",
        date_start="2022-01-01",
        date_end="2022-01-02",
        latitude_range=(30, 40),
        longitude_range=(-120, -119)
    )
    assert len(df) == 25
    assert 'US-xSJ' in df.columns


def test_get_data_temperature_fluxnet():
    """Tests for hourly temperature data"""
    df = point.get_data(
        "ameriflux",
        "temperature",
        "hourly",
        "average",
        date_start="2022-01-01",
        date_end="2022-01-02",
        latitude_range=(30, 40),
        longitude_range=(-120, -119)
    )
    assert len(df) == 25
    assert 'US-xSJ' in df.columns


def test_get_data_wind_speed():
    """Tests for hourly wind speed data"""
    df = point.get_data(
        "ameriflux",
        "wind speed",
        "hourly",
        "average",
        date_start="2022-01-01",
        date_end="2022-01-02",
        latitude_range=(30, 40),
        longitude_range=(-120, -119)
    )
    assert len(df) == 25
    assert 'US-xSJ' in df.columns


def test_get_metadata_streamflow():
    """Test for streamflow metadata"""
    metadata_df = point.get_metadata(
        "usgs_nwis",
        "streamflow",
        "daily",
        "average",
        date_start="2002-01-01",
        date_end="2002-01-05",
        latitude_range=(47, 50),
        longitude_range=(-75, -50)
    )
    assert len(metadata_df) == 4
    assert len(metadata_df.columns) == 23
    assert '01011000' in list(metadata_df['site_id'])


def test_get_metadata_streamflow_dict():
    """Test for streamflow metadata using dictionary input"""
    metadata_df = point.get_metadata(
        "usgs_nwis",
        "streamflow",
        "daily",
        "average",
        {"date_start": "2002-01-01",
         "date_end": "2002-01-05",
         "latitude_range": (47, 50),
         "longitude_range": (-75, -50)}
    )
    assert len(metadata_df) == 4
    assert len(metadata_df.columns) == 23
    assert '01011000' in list(metadata_df['site_id'])


def test_get_metadata_streamflow_hourly():
    """Test for streamflow metadata, hourly"""
    metadata_df = point.get_metadata(
        "usgs_nwis",
        "streamflow",
        "hourly",
        "average",
        date_start="2002-01-01",
        date_end="2002-01-05",
        latitude_range=(47, 50),
        longitude_range=(-75, -50)
    )
    assert len(metadata_df) == 4
    assert len(metadata_df.columns) == 23
    assert '01011000' in list(metadata_df['site_id'])


def test_get_metadata_wtd():
    """Test for wtd metadata"""
    metadata_df = point.get_metadata(
        "usgs_nwis",
        "wtd",
        "daily",
        "average",
        date_start="2002-01-01",
        date_end="2002-01-05",
        latitude_range=(47, 50),
        longitude_range=(-75, -50)
    )
    assert len(metadata_df) == 1
    assert len(metadata_df.columns) == 25
    assert '471457068353001' in list(metadata_df['site_id'])


def test_get_metadata_swe():
    """Test for swe metadata"""
    metadata_df = point.get_metadata(
        "usda_nrcs",
        "swe",
        "daily",
        "start-of-day",
        date_start="2002-01-01",
        date_end="2002-01-05",
        latitude_range=(39, 40),
        longitude_range=(-120, -119)
    )
    assert len(metadata_df) == 3
    assert len(metadata_df.columns) == 20
    assert '340:NV:SNTL' in list(metadata_df['site_id'])


def test_get_metadata_flux():
    """Test for ameriflux metadata"""
    metadata_df = point.get_metadata(
        "ameriflux",
        "latent heat flux",
        "hourly",
        "total",
        date_start="2022-01-01",
        date_end="2022-01-02",
        latitude_range=(30, 40),
        longitude_range=(-120, -119)
    )
    assert len(metadata_df) == 3
    assert len(metadata_df.columns) == 37
    assert 'US-xSJ' in list(metadata_df['site_id'])


def test_get_data_state_filter():
    """Test for using state filter"""
    df = point.get_data(
        "usgs_nwis",
        "streamflow",
        "daily",
        "average",
        date_start="2002-01-01",
        date_end="2002-01-05",
        state='ME'
    )
    assert len(df) == 5
    assert '01011000' in df.columns
    assert len(df.columns) >= 64


def test_get_data_site_filter():
    """Test for using site_ids filter"""
    df = point.get_data(
        "usgs_nwis",
        "streamflow",
        "daily",
        "average",
        date_start="2002-01-01",
        date_end="2002-01-05",
        site_ids=['01011000', '01013500']
    )
    assert len(df) == 5
    assert list(df.columns == ['date', '01011000', '01013500'])


def test_site_networks_filter():
    """Test for using site_networks filter"""
    data_df = point.get_data(
        "usgs_nwis",
        "streamflow",
        "daily",
        "average",
        date_start="2002-01-01",
        date_end="2002-01-05",
        state='NJ',
        latitude_range=(40, 41),
        longitude_range=(-75, -74),
        site_networks=['gagesii']
    )
    assert len(data_df) == 5
    assert '01377500' in data_df.columns

    metadata_df = point.get_metadata(
        "usgs_nwis",
        "streamflow",
        "daily",
        "average",
        date_start="2002-01-01",
        date_end="2002-01-05",
        state='NJ',
        latitude_range=(40, 41),
        longitude_range=(-75, -74),
        site_networks=['gagesii']
    )
    assert len(metadata_df) == 60


def test_get_data_min_num_obs_filter():
    """Test for using min_num_obs filter"""
    df = point.get_data(
        "usgs_nwis",
        "streamflow",
        "daily",
        "average",
        date_start="2002-01-01",
        date_end="2002-01-05",
        site_ids=['01377500', '01378500', '01445000'],
        min_num_obs=5
    )
    assert list(df.columns) == ['date', '01377500', '01378500']

    df = point.get_data(
        "usgs_nwis",
        "streamflow",
        "daily",
        "average",
        date_start="2002-01-01",
        date_end="2002-01-05",
        site_ids=['01377500', '01378500', '01445000'],
        min_num_obs=1
    )
    assert list(df.columns) == ['date', '01377500', '01378500']

    # If no min_num_obs filter supplied, all three sites returned
    df = point.get_data(
        "usgs_nwis",
        "streamflow",
        "daily",
        "average",
        date_start="2002-01-01",
        date_end="2002-01-05",
        site_ids=['01377500', '01378500', '01445000']
    )
    assert list(df.columns) == ['date', '01377500', '01378500', '01445000']


def test_polygon_filter_data_remote():
    """Test data filter for accepting a shapefile when the file is local to an end user and remote 
    to /hydrodata and the API."""
    df = point.get_data(
        "usgs_nwis",
        "streamflow",
        "daily",
        "average",
        date_start="2002-01-01",
        date_end="2002-01-05",
        polygon=f'{str(LOCAL_TEST_DATA_DIR)}/raritan_watershed.shp',
        polygon_crs="""GEOGCS["GCS_North_American_1983",
                        DATUM["D_North_American_1983",
                        SPHEROID["GRS_1980",6378137.0,298.257222101]],
                        PRIMEM["Greenwich",0.0],
                        UNIT["Degree",0.0174532925199433]]"""
    )
    assert len(df) == 5
    assert len(df.columns) >= 25
    assert '01401000' in df.columns


def test_polygon_filter_metadata_remote():
    """Test metadata filter for accepting a shapefile when the file is local to an end user and remote 
    to /hydrodata and the API."""
    metadata_df = point.get_metadata(
        "usgs_nwis",
        "streamflow",
        "daily",
        "average",
        date_start="2002-01-01",
        date_end="2002-01-05",
        polygon=f'{str(LOCAL_TEST_DATA_DIR)}/raritan_watershed.shp',
        polygon_crs="""GEOGCS["GCS_North_American_1983",
                        DATUM["D_North_American_1983",
                        SPHEROID["GRS_1980",6378137.0,298.257222101]],
                        PRIMEM["Greenwich",0.0],
                        UNIT["Degree",0.0174532925199433]]"""
    )
    assert len(metadata_df) >= 24
    assert '01401000' in list(metadata_df['site_id'])


def test_polygon_filter_fail():
    """Ensure polygon processing fails if no polygon_crs provided"""
    with pytest.raises(Exception):
        point.get_data(
            "usgs_nwis",
            "streamflow",
            "daily",
            "average",
            date_start="2002-01-01",
            date_end="2002-01-05",
            polygon=f'{str(LOCAL_TEST_DATA_DIR)}/raritan_watershed.shp')


def test_get_citations_usgs():
    """Test for get_citations function with return DataFrame."""
    c_dict = point.get_citations(data_source='usgs_nwis', variable='streamflow',
                                 temporal_resolution='daily', aggregation='average')

    assert len(c_dict.keys()) == 1
    assert 'usgs_nwis' in c_dict


def test_get_citations_ameriflux():
    """Test for get_citations function with return DataFrame."""
    c_dict = point.get_citations(data_source='ameriflux', variable='latent heat flux',
                                 temporal_resolution='hourly', aggregation='total',
                                 site_ids=['US-Act', 'US-Bar'])

    assert len(c_dict.keys()) == 3
    assert 'ameriflux' in c_dict
    assert 'US-Act' in c_dict
    assert 'US-Bar' in c_dict


if __name__ == "__main__":
    pytest.main()
