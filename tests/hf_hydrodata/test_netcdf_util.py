"""
Unit tests for netcdf_util.py that contains utilities for generating
netcdf files from numpy arrays for use by the /gridded-data-download route.
"""

# pylint: disable=E0401,C0413

import sys
import os
import tempfile
import pytest
import xarray as xr
import hf_hydrodata as hf

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../src")))
import hf_hydrodata.netcdf_util


def test_cw3e_huc():
    """Test generating a NetCDF file from gridded data numpy array."""

    filter_options = {
        "variable": "air_temp",
        "aggregation": "mean",
        "temporal_resolution": "daily",
        "start_time": "2006-01-01",
        "end_time": "2006-01-03",
        "file_type": "pfb",
        "huc_id": "14010002",
        "dataset": "CW3E",
    }
    with tempfile.TemporaryDirectory() as temp_dir:
        file_name = f"{temp_dir}/filename.nc"
        data = hf.gridded.get_gridded_data(filter_options)
        entry = hf.data_catalog.get_catalog_entry(filter_options)
        hf_hydrodata.netcdf_util.generate_netcdf_file(
            data, entry, filter_options, file_name
        )
        ds = xr.open_dataset(file_name)
        assert os.path.exists(file_name)
        assert len(ds["x"].values) == 52
        assert len(ds["y"].values) == 76
        assert len(ds["time"].values) == 2
        assert len(ds.coords["x"]) == 52
        assert ds.coords["x"][0] == pytest.approx(-779500.3088117298)
        da = ds["air_temp"]
        assert da.shape == (2, 76, 52)
        assert da.values[0, 20, 20] == pytest.approx(269.59402)
        assert ds.attrs["variable"] == "air_temp"
        assert ds.attrs["temporal_resolution"] == "daily"


def test_hourly():
    """
    Test generating a NetCDF file from gridded data numpy
    with hourly data starting at hour 01:00:00 to test parsing dates with hours
    """

    filter_options = {
        "variable": "air_temp",
        "aggregation": "-",
        "temporal_resolution": "hourly",
        "start_time": "2006-01-01 00:00:00",
        "end_time": "2006-01-01 01:00:00",
        "file_type": "pfb",
        "huc_id": "14010002",
        "dataset": "CW3E",
    }
    with tempfile.TemporaryDirectory() as temp_dir:
        file_name = f"{temp_dir}/filename.nc"
        data = hf.gridded.get_gridded_data(filter_options)
        entry = hf.data_catalog.get_catalog_entry(filter_options)
        hf_hydrodata.netcdf_util.generate_netcdf_file(
            data, entry, filter_options, file_name
        )
        ds = xr.open_dataset(file_name)
        assert os.path.exists(file_name)
        assert len(ds["x"].values) == 52
        assert len(ds["y"].values) == 76
        assert len(ds["time"].values) == 1
        assert len(ds.coords["x"]) == 52
        assert ds.coords["x"][0] == pytest.approx(-779500.3088117298)
        da = ds["air_temp"]
        assert da.shape == (1, 76, 52)
        assert da.values[0, 20, 20] == pytest.approx(273.6231)
        assert ds.attrs["variable"] == "air_temp"
        assert ds.attrs["temporal_resolution"] == "hourly"
