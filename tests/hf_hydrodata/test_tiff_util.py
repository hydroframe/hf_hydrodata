"""
Unit tests for tiff_util.py that contains utilities for generating
tiff files from numpy arrays.
"""

# pylint: disable=E0401,C0413

import sys
import os
import tempfile
import xarray as xr
import hf_hydrodata as hf

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../src")))
import hf_hydrodata.tiff_util


def test_tiff_cw3e_huc():
    """Test generating a Tiff file from gridded data numpy array."""

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
        file_name = f"{temp_dir}/filename.tif"
        data = hf.get_gridded_data(filter_options)
        entry = hf.get_catalog_entry(filter_options)
        assert not os.path.exists(file_name)
        hf_hydrodata.tiff_util.generate_tiff_file(
            data, entry, filter_options, file_name
        )
        assert os.path.exists(file_name)
        tiff_dataset = xr.open_dataset(file_name)
        tiff_variable = tiff_dataset["band_data"]
        data = tiff_variable.values
        assert data is not None
        assert data.shape == (2, 76, 52)
