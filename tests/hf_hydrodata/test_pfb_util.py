"""
Unit tests for pfb_util.py that contains utilities for generating
pbf files from numpy arrays for use by the /gridded-data-download route.
"""

# pylint: disable=E0401,C0413

import sys
import os
import tempfile
import pytest
import numpy as np
import parflow as pf
import hf_hydrodata as hf

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../src")))
import hf_hydrodata.pfb_util


def test_pfb_cw3e_huc():
    """Test generating a pfb file from a gridded data numpy array."""

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
        file_name = f"{temp_dir}/filename.pfb"
        data = hf.gridded.get_gridded_data(filter_options)
        hf_hydrodata.pfb_util.generate_pfb_file(data, file_name)
        new_data = pf.read_pfb(file_name)
        # Check shape and values are the same before and after compression
        assert new_data.shape == (2, 76, 52)
        assert new_data[0, 20, 20] == pytest.approx(269.59402)


def test_2d():
    """Test generating a pfb file from a 2d gridded data numpy array."""

    with tempfile.TemporaryDirectory() as temp_dir:
        file_name = f"{temp_dir}/filename.pfb"
        data = np.zeros((20, 30))
        hf_hydrodata.pfb_util.generate_pfb_file(data, file_name)
        new_data = pf.read_pfb(file_name)
        # Check shape and values are the same before and after compression
        assert new_data.shape == (1, 20, 30)


def test_4d():
    """Test generating a pfb file from a 4d gridded data numpy array."""

    with tempfile.TemporaryDirectory() as temp_dir:
        file_name = f"{temp_dir}/filename.pfb"
        data = np.zeros((2, 3, 20, 30))
        hf_hydrodata.pfb_util.generate_pfb_file(data, file_name)
        new_data = pf.read_pfb(file_name)
        # Check shape and values are the same before and after compression
        assert new_data.shape == (6, 20, 30)
