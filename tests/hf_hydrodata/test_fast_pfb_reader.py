"""
Unit test for the fast_pfb_reader module.
"""
# pylint: disable=E0401,C0413
import sys
import os
import json
import glob
import time
import parflow
import numpy as np

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../src")))
import hf_hydrodata.fast_pfb_reader

def test_reading_multiple_files():
    """Test reading multiple files of one point."""

    path_template = "/hydrodata/forcing/processed_data/CONUS2/CW3E_v1.0/hourly/WY1998/CW3E.Temp.*.pfb" 
    pfb_constraints = {
        "x": {"start": 4057, "stop": 4058},
        "y": {"start": 1914, "stop": 1915},
        "z": {"start": 0, "stop": 0},
    }
    pfb_files = glob.glob(path_template)
    pfb_files.sort()

    # Read using fast_pfb_reader
    fast_data = hf_hydrodata.fast_pfb_reader.read_files(pfb_files, pfb_constraints)
    fast_total = fast_data.sum()

    # Read using parflow read_pfb_sequence
    pfb_seq_data = parflow.read_pfb_sequence(pfb_files, pfb_constraints)
    pfb_seq_total = pfb_seq_data.sum()

    # Check that both give the same answer
    assert fast_data.shape == (365, 24, 1, 1)
    assert pfb_seq_data.shape == (365, 24, 1, 1)
    assert fast_total == pfb_seq_total
