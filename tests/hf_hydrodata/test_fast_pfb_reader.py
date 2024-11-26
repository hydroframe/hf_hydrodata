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
    assert fast_data.shape == (365, 24, 1, 1)

    # Read using parflow read_pfb_sequence
    pfb_seq_data = parflow.read_pfb_sequence(pfb_files, pfb_constraints)
    pfb_seq_total = pfb_seq_data.sum()
    assert pfb_seq_data.shape == (365, 24, 1, 1)
    assert fast_total == pfb_seq_total

def test_compare_performance():
    """
    Test compare performance of fast_pfb_reader and parflow read_pfb_sequence.
    Note this is about the same when HDFS cache is hot, but fast_pfb_reader is 10x faster when HDFS is cold.
    """

    pfb_constraints = {
        "x": {"start": 4057, "stop": 4058},
        "y": {"start": 1914, "stop": 1915},
        "z": {"start": 0, "stop": 0},
    }

    # Read using fast_pfb_reader
    template_fast = "/hydrodata/forcing/processed_data/CONUS2/CW3E_v1.0/hourly/WY1998/CW3E.DLWR.*.pfb" 
    pfb_files = glob.glob(template_fast)
    pfb_files.sort()
    fast_t0 = time.time()
    fast_data = hf_hydrodata.fast_pfb_reader.read_files(pfb_files, pfb_constraints)
    fast_t1 = time.time()
    assert fast_data.shape == (365, 24, 1, 1)

    # Read using parflow read_pfb_sequence
    template_seq = "/hydrodata/forcing/processed_data/CONUS2/CW3E_v1.0/hourly/WY2001/CW3E.DSWR.*.pfb" 
    pfb_files = glob.glob(template_fast)
    pfb_files.sort()
    seq_t0 = time.time()
    pfb_seq_data = parflow.read_pfb_sequence(pfb_files, pfb_constraints)
    seq_t1 = time.time()
    seq_duration = seq_t1 - seq_t0
    fast_duration = fast_t1 - fast_t0
    assert pfb_seq_data.shape == (365, 24, 1, 1)
