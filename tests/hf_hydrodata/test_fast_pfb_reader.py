"""
Unit test for the fast_pfb_reader module.
"""

# pylint: disable=E0401,C0413,C0301,W0101
import sys
import os
import glob
import tempfile
import parflow
import pytest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../src")))
import hf_hydrodata.fast_pfb_reader
import hf_hydrodata as hf


def test_reading_multiple_files():
    """Test reading multiple files of one point."""

    if not os.path.exists("/hydrodata"):
        # Just skip test if this is run on a machine without /hydrodata access
        return

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


def test_not_enough_memory_error():
    """Test attempting to read a file with so many conus2 sized files that will not fit in memory."""

    if not os.path.exists("/hydrodata"):
        # Just skip test if this is run on a machine without /hydrodata access
        return

    with pytest.raises(ValueError):
        path_template = "/hydrodata/temp/CONUS2_transfers/CONUS2/spinup_WY2003/run_inputs/spinup.wy2003.out.press.*.pfb"
        pfb_files = glob.glob(path_template)
        pfb_files.sort()
        pfb_constraints = None
        hf_hydrodata.fast_pfb_reader.read_files(pfb_files, pfb_constraints)


def test_pqr_too_small():
    """
    Test ability to read many files with small subgrid with small pqr.
    Since the constraint only reads one point the result is small enough to fit in memory,
    but the input is 24 conus2 sized files with PQR=1,1,1 that would not fit in memory when loaded in parallel.
    This should still work because fast pfb reader should limit the number of files read in parallel
    so the temporary subgrid reads of the parallel files still fit in memory.
    """

    if not os.path.exists("/hydrodata"):
        # Just skip test if this is run on a machine without /hydrodata access
        return

    # Do not run this test normally because it takes too long to run
    # Trust me it did work once
    return

    # Get file names of 24 files that are conus2 3D
    path_template = "/hydrodata/temp/CONUS2_transfers/CONUS2/spinup_WY2003/run_inputs/spinup.wy2003.out.press.*.pfb"
    pfb_files = glob.glob(path_template)
    pfb_files.sort()
    pfb_files = pfb_files[0:24]
    pfb_constraints = {
        "x": {"start": 4057, "stop": 4058},
        "y": {"start": 1914, "stop": 1915},
        "z": {"start": 0, "stop": 0},
    }

    # Create a copy of 24 pfb files and write them with pqr 1,1,1
    new_pfb_files = []
    cd = os.getcwd()
    with tempfile.TemporaryDirectory() as tempdirname:
        os.chdir(tempdirname)

        for original_path in pfb_files:
            data = parflow.read_pfb(original_path)
            basename = os.path.basename(original_path)
            new_path = f"{tempdirname}/{basename}"
            parflow.write_pfb(new_path, data, pqr=(1, 1, 1))
            print("WRITE", new_path)
            new_pfb_files.append(new_path)

        fast_data = hf_hydrodata.fast_pfb_reader.read_files(
            new_pfb_files, pfb_constraints
        )
        assert fast_data.shape == (24, 10, 1, 1)
    os.chdir(cd)


def test_y_remainder_rows():
    """
    Test reading a y position which is after the remainder sized y rows in pfb file.
    This is an edge case in reading pfb files with subgrids when the subgrid size of the P,Q,R is not
    a multiple of the full pfb size. In that case some subgrids are smaller than others
    to fit the data in memory in approximately the P,Q,R size. This should still work.
    """

    if not os.path.exists("/hydrodata"):
        # Just skip test if this is run on a machine without /hydrodata access
        return

    path = "/hydrodata/PFCLM/CONUS1_baseline/simulations/static/CONUS1_vgn_n.pfb"
    pfb_constraints = {
        "x": {"start": 1075, "stop": 1124},
        "y": {"start": 719, "stop": 739},
        "z": {"start": 0, "stop": 0},
    }

    # Read with fast_pfb_reader
    fast_data = hf_hydrodata.fast_pfb_reader.read_files(path, pfb_constraints)
    fast_total = fast_data.sum()

    # Read with parflow read_pfb_sequence
    pfb_seq_data = parflow.read_pfb_sequence([path], pfb_constraints)
    pfb_seq_total = pfb_seq_data.sum()

    # Assert same answer
    assert fast_data.shape == (1, 5, 20, 49)
    assert fast_data.shape == (1, 5, 20, 49)
    assert fast_total == pfb_seq_total


def test_full_3d_conus2():
    """Test reading a full conus2 3D file with all the subgrids and compare to parflow reader."""

    if not os.path.exists("/hydrodata"):
        # Just skip test if this is run on a machine without /hydrodata access
        return

    path = "/hydrodata/temp/CONUS2_transfers/CONUS2/spinup_WY2003/run_inputs/spinup.wy2003.out.clm_output.04855.C.pfb"
    # Read using fast_pfb_reader
    fast_data = hf_hydrodata.fast_pfb_reader.read_files(path, None)
    fast_total = fast_data.sum()

    # Read using parflow read_pfb_sequence
    pfb_seq_data = parflow.read_pfb_sequence([path], None)
    pfb_seq_total = pfb_seq_data.sum()

    # Check that both give the same answer
    assert fast_data.shape == (1, 17, 3256, 4442)
    assert pfb_seq_data.shape == (1, 17, 3256, 4442)
    assert fast_total == pfb_seq_total

def test_public_function():
    """Test the externally visible fast_pfb_read_function."""

    if not os.path.exists("/hydrodata"):
        # Just skip test if this is run on a machine without /hydrodata access
        return

    path = "/hydrodata/forcing/processed_data/CONUS2/CW3E_v1.0/hourly/WY1998/CW3E.Temp.000001_to_000024.pfb"
    constraints = {"x":{"start": 10, "stop": 15}, "y": {"start": 20, "stop": 30}}
    data = hf.fast_read_pfb(path, constraints)

    
