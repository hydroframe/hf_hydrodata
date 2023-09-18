"""
Unit test for the grid.py module
"""

# pylint: disable=C0301,C0103,W0632,W0702,W0101,C0302,W0105,E0401,C0413,R0903,W0613,R0912
import sys
import os
import pytest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../src")))

import hf_hydrodata.grid


def test_grid_to_latlng():
    """Test grid_to_latlng."""

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

    (x, y) = hf_hydrodata.grid.from_latlon("conus1", 31.759219, -115.902573)
    assert x == 10
    assert y == 10
    grid_bounds = hf_hydrodata.grid.from_latlon(
        "conus1", 31.65, -115.98, 31.759219, -115.902573
    )
    assert grid_bounds[0] == 0
    assert grid_bounds[1] == 0
    grid_bounds = hf_hydrodata.grid.from_latlon(
        "conus2", 31.65, -115.98, 31.759219, -115.902573
    )
    assert grid_bounds[0] == 441
    assert grid_bounds[1] == 970

    (x, y) = hf_hydrodata.grid.from_latlon("conus1", 49.1423, -76.3369)
    assert x == 3324
    assert y == 1888


def test_get_huc_from_point():
    """Unit test for get_huc_from_latlon and get_huc_from_xy"""

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

    with pytest.raises(ValueError):
        hf_hydrodata.grid.get_huc_bbox("bad grid", ["1019000404"])
    with pytest.raises(ValueError):
        hf_hydrodata.grid.get_huc_bbox("conus1", ["1019000404", "123"])

    bbox = hf_hydrodata.grid.get_huc_bbox("conus1", ["1019000404"])
    assert bbox == (1076, 720, 1124, 739)

    bbox = hf_hydrodata.grid.get_huc_bbox("conus1", ["1102001002", "1102001003"])
    assert bbox == (1088, 415, 1132, 453)


def test_get_huc_bbox_conus2():
    """Unit test for get_huc_bbox for conus2"""

    bbox = hf_hydrodata.grid.get_huc_bbox("conus2", ["1019000404"])
    assert bbox == (1468, 1664, 1550, 1693)


def test_latlng_to_grid_out_of_bounds():
    """Unit tests for when latlng is out of bounds of conus1."""

    (_, y) = hf_hydrodata.grid.from_latlon("conus1", 90, -180)
    assert y > 1888


if __name__ == "__main__":
    pytest.main([__file__])
