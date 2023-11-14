"""
Unit test for the grid.py module
"""

# pylint: disable=C0301,C0103,W0632,W0702,W0101,C0302,W0105,E0401,C0413,R0903,W0613,R0912
import sys
import os
import pytest
from unittest.mock import patch

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../src")))

import hf_hydrodata.grid

@pytest.fixture(autouse=True)
def patch_api(mocker):
    mocker.patch("hf_hydrodata.data_model_access._load_model_from_api", return_value=None)

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

    (x, y) = hf_hydrodata.grid.to_ij("conus1", 49.1423, -76.3369)
    assert x == 3324
    assert y == 1888

    (lat1, lon1, lat2, lon2) = hf_hydrodata.grid.to_latlon("conus1", 375, 239, 487, 329)
    (x1, y1, x2, y2) = hf_hydrodata.grid.to_ij("conus1", lat1, lon1, lat2, lon2)
    assert x1 == 375
    assert y1 == 239
    assert x2 == 487
    assert y2 == 329

def test_meters_to_ij():
    """Unit test the meters_to_ij() and to_meters() functions."""

    meters = hf_hydrodata.grid.to_meters("conus1", 31.759219, -115.902573)
    (x, y) = hf_hydrodata.grid.meters_to_ij("conus1", *meters)
    assert round(x) == 10
    assert round(y) == 10

    (x, y) = hf_hydrodata.grid.meters_to_ij("conus1", meters[0], meters[1])
    assert x == 10
    assert y == 10

    (x, y) = hf_hydrodata.grid.meters_to_xy("conus1", meters[0], meters[1])
    assert round(x) == 10
    assert round(y) == 10

def test_latlng_to_grid_out_of_bounds():
    """Unit tests for when latlng is out of bounds of conus1."""

    with pytest.raises(ValueError):
        (_, y) = hf_hydrodata.grid.from_latlon("conus1", 90, -180)

    (lat, lon) = hf_hydrodata.grid.to_latlon("conus1", 0, 0)
    (_, _) = hf_hydrodata.grid.to_ij("conus1", lat, lon)
    with pytest.raises(ValueError):
        (_, _) = hf_hydrodata.grid.to_ij("conus1", lat, lon+0.025)

if __name__ == "__main__":
    pytest.main([__file__])
