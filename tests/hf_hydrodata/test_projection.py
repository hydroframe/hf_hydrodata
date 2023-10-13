"""Unit tests of the projection module."""
# pylint: disable=C0103,W0703,E0401,E0633,R0902,C0301,C0413,R0914

import sys
import os
import pyproj
import pytest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../src")))
from hf_hydrodata.projection import to_conic, from_conic

@pytest.fixture(autouse=True)
def patch_api(mocker):
    mocker.patch("hf_hydrodata.data_model_access._load_model_from_api", return_value=None)

def test_to_conic_conus2():
    """Unit test of to_conic using conus2"""

    lat = 31.759219
    lng = -115.902573
    cx, cy = to_conic(lat, lng, "conus2")
    assert round(cx) == 450448
    assert round(cy) == 979566
    cx_lat, cy_lng = from_conic(cx, cy, "conus2")
    assert abs(cx_lat - lat) < 0.0000001
    assert abs(cy_lng - lng) < 0.0000001


def test_to_conic():
    "Unit test of the to_conic function"

    x_origin = -1885055.4995
    y_origin = -604957.0654
    lat = 31.759219
    lng = -115.902573
    cx, cy = to_conic(lat, lng)

    transformer = pyproj.Transformer.from_crs("EPSG:4326", "ESRI:102004")
    px, py = transformer.transform(lat, lng)
    px = px - x_origin
    py = py - y_origin
    diff_x = abs(cx - px)
    diff_y = abs(cy - py)
    assert diff_x <= 0.001
    assert diff_y <= 0.001


def test_to_conic_all():
    """Unit tests to check a full range of points on conus 1 grid."""
    points = []
    for x in range(0, int(3342 / 20)):
        for y in range(0, int(1888 / 20)):
            cx = x * 1000
            cy = y * 1000
            (lat, lng) = from_conic(cx, cy)
            points.append([x, y, cx, cy, lat, lng])
    for p in points:
        [x, y, cx, cy, lat, lng] = p
        (cx2, cy2) = to_conic(lat, lng)
        diff_x = cx2 - cx
        diff_y = cy2 - cy
        assert abs(diff_x) <= 0.001
        assert abs(diff_y) <= 0.001


def test_conus1_with_pyproj():
    """Verify that the projection gets the same answer as prproj for conus1"""

    projection_string = "+proj=lcc +lat_1=33 +lat_2=45 +lon_0=-96.0 +lat_0=39 +a=6378137.0 +b=6356752.31 +x_0=1885055.4995 +y_0=604957.0654"
    CONIC_TRANSFORMER = pyproj.Transformer.from_crs(
        "epsg:4326", projection_string, always_xy=False
    )
    points = []
    for x in range(0, int(3342 / 20)):
        for y in range(0, int(1888 / 20)):
            cx = x * 1000
            cy = y * 1000
            (lat, lng) = from_conic(cx, cy)
            points.append([x, y, cx, cy, lat, lng])
    for p in points:
        [x, y, cx, cy, lat, lng] = p
        (cx2, cy2) = to_conic(lat, lng)
        (px2, py2) = CONIC_TRANSFORMER.transform(lat, lng)
        diff_x = cx2 - px2
        diff_y = cy2 - py2
        # Within 0.001 meters the same
        assert abs(diff_x) <= 0.001
        assert abs(diff_y) <= 0.001


def test_conus2_with_pyproj():
    """Verify that the projection gets the same answer as prproj for conus2"""

    # Create a pyproj transformer
    projection_string = "+proj=lcc +lat_1=30 +lat_2=60 +lon_0=-97.0 +lat_0=40.0000076294444 +a=6370000.0 +b=6370000 +x_0=2208000.30881173 +y_0=1668999.65483222"
    CONIC_TRANSFORMER = pyproj.Transformer.from_crs(
        "epsg:4326", projection_string, always_xy=True
    )

    points = []
    for x in range(0, int(3342 / 20)):
        for y in range(0, int(1888 / 20)):
            cx = x * 1000
            cy = y * 1000
            (lat, lng) = from_conic(cx, cy, "conus2")
            points.append([x, y, cx, cy, lat, lng])
    for p in points:
        [x, y, cx, cy, lat, lng] = p
        (cx2, cy2) = to_conic(lat, lng, "conus2")
        (px2, py2) = CONIC_TRANSFORMER.transform(lng, lat)
        diff_x = cx2 - px2
        diff_y = cy2 - py2
        # Within 0.001 meters the same
        assert abs(diff_x) <= 0.01
        assert abs(diff_y) <= 0.01
