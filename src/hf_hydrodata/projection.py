"""
    Convert lat/lng (EPSG:4326) to to  Lambert Confirmal Conic (ESRI:102004) for conus 1.

    Do this using the projection constants of the coordinate systems obtained from the
    python pyproj package. This module is a custom implementation of lat/lng to conic transformation
    that is 35x faster than the pyproj.Transform class. The speed improvement is because the
    code is not generic and can be compiled instead interpreting all the possible
    coordinates types supported by pyproj. Since the algorithm and conversion constants are 
    the same as pyproj the answer is the same for conus1.
    For conus2 the conversion constants are different.

    The unit tests compares the answer with pyproj to verify that the two functions produce
    the same answer within 0.001 meters.
    
    Inspiration (not code) taken from:
        https://github.com/vraida/Lambert-projection
        https://en.wikipedia.org/wiki/Lambert_conformal_conic_projection

"""
# pylint: disable=C0103,W0703,E0401,E0633,R0902

from typing import List
import math
import json
from hf_hydrodata.data_model_access import load_data_model

def to_conic(lat: float, lng: float, grid="conus1") -> List[float]:
    """
    Convert lat/lng point to conic x,y point.
    Args:
        lat:    Latitude in degrees.
        lng:    Longitude in degress.
    Returns:
        A tuple (x, y) as a float in flat projected conic coordinates in meters.
    """

    phi = math.radians(lat)
    lmbda = math.radians(lng)
    constants = _get_constants(grid)
    t = constants.calculate_t(phi)
    rho = constants.r * constants.f * math.pow(t, constants.n)
    theta = constants.n * (lmbda - constants.lmbda_0)

    x = rho * math.sin(theta) + constants.false_easting
    y = constants.rho_0 - rho * math.cos(theta) + constants.false_northing
    return (x, y)


def from_conic(x: float, y: float, grid="conus1") -> List[float]:
    """
    Convert conic x,y point to lat/lng point.
    Args:
        lat:    x position in meters in conic flat coordinates
        lng:    y position in meters in conic flat coordinates
    Returns:
        A tuple (lat, lng) as a float in degrees in EPSG:4326 coordinates.
    """

    constants = _get_constants(grid)
    x = x - constants.false_easting
    y = y - constants.false_northing
    theta = math.atan(x / (constants.rho_0 - y))
    lmbda = theta / constants.n + constants.lmbda_0
    rho = x / math.sin(theta)
    t = math.exp(math.log(rho / (constants.r * constants.f)) / constants.n)
    phi = constants.un_calculate_t(t)
    lat = math.degrees(phi)
    lng = math.degrees(lmbda)

    return (lat, lng)


class ProjConstants:
    """
    Constants from the pyproj package used for the EPSG:4326 to ESRI:102004 transformation.
    """

    def __init__(self, grid: str):
        """Initialize constants"""

        # Use constants instead of calling pyproj because
        # pyproj produces an ugly warning when using ESRI coordinates, so hard code constants
        data_model = load_data_model()
        table = data_model.get_table("grid")
        grid_row = table.get_row(grid)
        if grid_row is None:
            raise ValueError(f"Grid '{grid}' is not recognized")
        grid_origin = str(grid_row["origin"])
        crs = grid_row["crs"]
        crs = crs.strip()
        if crs is None:
            raise ValueError(f"Grid '{grid}' does not have a projection.")
        crs_dict = self._parse_crs(crs)

        self.r = float(crs_dict.get("a"))
        self.a = float(crs_dict.get("a"))
        self.b = float(crs_dict.get("b"))
        self.flattening = (self.a - self.b)/(self.a)
        self.first_parallel = float(crs_dict.get("lat_1"))
        self.second_parallel = float(crs_dict.get("lat_2"))
        self.origin_latitude = float(crs_dict.get("lat_0"))
        self.origin_longitude = float(crs_dict.get("lon_0"))

        # Flattening for conus1 = 0.0033528106647474805
        self.false_easting = 0.0
        self.false_northing = 0.0
        if grid_origin:
            # Use the grid_origin from the data catalog as the false easting and northing
            grid_origin_array = json.loads(grid_origin)
            if len(grid_origin_array) == 2:
                self.false_easting = -float(grid_origin_array[0])
                self.false_northing = -float(grid_origin_array[1])

        self.phi_0 = math.radians(self.origin_latitude)
        self.phi_1 = math.radians(self.first_parallel)
        self.phi_2 = math.radians(self.second_parallel)
        self.lmbda_0 = math.radians(self.origin_longitude)
        self.ecc = math.sqrt(self.flattening * (2 - self.flattening))
        self.m1 = self.calculate_m(self.phi_1)
        self.m2 = self.calculate_m(self.phi_2)
        self.t0 = self.calculate_t(self.phi_0)
        self.t1 = self.calculate_t(self.phi_1)
        self.t2 = self.calculate_t(self.phi_2)
        self.n = (math.log(self.m1) - math.log(self.m2)) / (
            math.log(self.t1) - math.log(self.t2)
        )
        self.f = self.m1 / (self.n * math.pow(self.t1, self.n))
        self.rho_0 = self.r * self.f * math.pow(self.t0, self.n)
        self.from_conic_transformer = None

    def calculate_m(self, x: float) -> float:
        """Return the M value associated with the x radians values as required by the algorithm."""
        return math.cos(x) / (1 - self.ecc**2 * math.sin(x) ** 2) ** 0.5

    def calculate_t(self, x):
        """Return the T value associated with the x radians values as required by the algorithm."""
        return math.tan(math.pi / 4 - x / 2) / (
            (1 - self.ecc * math.sin(x)) / (1 + self.ecc * math.sin(x))
        ) ** (self.ecc / 2)

    def un_calculate_t(self, t):
        """Return the x value given t where x=calculate_t(x) using numerical iteration method."""

        guess_1 = 0.0
        guess_2 = 0.5
        for _ in range(7):
            val_1 = self.calculate_t(guess_1) - t
            val_2 = self.calculate_t(guess_2) - t
            if abs(guess_1 - guess_2) < 0.00000000001:
                return guess_2
            slope = (val_1 - val_2) / (guess_1 - guess_2)
            next_guess = guess_2 - val_2 / slope
            guess_1 = guess_2
            guess_2 = next_guess
        return guess_2

    def _parse_crs(self, crs:str)->dict:
        """Parse a pyproj crs string into a dict"""
        result = {}
        crs_parts = crs.split(" ")
        for part in crs_parts:
            (param, value) = part.split("=")
            param = param.replace("+", "")
            result[param] = value
        return result

PROJ_CONSTANTS = {}
def _get_constants(grid:str)->ProjConstants:
    result = PROJ_CONSTANTS.get(grid)
    if result is None:
        result = ProjConstants(grid)
        PROJ_CONSTANTS[grid] = result
    return result
