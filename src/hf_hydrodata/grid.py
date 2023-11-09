"""
Functions using a data catalog grid to perform conversions.
"""

# pylint: disable=W0603,C0103,E0401,W0702,C0209,C0301,R0914,R0912,W1514,E0633,R0915,R0913,C0302,W0632
import os
from typing import List
import numpy as np
import xarray as xr
from hf_hydrodata.data_model_access import load_data_model
from hf_hydrodata.projection import to_conic, from_conic


def to_latlon(grid: str, *args) -> List[float]:
    """
    Convert grid x,y coordinates to lat,lon.

    Args:
        grid:       The name of a grid dimension from the data catalog grid table (e.g. conus1 or conus2).
        args:       A list of numbers of (x,y) values that are coordinates in the grid (may be int or float).
    Returns:
        An array of lat,lon points converted from each of the (x,y) grid coordinates in args.

    Note, this may be used to convert a single point or a bounds of 2 points or a large array of points.

    This conversion is fast. It is about 100K+ points/second.

    For an example,
        (lat, lon) = to_latlon("conus1", 10, 10)

        latlon_bounds = to_latlon("conus1", *[0, 0, 20, 20])

        (lat, lon) = to_latlon("conus1", 10.5, 10.5)
    """
    result = []
    data_model = load_data_model()
    table = data_model.get_table("grid")
    grid_row = table.get_row(grid.lower())
    if grid_row is None:
        raise ValueError(f"No such grid {grid} available.")
    grid_resolution = float(grid_row["resolution_meters"])
    if len(args) == 0:
        raise ValueError("At least two x, y values must be provided.")
    if len(args) % 2 == 1:
        raise ValueError(
            "Number of args must be even number. E.g. list of (x, y) coordinates."
        )

    for index in range(0, len(args), 2):
        x = int(args[index] * grid_resolution)
        y = int(args[index + 1] * grid_resolution)
        lat, lon = from_conic(x, y, grid)
        result.append(lat)
        result.append(lon)
    return result

def from_latlon(grid: str, *args) -> List[float]:
    """
    Convert grid lat,lon coordinates to x,y float values in grid resolution coordinates from the grid origin.

    Args:
        grid:       The name of a grid dimension from the data catalog grid table (e.g. conus1 or conus2).
        args:       A list of floating pairs if (lat,lon) values.
    Returns:
        An array of x,y integer points converted from each of the (lat,lon) grid coordinates in args.

    Note, this may be used to convert a single point or a bounds of 2 points or a large array of points.

    This conversion is fast. It is about 100K+ points/second.

    For example,
        (x, y) = from_latlon("conus1", 31.759219, -115.902573)

        latlon_bounds = from_latlon("conus1", *[31.651836, -115.982367, 31.759219, -115.902573])
    """    
    result = []
    data_model = load_data_model()
    table = data_model.get_table("grid")
    grid_row = table.get_row(grid.lower())
    if grid_row is None:
        raise ValueError(f"No such grid {grid} available.")
    grid_resolution = float(grid_row["resolution_meters"])
    shape = grid_row["shape"]
    for index in range(0, len(args), 2):
        lat = args[index]
        lon = args[index + 1]
        (x, y) = to_meters(grid, lat, lon)
        x = x / grid_resolution
        y = y / grid_resolution
        if shape and len(shape) >= 2:
            # Check if x,y points are within the grid bounds
            bounds_x = float(shape[2])
            bounds_y = float(shape[1])
            if not (0 <= round(x) <= bounds_x and 0 <= round(y) <= bounds_y):
                raise ValueError(f"The lat/lon point maps to {x},{y} which is outside of grid bounds {bounds_x}, {bounds_y}")
        result.append(x)
        result.append(y)
    return result  

def to_meters(grid: str, *args) -> List[float]:
    """
    Convert grid lat,lon coordinates to x,y in meters from grid origin.

    Args:
        grid:       The name of a grid dimension from the data catalog grid table (e.g. conus1 or conus2).
        args:       A list of floating pairs if (lat,lon) values.
    Returns:
        An array of x,y integer points converted from each of the (lat,lon) grid coordinates in args.

    Note, this may be used to convert a single point or a bounds of 2 points or a large array of points.

    This conversion is fast. It is about 100K+ points/second.

    For example,
        (x, y) = to_meters("conus1", 31.759219, -115.902573)

        latlon_bounds = to_meters("conus1", *[31.651836, -115.982367, 31.759219, -115.902573])
    """
    result = []
    if len(args) == 0:
        raise ValueError("At least two x, y values must be provided.")
    if len(args) % 2 == 1:
        raise ValueError(
            "Number of args must be even number. E.g. list of (lat,lon) coordinates."
        )

    for index in range(0, len(args), 2):
        lat = args[index]
        lon = args[index + 1]
        (x, y) = to_conic(lat, lon, grid)
        result.append(x)
        result.append(y)
    return result

def to_ij(grid: str, *args) -> List[int]:
    """
        Convert grid lat,lon coordinates to i,j integers in grid resolution coordinates from grid origin.

        For example,
            (i, j) = to_ij("conus1", 31.759219, -115.902573)

            ij_bounds = to_ij("conus1", *[31.651836, -115.982367, 31.759219, -115.902573])        
    """

    result = [round(v) for v in from_latlon(grid, *args)]
    return result

def to_xy(grid: str, *args) -> List[float]:
    """
    Convert grid lat,lon coordinates to x,y float values in grid resolution coordinates from grid origin.

    For example,
        (x, y) = to_xy("conus1", 31.759219, -115.902573)

        xy_bounds = to_xy("conus1", *[31.651836, -115.982367, 31.759219, -115.902573])      
    """

    result = from_latlon(grid, *args)
    return result

