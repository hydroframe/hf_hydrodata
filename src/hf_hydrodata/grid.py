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

    For example,
        (lat, lon) = to_latlon("conus1", 10, 10)

        latlon_bounds = to_latlon("conus1", *[0, 0, 20, 20])

        (lat, lon) = grid_to_latlon("conus1", 10.5, 10.5)
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
    Convert grid lat,lon coordinates to x,y.

    Args:
        grid:       The name of a grid dimension from the data catalog grid table (e.g. conus1 or conus2).
        args:       A list of floating pairs if (lat,lon) values.
    Returns:
        An array of x,y integer points converted from each of the (lat,lon) grid coordinates in args.

    Note, this may be used to convert a single point or a bounds of 2 points or a large array of points.

    This conversion is fast. It is about 100K+ points/second.

    For example,
        (x, y) = to_latlon("conus1", 31.759219, -115.902573)

        latlon_bounds = to_latlon("conus1", *[31.651836, -115.982367, 31.759219, -115.902573])
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
            "Number of args must be even number. E.g. list of (lat,lon) coordinates."
        )

    for index in range(0, len(args), 2):
        lat = args[index]
        lon = args[index + 1]
        (x, y) = to_conic(lat, lon, grid)
        result.append(round((x) / grid_resolution))
        result.append(round((y) / grid_resolution))
    return result


def get_huc_from_latlon(grid: str, level: int, lat: float, lon: float) -> str:
    """
        Get a HUC id at a lat/lon point for a given grid and level.

        Args:
            grid:   grid name (e.g. conus1 or conus2)
            level:  HUC level (length of HUC id to be returned)\
            lat:    lattitude of point
            lon:    longitude of point
        Returns:
            The HUC id string containing the lat/lon point or None.
    """
    huc_id = None
    tiff_ds = __get_geotiff(grid, level)
    [x, y] = from_latlon(grid, lat, lon)
    data = np.flip(tiff_ds[0].to_numpy(), 0)
    if 0 <= x <= data.shape[1] and 0 <= y <= data.shape[0]:
        huc_id = np.flip(tiff_ds[0].to_numpy(), 0)[y][x].item()
        if isinstance(huc_id, float):
            huc_id = str(huc_id).replace(".0", "")
    return huc_id


def get_huc_from_xy(grid: str, level: int, x: int, y: int) -> str:
    """
        Get a HUC id at an xy point for a given grid and level.

        Args:
            grid:   grid name (e.g. conus1 or conus2)
            level:  HUC level (length of HUC id to be returned)\
            x:      x coordinate in the grid
            y:      y coordinate in the grid
        Returns:
            The HUC id string containing the lat/lon point or None.
    """
    tiff_ds = __get_geotiff(grid, level)
    data = np.flip(tiff_ds[0].to_numpy(), 0)
    huc_id = None
    if 0 <= x <= data.shape[1] and 0 <= y <= data.shape[0]:
        huc_id = data[y][x].item()
        if isinstance(huc_id, float):
            huc_id = str(huc_id).replace(".0", "")
    return huc_id


def get_huc_bbox(grid: str, huc_id_list: List[str]) -> List[int]:
    """
    Get the grid bounding box containing all the HUC ids.

    Args:
        grid:           A grid id from the data catalog (e.g. conus1 or conus2)
        huc_id_list:    A list of HUC id strings of HUCs in the grid.
    Returns:
        A bounding box in grid coordinates as a list of int (i_min, j_min, i_max, j_max)
    Raises:
        ValueError if all the HUC id are not at the same level (same length).
        ValueError if grid is not valid.
    """
    # Make sure all HUC ids in the list are the same length
    level = None
    for huc_id in huc_id_list:
        if level is None:
            level = len(huc_id)
        elif len(huc_id) != level:
            raise ValueError("All HUC ids in the list must be the same length.")

    # Open the TIFF file of the grid and level
    tiff_ds = __get_geotiff(grid, level)

    result_imin = 1000000
    result_imax = 0
    result_jmin = 1000000
    result_jmax = 0
    for huc_id in huc_id_list:
        tiff_value = int(huc_id) if grid == "conus1" else float(huc_id)
        sel_huc = (tiff_ds == tiff_value).squeeze()

        # First find where along the y direction has "valid" cells
        y_mask = (sel_huc.sum(dim="x") > 0).astype(int)

        # Then, taking a diff along that dimension let's us see where the boundaries of that mask ar
        diffed_y_mask = y_mask.diff(dim="y")

        # Taking the argmin and argmax get's us the locations of the boundaries
        arr_jmax = (
            np.argmin(diffed_y_mask.values) + 1
        )  # this one is because you want to include this right bound in your slice
        arr_jmin = (
            np.argmax(diffed_y_mask.values) + 1
        )  # because of the point you actually want to indicate from the diff function

        jmin = tiff_ds.shape[1] - arr_jmax
        jmax = tiff_ds.shape[1] - arr_jmin

        # Do the exact same thing for the x dimension
        diffed_x_mask = (sel_huc.sum(dim="y") > 0).astype(int).diff(dim="x")
        imax = np.argmin(diffed_x_mask.values) + 1
        imin = np.argmax(diffed_x_mask.values) + 1

        # Extend the result values to combine multiple HUC ids
        result_imin = imin if imin < result_imin else result_imin
        result_imax = imax if imax > result_imax else result_imax
        result_jmin = jmin if jmin < result_jmin else result_jmin
        result_jmax = jmax if jmax > result_jmax else result_jmax

    return (result_imin, result_jmin, result_imax, result_jmax)


def __get_geotiff(grid: str, level: int) -> xr.Dataset:
    """
    Get an xarray dataset of the geotiff file for the grid at the level.

    Args:
        grid:   grid name (e.g. conus1 or conus2)
        level:  HUC level (length of HUC id to be returned)\
    Returns:
        An xarray dataset with the contents of the geotiff file for the grid and level.
    """

    data_model = load_data_model()
    table = data_model.get_table("data_catalog_entry")
    if grid == "conus1":
        catalog_row = table.get_row("198")
    elif grid == "conus2":
        catalog_row = table.get_row("199")
    else:
        raise ValueError("Only conus1 or conus2 are supported")
    path_template = catalog_row["path"]
    file_path = path_template.format(level=level)
    variable = catalog_row["dataset_var"]

    if not os.path.exists(file_path):
        raise ValueError("File '{file_path}' does not exist.")

    # Open TIFF file
    tiff_ds = xr.open_dataset(file_path).drop_vars(("x", "y"))[variable]
    return tiff_ds
