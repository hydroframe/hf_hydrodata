"""
    Tiff utility to save a numpy array as a tiff file.
"""
# pylint: disable=R0914

import numpy as np
import pyproj
import rasterio
import hf_hydrodata as hf


def generate_tiff_file(
    data: np.ndarray, entry: dict, query_parameters: dict, filename: str
):
    """
    Generate a tiff file from numpy data array.
    Parameters:
        data:               A numpy array
        entry:              A dict containing the data catalog entry of the file.
        query_parameters:   A dict containing the query parameters of the query.
        file_path:          The file path to be written
    """
    grid = entry.get("grid")
    grid_bounds = _get_grid_bounds(query_parameters, entry)
    grid_data = hf.data_catalog.get_table_row("grid", id=grid)
    crs_string = grid_data["crs"]
    x_origin = grid_data["origin"][0]
    y_origin = grid_data["origin"][1]
    resolution_meters = grid_data["resolution_meters"]
    if not resolution_meters:
        raise ValueError(f"Grid {grid} does not have resolution_meters defined.")
    resolution_meters = float(resolution_meters)
    if (
        grid_bounds
        and len(grid_bounds) == 4
        and (
            query_parameters.get("huc_id")
            or query_parameters.get("grid_bounds")
            or query_parameters.get("latlon_bounds")
        )
    ):
        # Tiff files have origin at top left so left is grid_bounds[0] and top is grid_bounds[3]
        left_origin = x_origin + grid_bounds[0] * resolution_meters
        top_origin = y_origin + grid_bounds[3] * resolution_meters
    else:
        # If there no grid_bounds then the origin is the same as origin of the grid itself
        left_origin = x_origin
        top_origin = y_origin + grid_data["shape"][1] * resolution_meters

    # Remove false northing and false easting from CRS since this is handled by transform origins
    pos = crs_string.find("+x_0=")
    crs_string = crs_string[0:pos] if pos > 0 else crs_string

    transform = rasterio.transform.from_origin(
        left_origin, top_origin, resolution_meters, resolution_meters
    )
    if len(data.shape) == 3:
        pass
    elif len(data.shape) == 4:
        data = data[0, :, :, :]
    elif len(data.shape) == 2:
        data = np.expand_dims(data, axis=0)
    else:
        raise ValueError(f"Cannot create a tiff file with data with shape {data.shape}")
    data = np.flip(data, 1)
    dst_profile = {
        "driver": "GTiff",
        "dtype": np.float32,
        "nodata": 9999,
        "width": data.shape[2],
        "height": data.shape[1],
        "count": data.shape[0],
        "crs": pyproj.crs.CustomConstructorCRS(crs_string),
        "transform": transform,
        "tiled": True,
    }
    dst_profile["compress"] = "lzw"
    with rasterio.open(filename, "w", **dst_profile) as dst:
        for i in range(0, data.shape[0]):
            dst.write_band(i + 1, data[i])


def _get_grid_bounds(query_parameters, entry):
    """
    Get the grid_bounds from the query parameter information and the data catalog entry.

    Parameters:
        query_parameters:dict    The query parameter options passed in the request.
        entry:                   A data catalog entry.
    Returns:
        The grid_bounds of the query as [min_x, min_y, max_x, max_y]
    """

    grid_bounds = query_parameters.get("grid_bounds")
    if grid_bounds:
        if isinstance(grid_bounds, str):
            grid_bounds = json.loads(grid_bounds)
        return grid_bounds

    grid = entry.get("grid")
    huc_id = query_parameters.get("huc_id")
    if huc_id:
        grid_bounds = hf.gridded.get_huc_bbox(grid, huc_id.split(","))
        return grid_bounds

    # No subsettings, return the full size of grid
    grid_row = hf.data_catalog.get_table_row("grid", id=grid)
    shape = grid_row.get("shape")
    ny = shape[1]
    nx = shape[2]
    grid_bounds = [0, 0, ny, nx]
    return grid_bounds
