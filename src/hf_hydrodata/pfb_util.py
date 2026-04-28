"""
    Numpy pfb utility to save a numpy array as a parflow pfb file.
"""

import numpy as np
import parflow as pf


def generate_pfb_file(data: np.ndarray, file_path: str):
    """
    Generate a pfb file from numpy data array.
    Parameters:
        data:        a numpy array
        file_path:   the file path to be written
    """

    # A PFB file is always 3 dimensions z, y, z
    # Reshape the data if it is wrong.
    if len(data.shape) == 2:
        data = data.reshape(1, data.shape[0], data.shape[1])
    elif len(data.shape) == 1:
        data = data.reshap(1, 1, data.shape[0])
    elif len(data.shape) == 4:
        nz = data.shape[0] * data.shape[1]
        data = data.reshape(nz, data.shape[2], data.shape[3])

    pf.write_pfb(file_path, data, dist=None)
