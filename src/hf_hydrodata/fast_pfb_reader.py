"""
    Fast PFB file reading using parallel threads to read and subset many pfb files.
    Returns a single numpy array so this can only be used to read files and bounds
    where the result still fits in memory.

    The function read_files() has the same first two arguments as the parflow.read_pfb_sequence()
    and returns a numpy array with the same dimensions as parflow.read_pfb_sequence().

    The read_files() function reads files in parallel threads, but is 10x faster than
    calling read_pfb_sequence() in parallel threads because of the start up overhead of calling
    read_pfb_sequence for the first file in each thread call.
     
    The read_files() function also does not support the additional default parameters
    of read_pfb_sequence so this is not a drop in replacement for read_pfb_sequence.

"""

# pylint: disable=C0411,R0914,R0913,C0301

from typing import List
import math
import numpy as np
import concurrent.futures

INT_DT = np.dtype(np.int32).newbyteorder(">")
FLOAT_DT = np.dtype(np.float64).newbyteorder(">")
FLOAT_BYTES = 8
INT_BYTES = 4
FILE_HEADER_BYTES = 64
SUBGRID_HEADER_BYTES = 36

def read_files(pfb_files: List[str], pfb_constraints: dict = None):
    """
    Read and subset a list of pfb files.

    Parameters:
        pfb_files:      A list of pfb files to be read or a single pfb file name.
        pfb_constaints: A dict with keys: x, y, z with values a dict of start, stop.

    If pfb_constraints is None then reads the entire contents of all pfb_files.
    If the z part of the constraint is missing or start and stop are both 0 then returns all pfb file z values.

    Returns:
        A numpy array of dimemensions (n, z, y, x) where n is number of files.
    Throws:
        ValueError:  If the pfb_files parameters is missing or empty, or the the returned numpy array is too big.

    The returned numpy array is too big if it contains more then 347115648 cells (24 days of conus2 3D array).

    For example,

    .. code-block:: python

        data = read_files(["a.pfb", "b.pfb"] {"x":{"start": 10, "stop": 50}, "y": {"start": 20, "stop", "50"}, "z": {"start": 0, "stop": 0}})

    If the pfb files have dimensions (25, 3247, 4222) the return numpy array is (2, 25, 30, 40).
    """

    if pfb_files is None:
        raise ValueError("No pfb_files.")
    if isinstance(pfb_files, str):
        pfb_files = [pfb_files]
    if len(pfb_files) == 0:
        raise ValueError("The pfb_files list is empty.")

    # Read the pfb file header of the first file to get the shape, topology and subgrid size
    with open(pfb_files[0], "rb") as fp:
        (pfb_shape, sg_nxyz, pqr) = _read_file_header(fp)
        if pfb_constraints is None:
            pfb_constraints = {
                "x": {"start": 0, "stop": pfb_shape[0]},
                "y": {"start": 0, "stop": pfb_shape[1]},
                "z": {"start": 0, "stop": 0},
            }

    # Get the x,y,z and size of the pfb constraints
    x = (
        pfb_constraints.get("x").get("start")
        if pfb_constraints is not None and pfb_constraints.get("x")
        else 0
    )
    y = (
        pfb_constraints.get("y").get("start")
        if pfb_constraints is not None and pfb_constraints.get("y")
        else 0
    )
    z = (
        pfb_constraints.get("z").get("start")
        if pfb_constraints is not None and pfb_constraints.get("z")
        else None
    )
    x_size = (
        pfb_constraints.get("x").get("stop") - x
        if pfb_constraints is not None and pfb_constraints.get("x")
        else pfb_shape[0]
    )
    y_size = (
        pfb_constraints.get("y").get("stop") - y
        if pfb_constraints is not None and pfb_constraints.get("y")
        else pfb_shape[1]
    )
    z_size = pfb_constraints.get("z").get("stop") - z if z is not None else None
    z_size = None if z_size == 0 else z_size
    z = 0 if z is None else z
    z_size = z_size if z_size is not None else pfb_shape[2]
    x_size = x_size + 1 if x_size == 0 else x_size
    y_size = y_size + 1 if y_size == 0 else y_size

    # result_shape is (n, time, z, y, x) where n is the number of files
    result_shape = (len(pfb_files), z_size, y_size, x_size)

    max_memory_size = 2000000000
    if (
        result_shape[0] * result_shape[1] * result_shape[2] * result_shape[3]
        > max_memory_size
    ):
        raise ValueError("Requested returned numpy array is larger than 2GB.")

    # Check if we would run out of memory reading subgrids of the files in parallel (depends on PQR)
    # Determine the maximum numober of files that could be read in parallel with shape and pqr of the files.
    # Each thread will be reading 1 subgrid for each parallel file being executed so running too many files
    # in parallel might exceed the max memory size even if the final result has enough memory.
    max_files = int(
        max_memory_size
        / (pfb_shape[0] / pqr[0] * pfb_shape[1] / pqr[1] * pfb_shape[2] / pqr[2])
    )

    if max_files <= 0:
        raise ValueError(
            f"The PQR of the pfb files is {pqr} which is too small to read the file of shape{pfb_shape}."
        )

    # The max files cannot be more than the total number of pfb_files requested
    max_files = min(len(pfb_files), max_files)

    # Pre-create the numpy array to be returned
    np_values = np.zeros(result_shape)

    # Read files in parallel in blocks of max_files
    index = 0
    while index < len(pfb_files):
        # Read a block of files in parallel
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            while len(futures) < max_files and index < len(pfb_files):
                pfb_file = pfb_files[index]
                future = executor.submit(
                    read_file,
                    pfb_file,
                    x,
                    y,
                    z,
                    x_size,
                    y_size,
                    z_size,
                    pfb_shape,
                    sg_nxyz,
                    pqr,
                    np_values,
                    index,
                )
                index = index + 1
                futures.append(future)

            # Wait for all the threads in the block to finish before starting next block
            _ = [future.result() for future in concurrent.futures.as_completed(futures)]
            futures = []

    return np_values


def read_file(
    pfb_file: str,
    x: int,
    y: int,
    z: int,
    x_size: int,
    y_size: int,
    z_size: int,
    pfb_shape: List[int],
    sg_nxyz: List[int],
    pqr: list[int],
    np_values,
    index,
):
    """
    Read a subset of data from a single PFB file.
    Parameters:
        pfb_file:       Path to pfb file.
        x:              X position of start of subset to read
        y:              Y position of start of subset to read
        z:              Z position of start of subset to read
        x_size:         Number of x cells in subset to read
        y_size:         Number of y cells in subset to read
        z_size:         Number of z cells in subset to read
        pfb_shape:      Tuple or list PQR topology of pfb file (P, Q R)
        np_values:      A pre-created numpy array to hold the result data read
        index:          Index of the pfb file to be read
    Reads the pfb file and stores the z,y,z data into the [index, z, y, z] of the np_values array.
    """
    # Open the PFB file
    with open(pfb_file, "rb") as fp:
        # Find the subgrid number of the x,y starting point of the constraints
        subgrid_num = find_subgrid(x, y, pfb_shape, sg_nxyz, pqr)

        # Loop to read all the subgrids with data in the pfb_constraint
        # Loop no more than p*q times to prevent an infinte loop

        previous_row_subgrid_num = subgrid_num
        for _ in range(0, pqr[0] * pqr[1]):
            subgrid_position, header_sg_nxyz = _copy_data_from_subgrid(
                fp,
                subgrid_num,
                x,
                y,
                z,
                x_size,
                y_size,
                z_size,
                pfb_shape,
                sg_nxyz,
                pqr,
                np_values,
                index,
            )
            if x + x_size > subgrid_position[0] + header_sg_nxyz[0]:
                # There are more subgrids with desired data in the same y row
                subgrid_num = subgrid_num + 1
            elif y + y_size > subgrid_position[1] + header_sg_nxyz[1]:
                # There are no more subgrids in the same y row, but there is another y row
                subgrid_num = previous_row_subgrid_num + pqr[0]
                previous_row_subgrid_num = subgrid_num
            else:
                # We loaded all the data from all the subgrids requested
                break


def _copy_data_from_subgrid(
    fp,
    subgrid_num,
    x: int,
    y: int,
    z: int,
    x_size: int,
    y_size: int,
    z_size: int,
    pfb_shape: List[int],
    sg_nxyz: List[int],
    pqr: list[int],
    np_values,
    index,
):
    """
    Read a subgrid and copy data from the subgrid_num to approprate place in np_values.

    Returns:
        A tuple (subgrid_position, header_sg_nxyz) of the subgrid that was copied.

    Where subgrid_position is (x,y,z) position of the subgrid.
    and header_sg_nxyz is (nx, ny, nz) size of the subgrid.
    """

    # Find the byte offset of the subgrid from the start of the file
    offset = get_subgrid_offset(subgrid_num, pfb_shape, sg_nxyz, pqr)
    # Read the subgrid from that offset byte position
    data, subgrid_position, header_sg_nxyz = _read_subgrid(fp, offset, sg_nxyz)

    # Compute start position of the data to be copied in the subgrid and the np_values array for X dimension
    if subgrid_position[0] < x:
        # this subgrid is first X subgrid in a row and request does not start at beginning of subgrid
        target_x = 0
        subgrid_x = x - subgrid_position[0]
    else:
        # this a subsequent subgrid in the same X row as the first one, start copying from beginning of subgrid
        target_x = subgrid_position[0] - x
        subgrid_x = 0

    # Compute end position of the data to be copied in the subgrid and the np_values array for X dimension
    if x + x_size < subgrid_position[0] + header_sg_nxyz[0]:
        # the X request range ends before the end of this subgrid
        end_target_x = x_size
        end_subgrid_x = (x + x_size) - subgrid_position[0]
    else:
        # We copy all X data to the end of this subgrid
        end_target_x = subgrid_position[0] + header_sg_nxyz[0] - x
        end_subgrid_x = header_sg_nxyz[0]

    # Compute start position of the data to be copied in the subgrid and the np_values array for Y dimension
    if subgrid_position[1] < y:
        target_y = 0
        subgrid_y = y - subgrid_position[1]
    else:
        target_y = subgrid_position[1] - y
        subgrid_y = 0

    # Compute end position of the data to be copied in the subgrid and the np_values array for Y dimension
    if y + y_size < subgrid_position[1] + header_sg_nxyz[1]:
        end_target_y = y_size
        end_subgrid_y = (y + y_size) - subgrid_position[1]

    else:
        end_target_y = subgrid_position[1] + header_sg_nxyz[1] - y
        end_subgrid_y = header_sg_nxyz[1]

    # Compute Z dimension start and end position from the subgrid and the np_value target array
    target_z = 0
    end_target_z = target_z + z_size
    subgrid_z = z
    end_subgrid_z = subgrid_z + z_size

    # Copy the data from the subgrid into the target np_values array
    if end_subgrid_x > subgrid_x and end_subgrid_y > subgrid_y:
        np_values[
            index,
            target_z:end_target_z,
            target_y:end_target_y,
            target_x:end_target_x,
        ] = data[
            subgrid_z:end_subgrid_z,
            subgrid_y:end_subgrid_y,
            subgrid_x:end_subgrid_x,
        ]
    else:
        raise ValueError(f"PFB File read failure of subgrid {subgrid_num}.")
    # return the position and header of the subgrid that was copied
    return subgrid_position, header_sg_nxyz


def _read_file_header(fp):
    """
    Read the master pfb file header of the file:
    Args:
        fp:     File pointer of the open PFB file.
    Returns:
         A tuple (pfb_shape, sgn_xyz, pqr).

    Where
        pfb_shape:  List[NX, NY, NZ] of full PFB file.
        sgn_xyz:    List[SG_NX, SG_NY, SG_NZ] shape of first subgrid.
        pqr:        List[P, Q, R] topology of the PFB file.
    Note in the file the first subgrid and all full subgrids are shape SGN__XYZ.
    Some subgrids dimensions are smaller by 1 in X or Y dimension because of
    reminders when P,Q,R is divided by the shape.
    """

    fp.seek(0)
    contents = fp.read(FILE_HEADER_BYTES + SUBGRID_HEADER_BYTES)
    offset = 3 * FLOAT_BYTES
    pfb_shape = np.frombuffer(
        contents[offset : offset + 3 * INT_BYTES], dtype=INT_DT
    ).tolist()
    offset = FILE_HEADER_BYTES
    subgrid_header = np.frombuffer(
        contents[offset : offset + 9 * INT_BYTES], dtype=INT_DT
    )
    _, _, _, sg_nx, sg_ny, sg_nz, _, _, _ = subgrid_header
    sg_nxyz = [int(sg_nx), int(sg_ny), int(sg_nz)]

    p = math.ceil(pfb_shape[0] / sg_nxyz[0])
    q = math.ceil(pfb_shape[1] / sg_nxyz[1])
    r = math.ceil(pfb_shape[2] / sg_nxyz[2])
    pqr = (p, q, r)
    return (pfb_shape, sg_nxyz, pqr)


def find_subgrid(
    x: int, y: int, pfb_shape: List[int], sg_nxyz: List[int], pqr: List[int]
) -> int:
    """Find the subgrid number that contains the x,y point."""

    (p, q, _) = pqr
    (sg_nx, sg_ny, _) = sg_nxyz
    (nx, ny, _) = pfb_shape

    # Get x and y remainder sized subgrids
    remain_x = nx % p
    remain_y = ny % q

    # Compute result x subgrid number in row
    result_x = math.floor(x / sg_nx)
    remain_parts = result_x - remain_x
    full_parts = result_x - remain_parts
    if result_x > remain_x:
        if x - full_parts * sg_nx - remain_parts * (sg_nx - 1) >= (sg_nx - 1):
            result_x = result_x + 1

    # Compute result y subgrid row number
    if y <= remain_y * sg_ny:
        # y position is before remainder y rows in file
        # So y subgrid index is just y / sg_ny since all are full y rows that are sg_ny each
        result_y = math.floor(y / sg_ny)
    else:
        # y position as after remainder y rows in file
        # so y subgrid index is the full remain_y rows plus the remaining sg_ny-1 size rows
        result_y = remain_y + math.floor((y - remain_y * sg_ny) / (sg_ny - 1))

    # Subgrid number is result_y subgrid rows plus the result_x subgrid in that last row
    subgrid = math.floor(result_y * p + result_x)
    return subgrid


def get_subgrid_offset(
    subgrid_num: int, pfb_shape: List[int], sg_nxyz, pqr: List[int]
) -> int:
    """
    Get the byte offset in the file of the subgrid header the subgrid_num.
    This return the same value as the value from the .dist file for a subgrid number.
    The byte offset for subgrid 0 is 64 (after file header).
    """

    (nx, ny, _) = pfb_shape
    (p, q, _) = pqr
    remain_x = nx % p
    remain_y = ny % q
    part_x = p - remain_x
    full_x = remain_x
    full_y = remain_y if remain_y > 0 else ny / q

    sg_nx, sg_ny, sg_nz = sg_nxyz

    subgrid_full_full_bytes = sg_nx * sg_ny * sg_nz * FLOAT_BYTES + SUBGRID_HEADER_BYTES
    subgrid_full_part_x_bytes = (
        sg_nx - 1
    ) * sg_ny * sg_nz * FLOAT_BYTES + SUBGRID_HEADER_BYTES
    subgrid_part_y_full_bytes = (
        sg_nx * (sg_ny - 1) * sg_nz * FLOAT_BYTES + SUBGRID_HEADER_BYTES
    )
    subgrid_part_y_partx_bytes = (sg_nx - 1) * (
        sg_ny - 1
    ) * sg_nz * FLOAT_BYTES + SUBGRID_HEADER_BYTES

    full_row_full_y_bytes = (
        full_x * subgrid_full_full_bytes + part_x * subgrid_full_part_x_bytes
    )
    full_row_part_y_bytes = (
        full_x * subgrid_part_y_full_bytes + part_x * subgrid_part_y_partx_bytes
    )

    y = int(subgrid_num / p)
    x = subgrid_num - y * p
    result = FILE_HEADER_BYTES
    if y < full_y:
        result = result + y * full_row_full_y_bytes
        if x < full_x:
            result = result + x * subgrid_full_full_bytes
        else:
            result = result + full_x * subgrid_full_full_bytes
            result = result + (x - full_x) * subgrid_full_part_x_bytes
    else:
        result = result + full_y * full_row_full_y_bytes
        result = result + (y - full_y) * full_row_part_y_bytes
        if x <= full_x:
            result = result + x * subgrid_part_y_full_bytes
        else:
            result = result + full_x * subgrid_part_y_full_bytes
            result = result + (x - full_x) * subgrid_part_y_partx_bytes
    return result


def _read_subgrid(fp, subgrid_offset: int, sg_nxyz: List[int]):
    """
    Read the data in the subgrid.
    Parameters:
        fp:             File pointer of the open pfb file.
        subgrid_offset: Offset in bytes of the beginning of the subgrid header in the file.
        ng_nxyz:        An array (nx, ny, nz) of largest subgrid for the PQR of the file.

    Returns:
        (data, subgrid_position, subgrid_sg_nx)

    Where data is a numpy array containing data of the subgrid
    and subgrid_position is the (x, y, z) cell position of the subgrid.
    and subgrid_sg_nx is the (nx, ny, nz) size of the subgrid that is read (maybe be smaller than ng_nxyz).
    """

    (sg_nx, sg_ny, sg_nz) = sg_nxyz
    fp.seek(subgrid_offset)
    subgrid_size = sg_nx * sg_ny * sg_nz * FLOAT_BYTES
    contents = fp.read(subgrid_size + 9 * INT_BYTES)
    subgrid_header = np.frombuffer(contents[0 : 9 * INT_BYTES], dtype=INT_DT)
    subgrid_position = [
        int(subgrid_header[0]),
        int(subgrid_header[1]),
        int(subgrid_header[2]),
    ]
    header_sg_nx = int(subgrid_header[3])
    header_sg_ny = int(subgrid_header[4])
    header_sg_nz = int(subgrid_header[5])
    subgrid_sg_nx = [header_sg_nx, header_sg_ny, header_sg_nz]
    header_subgrid_size = header_sg_nx * header_sg_ny * header_sg_nz * FLOAT_BYTES
    offset = 9 * INT_BYTES
    data = np.frombuffer(
        contents[offset : offset + header_subgrid_size], dtype=FLOAT_DT
    ).reshape((header_sg_nz, header_sg_ny, header_sg_nx))
    return (data, subgrid_position, subgrid_sg_nx)
