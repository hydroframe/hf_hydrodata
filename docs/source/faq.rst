.. _faq:

Frequently Asked Questions
==============================

I got a MaintenanceError when trying to access data, what does this mean?
--------------------------------------------------------------------------------
The ``MaintenanceError`` Exception indicates that data access is currently restricted due to a maintenance window.
This is a scheduled period during which the system is undergoing updates or maintenance, and data access is
temporarily unavailable. The maintenance window is typically set to occur on the second Tuesday of each month
from 6 AM to 2 PM Eastern Time. During this time, users may (but not always) experience errors related to data access.
If you encounter this error, we suggest trying again after the maintenance window has ended. 
If you continue to experience issues after the maintenance period, please open a GitHub Issue with a detailed description 
of the problem.

How can I download a copy of the ma_2025 30m Water Table Depth Product?
----------------------------------------------------------------------------
You can download a subset of the 30m water table depth file using the `hf_hydrodata.get_gridded_data() <https://hf-hydrodata.readthedocs.io/en/latest/hf_hydrodata.gridded.html#hf_hydrodata.gridded.get_gridded_data>`_ function using
a grid_bounds or latlon_bounds filter limited to about 2GB download.

You can download the full version of the 30m water table depth using the `hf_hydrodata.get_raw_file() <https://hf-hydrodata.readthedocs.io/en/latest/hf_hydrodata.gridded.html#hf_hydrodata.gridded.get_raw_file>`_ function. 

See the Python API Reference documentation of the `hf_hydrodata.get_raw_file() <https://hf-hydrodata.readthedocs.io/en/latest/hf_hydrodata.gridded.html#hf_hydrodata.gridded.get_raw_file>`_ function for examples.
You can get this as either as a tiff file or a cog file.
You can also get the ma_2025 wtd_uncertainty variable and the belitz_2019 dataset variables the same way.

We also have the full ma_2025 water table depth dataset available as a tiff file
on `Zenodo <https://zenodo.org/records/18504963>`_ as a direct download.