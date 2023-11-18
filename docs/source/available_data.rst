.. _available_data:

Available Datasets and Data Products
========================================
Gridded data is data that can be represented in a grid such as a numpy array.
The gridded data stored in the hydrodata repository is accessed using attributes that can be used
to filter and slice the requested data.

Gridded data is organized by datasets. A dataset is a collection of data variables collected
or generated together. Data can be identified by filtering by the dataset, variable and period as well
as other optional filters such as grid or aggregation to identify data. 

Data can also be sliced by space using python parameters: 'grid_bounds' or 'latlon_bounds'.
Data can be sliced by time using python parameters: 'start_time' and 'end_time'.
See examples in the QuickStart Guide section.

Click on a Dataset Attribute Key below to see the variables and description of each supported dataset.

.. include:: ./gen_dataset_list.rst

