# hf_hydrodata

Python component to access data in the hydrodata hydrology file share.

## Installation

The best way to install `hf_hydrodata` is using pip. This installs our 
latest stable release with fully-supported features:

    pip install hf_hydrodata

## Documentation

You can view the documentation at [ReadTheDocs](https://hf-hydrodata.readthedocs.io).

## Usage


You can use `hf_hydrodata` to get access to both gridded and point observation data from various
datasets available in hy_hydrodata.

You can view the available datasets and variables from [the documentation](https://hf-hydrodata.readthedocs.io)
or you can get the list of dataset and variables from functions.


    import hf_hydrodata as hf

    datasets = hf.get_datasets()
    variables = hf_get_variables()

    variables = hf.get_variables("dataset": "NLDAS2", "grid": "conus1")

You can get gridded data using the get_gridded_data() function.

    import hf_hydrodata as hf

    options = {
      "dataset": "NLDAS2", "variable": "precipitation", "period": "hourly",
      "start_time": "2005-10-1", "end_time": "2005-10-2", "grid_bounds": [100, 100, 200, 200]
    }
    data = hf.get_gridded_data(options)

You can use the ``point`` module to read site-level observations data from the hydrodata repository to get a 
pandas DataFrame.

``hf_hydrodata`` supports access to a collection of site-level data from a variety of sources. 

The below syntax will return daily USGS streamflow data from January 1, 2022 through January 5, 2022 
for sites that are within the bounding box with latitude bounds of (45, 50) and longitude bounds
of (-75, -50).

    # Import package
    from hf_hydrodata.point import get_data, get_metadata

    # Define filters and return as pandas DataFrame
    data_source = 'usgs_nwis'
    variable = 'streamflow'
    temporal_resolution = 'daily'
    aggregation = 'average'

    data = get_data(data_source, variable, temporal_resolution, aggregation,
                    start_date="2022-01-01", end_date="2022-01-05", 
                    latitude_range = (45, 50),
                    longitude_range = (-75, -50))
    data.head(5)

    # Get the metadata about the sites with returned data
    metadata = get_metadata(data_source, variable, temporal_resolution, aggregation,
                            start_date="2022-01-01", end_date="2022-01-05", 
                            latitude_range = (45, 50),
                            longitude_range = (-75, -50))
    metadata.head(5)


## Build Instructions

To build the component you must have a python virtual environment containing
the required components. Install the required components with:

    pip install -r requirements.txt

Edit the python components in `src/hf_hydrodata` and the unit tests in `tests/hf_hydrodata` and the data catalog model CSV files in `src/hf_hydrodata/model`.
Use Excel to edit the CSV files so that files are saved in standard CSV format.

Generate the documentation with:

    cd docs
    make html

This will validate the model CSV files and 
generate the read-the-docs html into the html folder.

## License

`hf_hydrodata` was created by William M. Hasling, Laura Condon, Reed Maxwell, George Artavanis, Will Lytle, Amy M. Johnson, Amy C. Defnet. It is licensed under the terms of the MIT license.


