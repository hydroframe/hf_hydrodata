# hf_hydrodata

Tools and utility to access data in the hydrodata hydrology file share.

## Installation

The best way to install `hf_hydrodata` is using pip. This installs our 
latest stable release with fully-supported features:

    pip install hf_hydrodata

You can also install the latest development version by cloning the GitHub repository and using pip
to install from the local directory:  

    pip install git+https://github.com/hydroframe/hf_hydrodata.git

## Usage

### Accessing Gridded Data

You can use the ``gridded`` module to read gridded data and select site-level data from the 
hydrodata repository to get a NumPy array. 

The below syntax will return daily NLDAS2 precipitation files for March 1, 2005. Without specification,
gridded data will be returned on the CONUS1 grid (citation?/reference?) but a different grid or grid subset
can be specified by the user. Please see the [Python API Reference](https://maurice.princeton.edu/hydroframe/docs/api_reference.html) for a full list of available parameters and supported
features.

The user can also request the metadata for the specified file. This includes information on the 
variable units, time zone, overall time availability for this data source, any relevant DOI citations,
and many other fields. A full description of the metadata returned can be found in the gridded section of the package [documentation](https://maurice.princeton.edu/hydroframe/docs/gridded_data/index.html).

    # Import package
    from hf_hydrodata.gridded import get_numpy, get_catalog_entry

    # Define filters and return as NumPy array
    filters = {"dataset":"NLDAS2", "variable":"precipitation", "period":"daily", "start_time": "2005-03-01"}
    data = get_numpy(filters)
    print(data.shape)

    # Get the metadata about the returned data
    metadata = get_catalog_entry(filters)
    print(metadata)

Many of the files are very large so parameters can be provided to subset the files by space and/or time before
returning the data. See the [documentation](https://maurice.princeton.edu/hydroframe/docs/gridded_data/index.html) for details about the available parameters
that can be passed to the functions to filter data by space and/or time.

### Accessing Point Observations

You can use the ``point`` module to read site-level observations data from the hydrodata repository to get a 
pandas DataFrame.

``hf_hydrodata`` supports access to a collection of site-level data from a variety of sources. 
Please see the [documentation](https://maurice.princeton.edu/hydroframe/docs/point_data/index.html) for a full list of what is available and details on our 
data collection process.

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

Please see the [How To](https://maurice.princeton.edu/hydroframe/docs/examples/index.html) section of our documentation for additional example workflows.

## Build Instructions

To build the component you must have a python virtual environment containing
the required components. Install the required components with:

    pip install -r requirements.txt

Edit the python components in src/hf_hydrodata and the unit tests in tests/hf_hydrodata and the data catalog model CSV files in src/hf_hydrodata/model.
Use Excel to edit the CSV files so that files are saved in standard CSV format.

Generate the documentation with:

    cd build_docs
    bash build.sh

This will validate the model CSV files and 
generate the read-the-docs html into deploy_docs folder.
After committing to the main branch the CI/CD job will copy the deploy_docs folder to the public website for the documentation.

## License

`hf_hydrodata` was created by William M. Hasling, Laura Condon, Reed Maxwell, George Artavanis, Will Lytle, Amy M. Johnson, Amy C. Defnet. It is licensed under the terms of the MIT license.


