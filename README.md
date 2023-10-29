# hf_hydrodata

Tools and utility to access data in the hydrodata hydrology file share.

## Installation

```bash
You can install the python package with the API to access files using pip with::


    pip install git+https://github.com/hydroframe/hf_hydrodata.git
```

## Usage

    from hf_hydrodata.data_access import get_numpy_data

    filters = ["dataset":"NLDAS2", "variable":"precipitation", period="daily", "start_time": "2005-03-01", "file_type": "pfb"]
    data = get_numpy_data(filters)
    print(data.shape)

    # Get the metadata about the returned data
    emtry = get_data_catalog_entry(filters)
    print(entry)

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

`hydroframe` was created by William M. Hasling, Laura Cosondon, Reed Maxwell, George Artavanis, Will Lytle, Amy M. Johnson, Amy C. Defnet. It is licensed under the terms of the MIT license.


