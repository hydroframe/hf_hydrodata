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


## License

`hydroframe` was created by William M. Hasling, Laura Cosondon, Reed Maxwell, George Artavanis, Will Lytle, Amy M. Johnson, Amy C. Defnet. It is licensed under the terms of the MIT license.


