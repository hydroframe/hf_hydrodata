"""
Functions to access data_catalog metadata.
"""

import hf_hydrodata as hf

def get_citations(*args, **kwargs) -> str:
    """
    Get citation references for a dataset.

    Args:
        dataset:    The name of a dataset.
    Returns:
        A string containing citation references of the dataset.
    """

    if len(args) > 0 and isinstance(args[0], dict):
        options = args[0]
        dataset = options.get("dataset")
    elif len(args) > 0 and isinstance(args[0], str):
        dataset = args[0]
    else:
        options = kwargs
        dataset = options.get("dataset")

    if not dataset:
        raise ValueError("Dataset is not specified.")
    
    entries = hf.get_catalog_entries(dataset=dataset)
    if entries is None or len(entries) == 0:
        raise ValueError(f"No such dataset '{dataset}'")
    entry = entries[0]
    result = ""
    description = entry["description"]
    paper_dois = entry["paper_dois"]
    dataset_dois = entry["dataset_dois"]
    print(entry)
    result = result + f"{description}\n"
    found_reference = False
    if paper_dois:
        for doi in paper_dois.split(" "):
            if doi:
                doi = doi.replace(";", "")
                result = result + f"  Source: https://doi.org/{doi}\n"
                found_reference = True
    if dataset_dois:
        for doi in dataset_dois.split(" "):
            if doi:
                doi = doi.replace(";", "")
                result = result + f"  Source: {doi}\n"
                found_reference = True
    if not found_reference:
        result = result + "No paper references available.\n"
    return result

