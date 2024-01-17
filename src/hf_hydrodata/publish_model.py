"""
    Publish a copy of the data catalog model to /hydrodata/data_catalog.
"""
# pylint: disable=E0401
import os
import shutil
import data_model_access

def main():
    """
    Copy publish the model .csv files to /hydrodata/catalog.

    This does nothing if /hydrodata/data_catalog folder does not exist.
    This creates the remote data catalog version folder if it does not exist.
    This copies all the .csv files from the src/hf_hydrodata/model folder
    to the /hydrodata/data_catalog/<version> folder.
    """

    version = data_model_access.REMOTE_DATA_CATALOG_VERSION
    local_dir = "./model"
    dc_dir = "/hydrodata/data_catalog"
    target_dir = f"{dc_dir}/{version}"
    if os.path.exists(dc_dir):
        if not os.path.exists(target_dir):
            os.mkdir(target_dir)
            os.chmod(target_dir, 0o775)
        for f in os.listdir(local_dir):
            if f.endswith(".csv"):
                src = f"{local_dir}/{f}"
                dst = f"{target_dir}/{f}"
                shutil.copyfile(src, dst)
                os.chmod(dst, 0o664)
                print(f"Copied to {dst}")

if __name__ == "__main__":
    main()
