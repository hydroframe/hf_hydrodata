##
## Validate the CSV files and generate a yaml file with the data catalog
##
cd ../../src/hf_hydrodata
rm -f hydrodata_catalog.yaml.bkp
touch hydrodata_catalog.yaml
cp hydrodata_catalog.yaml hydrodata_catalog.yaml.bkp
python generate_hydrodata_catalog_yaml.py
if [ $? -ne 0 ]; then
    echo
    echo "Failed hydrodata_catalog.yaml validation."
    exit -1
fi
