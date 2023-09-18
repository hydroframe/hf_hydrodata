rm -rf html
sphinx-apidoc -f -e -o source ../src "../src/hf_hydrodata/data_access*" "../src/hf_hydrodata/data_model_access*" "../src/hf_hydrodata/projection.*" "../src/hf_hydrodata/generate_hydrodata_catalog_yaml*"
make html
rm source/modules.rst
rm -rf html/.buildInfo hml/.buildinfo html/objects.inv
rm -rf html/_sources doctrees html/.buildinfo
rm html/_static/_sphinx_javascript_frameworks_compat.js
rm html/_static/jquery.js
rsync -av html/ ../docs
rm -rf html
