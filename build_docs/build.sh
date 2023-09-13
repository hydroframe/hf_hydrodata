rm -rf html
sphinx-apidoc -f -e -o source ../src
make html
rm source/hydroframe.* source/modules.rst
rm -rf html/.buildInfo hml/.buildinfo html/objects.inv
rm -rf html/_sources doctrees
rm html/_static/_sphinx_javascript_frameworks_compat.js
rm html/_static/jquery.js
rsync -av html/ ../docs
rm -rf html