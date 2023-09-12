rm -rf html
sphinx-apidoc -f -e -o source ../src
make html
rm source/hydroframe.* source/modules.rst
rsync -av html/ .
rm -rf html
rm -rf .buildInfo .buildinfo objects.inv
rm -rf _sources doctrees
rm _static/_sphinx_javascript_frameworks_compat.js
rm _static/jquery.js