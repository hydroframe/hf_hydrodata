# Run this script to pushlish to pypi, this will prompt for pypi account which is "hmei-hydro"
# You must have poetry install to run this script

rm -rf dist
poetry build
twine upload dist/*
