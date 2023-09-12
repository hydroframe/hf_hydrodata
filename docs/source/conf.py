# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__),"..", "../src")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__),"..", "../src", "hydroframe")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__),"..", "../src", "hydroframe", "data_catalog")))

project = u"hf_hydrodata"
copyright = u"2023, Laura Condon, Reed Maxwell"
author = u"Laura Condon, Reed Maxwell, William M. Hasling, Amy M. Johnson, Amy C. Defnet, George Artavanis"

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.coverage',
    "sphinx.ext.napoleon",
    'sphinx.ext.githubpages'
]
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "sphinx_rtd_theme"
html_static_path = ['_static']

def setup(app):
    app.add_css_file("css/custom.css")