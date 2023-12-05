"""
Unit test for the data_catalog.py module
"""

# pylint: disable=C0301,C0103,W0632,W0702,W0101,C0302,W0105,E0401,C0413,R0903,W0613,R0912
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../src")))

import hf_hydrodata as hf

def test_get_citations():
    """Test get_citation"""

    result = hf.get_citations(dataset="conus1_domain")
    assert "10.5194" in result
    result = hf.get_citations("conus1_domain")
    assert "10.5194" in result
    result = hf.get_citations("CW3E")
    print(result)
    #result = hf.get_citations("CW3E")
    #print(result)
