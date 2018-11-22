
#! /usr/bin/env python

# module level doc-string
__doc__ = """
blp_pandas - blpapi wrapped into pandas module
=====================================================================

┌────────────────┐
│    Examples    │
└────────────────┘
from blpPandas import blpPandas as bbg
from datetime import datetime

df = bbg.IntradayBar(['CAC FP Equity', 'CACX LN Equity'],
                      ['BID', 'ASK'],
                      datetime(2018,11,22,9,0),
                      datetime(2018,11,22,17,30),
                      1)

df = bbg.RefData(['CAC FP Equity', 'CACX LN Equity'],
                 'EQY_WEIGHTED_AVG_PX',
                 {'VWAP_START_TIME': '9:30','VWAP_END_TIME': '11:30'})


df = bbg.IntradayTick(['CAC FP Equity', 'CACX LN Equity'],
                      ['BID', 'ASK'],
                      datetime(2018,11,22,9,0),
                      datetime(2018,11,22,17,30))

df = bbg.HistoData(['CAC FP Equity', 'CACX LN Equity'],
                   ['PX_LAST', 'VOLUME'],
                   datetime(2018,1,1),
                   datetime(2018,12,31))

Features
------------------

> The functions accept the overrides and any other parameters in a dictionary format

"""

__author__ = "Teddy Ambona"

__copyright__ = """
Copyright 2012. Bloomberg Finance L.P.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to
deal in the Software without restriction, including without limitation the
rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
sell copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:  The above
copyright notice and this permission notice shall be included in all copies
or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
IN THE SOFTWARE.
"""