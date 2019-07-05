# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import print_function
from __future__ import division

"""Top-level package for Dremio client."""

__author__ = """Ryan Murray"""
__email__ = 'rymurr@dremio.com'
__version__ = '0.1.0'


# https://github.com/ipython/ipython/issues/11653
# autocomplete doesn't work when using jedi so turn it off!
try:
    __IPYTHON__
except NameError:
    pass
else:
    from IPython import __version__

    major = int(__version__.split('.')[0])
    if major >= 6:
        from IPython import get_ipython
        get_ipython().Completer.use_jedi = False
