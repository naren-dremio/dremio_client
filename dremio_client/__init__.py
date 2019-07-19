# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import print_function
from __future__ import division

"""Top-level package for Dremio client."""

__author__ = """Ryan Murray"""
__email__ = 'rymurr@gmail.com'
__version__ = '0.1.0'

from .dremio_client import DremioClient
from .model.endpoints import catalog, catalog_item, sql, job_results, job_status
from .auth import basic_auth


def connect(hostname, username=None, password=None, tls=True, port=None, flight_port=47470, auth='basic'):
    """
    Create a Dremio Client instance. This currently only supports basic auth from the constructor.
    Will be extended for oauth, token auth and storing auth on disk or in stores in the future

    :param base_url: base url for Dremio coordinator
    :param username:  username
    :param password: password
    :param auth: always basic
    """
    return DremioClient(hostname, username, password, tls, port, flight_port, auth)


__all__ = ['connect', 'catalog', 'catalog_item', 'basic_auth', 'sql', 'job_status', 'job_results']

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
