# -*- coding: utf-8 -*-
#
# Copyright (c) 2019 Ryan Murray.
#
# This file is part of Dremio Client
# (see https://github.com/rymurr/dremio_client).
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
from __future__ import absolute_import
from __future__ import print_function
from __future__ import division


__author__ = """Ryan Murray"""
__email__ = 'rymurr@gmail.com'
__version__ = '0.4.3'

import os
from .dremio_client import DremioClient
from .dremio_simple_client import SimpleClient
from .conf import build_config
from .model.endpoints import catalog, catalog_item, sql, job_results, job_status


def get_config(config_dir=None):
    if config_dir:
        os.environ['DREMIO_CLIENTDIR'] = config_dir
    return build_config()


def init(config_dir=None, simple_client=False):
    """ create a new Dremio client object

    This returns a rich client by default. This client abstracts the Dremio catalog into a
    a set of Python objects and enables *<Tab>* completion where possible. It also supports
    fetching datasets via flight or odbc

    The simple client simply wraps the Dremio REST endpoints an returns ``dict`` objects


    :param config_dir: optional directory to look for config in
    :param simple_client: return the 'simple' client.
    :return: either a simple or rich client

    :example:

    >>> client = init('/my/config/dir')
    """
    config = get_config(config_dir)
    return _connect(config, simple_client)


def _connect(config_dir, simple=False):
    if simple:
        return SimpleClient(config_dir)
    return DremioClient(config_dir)


__all__ = ['init', 'catalog', 'catalog_item', 'sql', 'job_status', 'job_results']

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
