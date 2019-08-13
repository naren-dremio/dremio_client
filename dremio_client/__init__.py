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
__version__ = '0.2.4'

import os
from .dremio_client import DremioClient
from .conf import build_config
from .model.endpoints import catalog, catalog_item, sql, job_results, job_status


def init(config_dir=None):
    if config_dir:
        os.environ['DREMIO_CLIENTDIR'] = config_dir
    config = build_config()
    return connect(config['hostname'].get(),
                   config['auth']['username'].get(),
                   config['auth']['password'].get(),
                   config['ssl'].get(bool),
                   config['port'].get(int))


def connect(hostname, username=None, password=None, tls=True,
            port=None, flight_port=47470, auth='basic'):
    """
    Create a Dremio Client instance. This currently only supports basic auth from the constructor.
    Will be extended for oauth, token auth and storing auth on disk or in stores in the future

    :param base_url: base url for Dremio coordinator
    :param username:  username
    :param password: password
    :param auth: always basic
    """
    return DremioClient(hostname, username, password,
                        tls, port, flight_port, auth)


__all__ = ['init', 'connect', 'catalog', 'catalog_item', 'sql', 'job_status', 'job_results']

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
