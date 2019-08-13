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
from requests.exceptions import HTTPError

from .auth import basic_auth
from .error import DremioException
from .model.endpoints import catalog, job_results, job_status, sql, catalog_item


class SimpleClient(object):

    def __init__(self, hostname, username=None, password=None,
                 tls=True, port=None, flight_port=47470, auth='basic'):
        """
        Create a Dremio Simple Client instance. This currently only supports basic auth from the constructor.
        Will be extended for oauth, token auth and storing auth on disk or in stores in the future

        :param base_url: base url for Dremio coordinator
        :param username:  username
        :param password: password
        :param auth: always basic
        """
        self._hostname = hostname
        self._flight_port = flight_port
        self._base_url = ('https' if tls else 'http') + '://' + \
                         hostname + (':{}'.format(port) if port else '')
        self._username = username
        self._password = password
        self._token = basic_auth(self._base_url, username, password)

    def catalog(self):
        return catalog(self._token, self._base_url)

    def job_status(self, jobid):
        return job_status(self._token, self._base_url, jobid)

    def catalog_item(self, id, path):
        return catalog_item(self._token, self._base_url, id, path)

    def job_results(self, jobid):
        return job_results(self._token, self._base_url, jobid)

    def sql(self, query, context=None):
        return sql(self._token, self._base_url, query, context)
