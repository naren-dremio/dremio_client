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
from .auth import basic_auth
from .model.endpoints import catalog, job_results, job_status, sql, catalog_item, reflections, reflection, wlm_queues, \
    wlm_rules, votes, user, group, personal_access_token
from .util import run, run_async, refresh_metadata


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
        self._base_url = ('https' if tls else 'http') + '://' + hostname + (':{}'.format(port) if port else '')
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

    def reflections(self, summary=False):
        return reflections(self._token, self._base_url, summary)

    def reflection(self, reflectionid):
        return reflection(self._token, self._base_url, reflectionid)

    def wlm_queues(self):
        return wlm_queues(self._token, self._base_url)

    def wlm_rules(self):
        return wlm_rules(self._token, self._base_url)

    def votes(self):
        return votes(self._token, self._base_url)

    def user(self, uid=None, name=None):
        return user(self._token, self._base_url, uid, name)

    def group(self, gid=None, name=None):
        return group(self._token, self._base_url, gid, name)

    def personal_access_token(self, uid):
        return personal_access_token(self._token, self._base_url, uid)

    def query(self, query, context=None, sleep_time=10, asynchronous=False):
        """ Run a single sql query asynchronously

        This executes a single sql query against the rest api asynchronously and returns a future for the result

        :param query: valid sql query
        :param context: optional context in which to execute the query
        :param sleep_time: seconds to sleep between checking for finished state
        :param asynchronous: boolean execute asynchronously
        :raise: DremioException if job failed
        :raise: DremioUnauthorizedException if token is incorrect or invalid
        :return: concurrent.futures.Future for the result

        :example:

        >>> client =
        >>> f = run_async('abc', 'http://localhost:9047', 'select * from sys.options')
        >>> f.result()
        [{'record':'1'}, {'record':'2'}]
        """
        if asynchronous:
            return run_async(self._token, self._base_url, query, context, sleep_time)
        return run(self._token, self._base_url, query, context, sleep_time)

    def refresh_metadata(self, table):
        """ Refresh the metadata for a given physical dataset

        :param table: the physical dataset to be refreshed
        :raise: DremioException if job failed
        :raise: DremioUnauthorizedException if token is incorrect or invalid
        :return: None
        """
        return refresh_metadata(self._token, self._base_url, table)
