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

"""Main module."""
import logging

from .auth import basic_auth
from .model.catalog import catalog
from .model.endpoints import reflections, wlm_queues, wlm_rules, votes, user, group, personal_access_token, \
    collaboration_wiki, collaboration_tags
from .model.data import make_reflection, make_wlm_queue, make_wlm_rule, make_vote
from .flight import query as _flight_query
from .util import run as _rest_query
from .odbc import query as _odbc_query


class DremioClient(object):

    def __init__(self, hostname, username=None, password=None,
                 tls=True, port=None, flight_port=47470, odbc_port=31010, auth='basic'):
        """
        Create a Dremio Client instance. This currently only supports basic auth from the constructor.
        Will be extended for oauth, token auth and storing auth on disk or in stores in the future

        :param base_url: base url for Dremio coordinator
        :param username:  username
        :param password: password
        :param auth: always basic
        """
        self._hostname = hostname
        self._flight_port = flight_port
        self._odbc_port = odbc_port
        self._base_url = ('https' if tls else 'http') + '://' + \
                         hostname + (':{}'.format(port) if port else '')
        self._username = username
        self._password = password
        self._token = basic_auth(self._base_url, username, password)
        self._catalog = catalog(self._token, self._base_url, self.query)
        self._reflections = list()
        self._wlm_queues = list()
        self._wlm_rules = list()
        self._votes = list()

    @property
    def data(self):
        return self._catalog

    @property
    def reflections(self):
        if len(self._reflections) == 0:
            self._fetch_reflections()
        return self._reflections

    def _fetch_reflections(self):
        refs = reflections(self._token, self._base_url)
        for ref in refs['data']:  # todo I think we should attach reflections to their catalog entries...
            self._reflections.append(make_reflection(ref))

    @property
    def wlm_queues(self):
        if len(self._wlm_queues) == 0:
            self._fetch_wlm_queues()
        return self._wlm_queues

    def _fetch_wlm_queues(self):
        refs = wlm_queues(self._token, self._base_url)
        for ref in refs['data']:  # todo I think we should attach reflections to their catalog entries...
            self._wlm_queues.append(make_wlm_queue(ref))

    @property
    def wlm_rules(self):
        if len(self._wlm_rules) == 0:
            self._fetch_wlm_rules()
        return self._wlm_rules

    def _fetch_wlm_rules(self):
        refs = wlm_rules(self._token, self._base_url)
        for ref in refs['rules']:  # todo I think we should attach reflections to their catalog entries...
            self._wlm_rules.append(make_wlm_rule(ref))

    @property
    def votes(self):
        if len(self._votes) == 0:
            self._fetch_votes()
        return self._votes

    def _fetch_votes(self):
        refs = votes(self._token, self._base_url)
        for ref in refs['data']:  # todo I think we should attach reflections to their catalog entries...
            self._votes.append(make_vote(ref))

    def query(self, sql, pandas=True, method='flight'):
        failed = False
        if method == 'flight':
            try:
                return _flight_query(sql,
                                     hostname=self._hostname,
                                     port=self._flight_port,
                                     username=self._username,
                                     password=self._password,
                                     pandas=pandas)
            except NotImplementedError:
                logging.warning("Unable to run query as flight, downgrading to odbc")
                failed = True
        if method == 'odbc' or failed:
            try:
                return _odbc_query(sql,
                                   hostname=self._hostname,
                                   port=self._odbc_port,
                                   username=self._username,
                                   password=self._password)
            except NotImplementedError:
                logging.warning("Unable to run query as odbc, downgrading to rest")
        results = _rest_query(self._token, self._base_url, sql)
        if pandas:
            return pandas.DataFrame(results)
        return results

    def user(self, uid=None, name=None):
        """ return details for a user

        User must supply one of uid or name. uid takes precedence if both are supplied
        .. note:: can only be run by admin

        :param uid: group id
        :param name: group name
        :raise: DremioUnauthorizedException if token is incorrect or invalid
        :raise: DremioPermissionException user does not have permission
        :raise: DremioNotFoundException user could not be found
        :return: user info as a dict
        """
        return user(self._token, self._base_url, uid, name)

    def group(self, gid=None, name=None):
        """ return details for a group

        User must supply one of gid or name. gid takes precedence if both are supplied
        .. note:: can only be run by admin

        :param gid: group id
        :param name: group name
        :raise: DremioUnauthorizedException if token is incorrect or invalid
        :raise: DremioPermissionException user does not have permission
        :raise: DremioNotFoundException group could not be found
        :return: group info as a dict
        """
        return group(self._token, self._base_url, gid, name)

    def personal_access_token(self, uid):
        """ return a list of personal access tokens for a user

        .. note:: can only be run for the logged in user

        :param uid: user id
        :raise: DremioUnauthorizedException if token is incorrect or invalid
        :raise: DremioPermissionException user does not have permission
        :raise: DremioNotFoundException user could not be found
        :return: personal access token list
        """
        return personal_access_token(self._token, self._base_url, uid)
