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
from .auth import auth
from .model.endpoints import catalog, job_results, job_status, sql, catalog_item, reflections, reflection, wlm_queues, \
    wlm_rules, votes, user, group, personal_access_token, collaboration_tags, collaboration_wiki
from .util import run, run_async, refresh_metadata


class SimpleClient(object):

    def __init__(self, config):
        """
        Create a Dremio Simple Client instance. This currently only supports basic auth from the constructor.
        Will be extended for oauth, token auth and storing auth on disk or in stores in the future

        :param config: config dict from confuse
        """

        port = config['port'].get(int)
        self._hostname = config['hostname'].get()
        self._base_url = ('https' if config['ssl'].get(bool) else 'http') + '://' + self._hostname + (
            ':{}'.format(port) if port else '')
        self._token = auth(self._base_url, config)
        self._ssl_verify = config['verify'].get(bool)

    def catalog(self):
        return catalog(self._token, self._base_url, ssl_verify=self._ssl_verify)

    def job_status(self, jobid):
        return job_status(self._token, self._base_url, jobid, ssl_verify=self._ssl_verify)

    def catalog_item(self, id, path):
        return catalog_item(self._token, self._base_url, id, path, ssl_verify=self._ssl_verify)

    def job_results(self, jobid):
        return job_results(self._token, self._base_url, jobid, ssl_verify=self._ssl_verify)

    def sql(self, query, context=None):
        return sql(self._token, self._base_url, query, context, ssl_verify=self._ssl_verify)

    def reflections(self, summary=False):
        return reflections(self._token, self._base_url, summary, ssl_verify=self._ssl_verify)

    def reflection(self, reflectionid):
        return reflection(self._token, self._base_url, reflectionid, ssl_verify=self._ssl_verify)

    def wlm_queues(self):
        """ return details all workload management queues

        Summarizes all workload management queues in the system
        https://docs.dremio.com/rest-api/wlm/get-wlm-queue.html
        .. note:: can only be run by admin
        .. note:: Enterprise only

        :raise: DremioUnauthorizedException if token is incorrect or invalid
        :raise: DremioPermissionException user does not have permission
        :raise: DremioNotFoundException queues not found
        :return: queues as a list of dicts
        """
        return wlm_queues(self._token, self._base_url, ssl_verify=self._ssl_verify)

    def wlm_rules(self):
        """ return details all workload management rules

        Summarizes all workload management rules in the system
        https://docs.dremio.com/rest-api/wlm/get-wlm-rule.html
        .. note:: can only be run by admin
        .. note:: Enterprise only

        :raise: DremioUnauthorizedException if token is incorrect or invalid
        :raise: DremioPermissionException user does not have permission
        :raise: DremioNotFoundException ruleset is not found
        :return: rules as a list of dicts
        """
        return wlm_rules(self._token, self._base_url, ssl_verify=self._ssl_verify)

    def votes(self):
        """ return details all reflection votes

        Summarizes all votes in the system
        https://docs.dremio.com/rest-api/votes/get-vote.html
        .. note:: can only be run by admin

        :raise: DremioUnauthorizedException if token is incorrect or invalid
        :raise: DremioPermissionException user does not have permission
        :return: votes as a list of dicts
        """
        return votes(self._token, self._base_url, ssl_verify=self._ssl_verify)

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
        return user(self._token, self._base_url, uid, name, ssl_verify=self._ssl_verify)

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
        return group(self._token, self._base_url, gid, name, ssl_verify=self._ssl_verify)

    def personal_access_token(self, uid):
        """ return a list of personal access tokens for a user

        .. note:: can only be run for the logged in user

        :param uid: user id
        :raise: DremioUnauthorizedException if token is incorrect or invalid
        :raise: DremioPermissionException user does not have permission
        :raise: DremioNotFoundException user could not be found
        :return: personal access token list
        """
        return personal_access_token(self._token, self._base_url, uid, ssl_verify=self._ssl_verify)

    def collaboration_tag(self, cid):
        """ returns a list of tags for catalog entity

        :param cid: catalog entity id
        :raise: DremioBadRequestException if tags can't exist on this entity
        :raise: DremioUnauthorizedException if token is incorrect or invalid
        :raise: DremioPermissionException user does not have permission
        :raise: DremioNotFoundException user could not be found
        :return: list of tags
        """
        return collaboration_tags(self._token, self._base_url, cid, ssl_verify=self._ssl_verify)

    def collaboration_wiki(self, cid):
        """ returns a wiki details for catalog entity


        :param cid: catalog entity id
        :raise: DremioUnauthorizedException if token is incorrect or invalid
        :raise: DremioPermissionException user does not have permission
        :raise: DremioNotFoundException user could not be found
        :return: wiki details
        """
        return collaboration_wiki(self._token, self._base_url, cid, ssl_verify=self._ssl_verify)

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
        >>> f = query('select * from sys.options', asynchronous=True)
        >>> f.result()
        [{'record':'1'}, {'record':'2'}]
        """
        if asynchronous:
            return run_async(self._token, self._base_url, query, context, sleep_time, ssl_verify=self._ssl_verify)
        return run(self._token, self._base_url, query, context, sleep_time, ssl_verify=self._ssl_verify)

    def refresh_metadata(self, table):
        """ Refresh the metadata for a given physical dataset

        :param table: the physical dataset to be refreshed
        :raise: DremioException if job failed
        :raise: DremioUnauthorizedException if token is incorrect or invalid
        :return: None
        """
        return refresh_metadata(self._token, self._base_url, table, ssl_verify=self._ssl_verify)
