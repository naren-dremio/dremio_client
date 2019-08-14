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
import requests
from requests.exceptions import HTTPError
from ..error import DremioUnauthorizedException, DremioNotFoundException, DremioPermissionException, DremioException


def _get_headers(token):
    headers = {'Authorization': '_dremio{}'.format(
        token), 'content-type': 'application/json'}
    return headers


def _get(url, token, details=''):
    r = requests.get(url, headers=_get_headers(token))
    error, code, _ = _raise_for_status(r)
    if not error:
        data = r.json()
        return data

    if code == 401:
        raise DremioUnauthorizedException("Unauthorized on /api/v3/catalog " + details, error)
    if code == 403:
        raise DremioPermissionException("Not permissioned to view entity at " + details, error)
    if code == 404:
        raise DremioNotFoundException("No entity exists at " + details, error)
    raise DremioException('unknown error', error)


def catalog_item(token, base_url, id=None, path=None):
    """
    fetch a specific catalog item by id or by path
    https://docs.dremio.com/rest-api/catalog/get-catalog-id.html
    https://docs.dremio.com/rest-api/catalog/get-catalog-path.html
    :param token: auth token from previous login attempt
    :param base_url: base Dremio url
    :param id: unique dremio id for resource
    :param path: path (/adls/nyctaxi/filename) for a resource
    :return: json of resource
    """
    if id is None and path is None:
        raise TypeError(
            "both id and path can't be None for a catalog_item call")
    idpath = (id if id else '') + ', ' + ('.'.join(path) if path else '')
    endpoint = '/{}'.format(id) if id else '/by-path/{}'.format(
        '/'.join(path).replace('"', ''))
    return _get(base_url + "/api/v3/catalog{}".format(endpoint), token, idpath)


def catalog(token, base_url):
    """
    https://docs.dremio.com/rest-api/catalog/get-catalog.html populate the root dremio catalog
    :param token: auth token from previous login attempt
    :param base_url: base Dremio url
    :return: json of root resource
    """
    return _get(base_url + "/api/v3/catalog", token)


def sql(token, base_url, query, context=None):
    """
    submit job w/ given sql
    https://docs.dremio.com/rest-api/sql/post-sql.html
    :param token: auth token
    :param base_url: base Dremio url
    :param query: sql query
    :param context: optional dremio context
    :return: job id json object
    """
    r = requests.post(
        base_url + '/api/v3/sql',
        headers=_get_headers(token),
        json={
            'sql': query,
            'context': context})
    error, code, _ = _raise_for_status(r)
    if not error:
        data = r.json()
        return data
    if code == 401:
        raise DremioUnauthorizedException("Unauthorized on /api/v3/catalog", error)
    raise DremioException('unknown error', error)


def job_status(token, base_url, job_id):
    """
    fetch job status
    https://docs.dremio.com/rest-api/jobs/get-job.html
    :param token: auth token
    :param base_url: sql query
    :param job_id: job id (as returned by sql)
    :return: status object
    """
    return _get(base_url + '/api/v3/job/{}'.format(job_id), token)


def job_results(token, base_url, job_id, offset=0, limit=100):
    """
    fetch job results
    https://docs.dremio.com/rest-api/jobs/get-job.html

    :param token: auth token
    :param base_url: sql query
    :param job_id: job id (as returned by sql)
    :param offset: offset of result set to return
    :param limit: number of results to return (max 500)
    :return: result object
    """
    return _get(
        base_url +
        '/api/v3/job/{}/results?offset={}&limit={}'.format(
            job_id,
            offset,
            limit),
        token)


def reflections(token, base_url, summary=False):
    """
    fetch all reflections
    https://docs.dremio.com/rest-api/reflections/get-reflection.html

    :param token: auth token
    :param base_url: sql query
    :param summary: fetch only the reflection summary
    :return: result object
    """
    return _get(base_url + "/api/v3/reflection" + ('/summary' if summary else ''), token)


def reflection(token, base_url, reflectionid):
    """
    fetch a single reflection by id
    https://docs.dremio.com/rest-api/reflections/get-reflection.html

    :param token: auth token
    :param base_url: sql query
    :param reflectionid: id of the reflection to fetch
    :return: result object
    """
    return _get(base_url + "/api/v3/reflection/{}".format(reflectionid), token)


def wlm_queues(token, base_url):
    """
    fetch all wlm queues
    https://docs.dremio.com/rest-api/reflections/get-wlm-queue.html

    :param token: auth token
    :param base_url: sql query
    :return: result object
    """
    return _get(base_url + "/api/v3/wlm/queue", token)


def wlm_rules(token, base_url):
    """
    fetch all wlm rules
    https://docs.dremio.com/rest-api/reflections/get-wlm-queue.html

    :param token: auth token
    :param base_url: sql query
    :return: result object
    """
    return _get(base_url + "/api/v3/wlm/rule", token)


def votes(token, base_url):
    """
    fetch all votes
    https://docs.dremio.com/rest-api/reflections/get-vote.html

    :param token: auth token
    :param base_url: sql query
    :return: result object
    """
    return _get(base_url + "/api/v3/vote", token)


def user(token, base_url, uid=None, name=None):
    """
    fetch all votes
    https://docs.dremio.com/rest-api/reflections/get-user.html

    :param token: auth token
    :param base_url: sql query
    :param uid: unique dremio id for user
    :param name: name for a user
    :return: result object
    """
    if uid is None and name is None:
        raise TypeError(
            "both id and name can't be None for a user call")
    idpath = (uid if uid else '') + ', ' + ('.'.join(name) if name else '')
    endpoint = '/{}'.format(uid) if uid else '/by-name/{}'.format(
        '/'.join(name).replace('"', ''))
    return _get(base_url + "/api/v3/user{}".format(endpoint), token, idpath)


def group(token, base_url, gid=None, name=None):
    """
    fetch a group based on id or name
    https://docs.dremio.com/rest-api/reflections/get-group.html

    :param token: auth token
    :param base_url: sql query
    :param gid: unique dremio id for group
    :param name: name for a group
    :return: result object
    """
    if gid is None and name is None:
        raise TypeError(
            "both id and name can't be None for a user call")
    idpath = (gid if gid else '') + ', ' + ('.'.join(name) if name else '')
    endpoint = '/{}'.format(gid) if gid else '/by-name/{}'.format(
        '/'.join(name).replace('"', ''))
    return _get(base_url + "/api/v3/group{}".format(endpoint), token, idpath)


def personal_access_token(token, base_url, uid):
    """
    fetch a PAT for a user based on id
    https://docs.dremio.com/rest-api/user/get-user-id-token.html

    :param token: auth token
    :param base_url: sql query
    :param uid: id of a user
    :return: result object
    """
    return _get(base_url + "/api/v3/user/{}/token".format(uid), token)


def _raise_for_status(self):
    """Raises stored :class:`HTTPError`, if one occurred. Copy from requests request.raise_for_status()"""

    http_error_msg = ''
    if isinstance(self.reason, bytes):
        try:
            reason = self.reason.decode('utf-8')
        except UnicodeDecodeError:
            reason = self.reason.decode('iso-8859-1')
    else:
        reason = self.reason

    if 400 <= self.status_code < 500:
        http_error_msg = u'%s Client Error: %s for url: %s' % (self.status_code, reason, self.url)

    elif 500 <= self.status_code < 600:
        http_error_msg = u'%s Server Error: %s for url: %s' % (self.status_code, reason, self.url)

    if http_error_msg:
        return HTTPError(http_error_msg, response=self), self.status_code, reason
    else:
        return None, self.status_code, reason
