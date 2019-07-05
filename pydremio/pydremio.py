# -*- coding: utf-8 -*-

"""Main module."""

from .auth import basic_auth
from .model.catalog import catalog
from .flight import query


class DremioClient(object):

    def __init__(self, hostname, username=None, password=None, tls=True, port=None, flight_port=47470, auth='basic'):
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
        self._base_url = ('https' if tls else 'http') + '://' + hostname + (':{}'.format(port) if port else '')
        self._username = username
        self._password = password
        self._token = basic_auth(self._base_url, username, password)
        self._catalog = catalog(self._token, self._base_url, lambda sql: self.query(sql))

    @property
    def data(self):
        return self._catalog

    def query(self, sql, pandas=True):
        return query(sql,
                     hostname=self._hostname,
                     port=self._flight_port,
                     username=self._username,
                     password=self._password,
                     pandas=pandas)

