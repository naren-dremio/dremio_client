#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import print_function
from __future__ import division


import json
from click.testing import CliRunner

from dremio_client.auth import basic_auth
from dremio_client.model.catalog import catalog
from dremio_client import cli


def test_command_line_interface(requests_mock):
    """Test the CLI."""
    requests_mock.post('http://localhost:9047/apiv2/login', text=json.dumps({'token': '12345'}))
    with open('tests/data/sql.json', 'r+') as f:
        txt = json.dumps(json.load(f))
    requests_mock.post('http://localhost:9047/api/v3/sql', text=txt)
    with open('tests/data/job_status.json', 'r+') as f:
        txt = json.dumps(json.load(f))
    requests_mock.get('http://localhost:9047/api/v3/job/22b3b4fe-669a-4789-a9de-b1fc5ba7b500', text=txt)
    with open('tests/data/job_results.json', 'r+') as f:
        txt = json.dumps(json.load(f))
    requests_mock.get('http://localhost:9047/api/v3/job/22b3b4fe-669a-4789-a9de-b1fc5ba7b500/results', text=txt)
    runner = CliRunner()
    result = runner.invoke(cli.query, ['--sql', 'select * from sys.options'], obj={}, catch_exceptions=False)
    assert result.exit_code == 0
    assert "'rowCount': 100" in result.output
    help_result = runner.invoke(cli.query, ['--help'])
    assert help_result.exit_code == 0
    assert 'execute a query given by sql and print results' in help_result.output


def test_auth(requests_mock):
    requests_mock.post('https://example.com/apiv2/login', text=json.dumps({'token': '12345'}))
    token = basic_auth('https://example.com', 'foo', 'bar')
    assert token == '12345'


def test_catalog(requests_mock):
    with open('tests/data/catalog.json', 'r+') as f:
        txt = json.dumps(json.load(f))
    requests_mock.get('https://example.com/api/v3/catalog', text=txt)
    with open('tests/data/adls.json', 'r+') as f:
        txt = json.dumps(json.load(f))
    requests_mock.get('https://example.com/api/v3/catalog/1a2b82e3-08fc-43f7-a426-76bee4abaaef', text=txt)
    with open('tests/data/nyctaxi.json', 'r+') as f:
        txt = json.dumps(json.load(f))
    requests_mock.get('https://example.com/api/v3/catalog/by-path/adls/nyctaxi', text=txt)

    token = '12345'
    c = catalog(token, 'https://example.com', print)
    assert 'adls' in dir(c)
    assert 'nyctaxi' in dir(c.adls)
    assert 'yellow_tripdata_2009_01_csv' in dir(c.adls.nyctaxi)


def test_dataset(requests_mock):
    with open('tests/data/catalog.json', 'r+') as f:
        txt = json.dumps(json.load(f))
    requests_mock.get('https://example.com/api/v3/catalog', text=txt)
    with open('tests/data/adls.json', 'r+') as f:
        txt = json.dumps(json.load(f))
    requests_mock.get('https://example.com/api/v3/catalog/1a2b82e3-08fc-43f7-a426-76bee4abaaef', text=txt)
    with open('tests/data/profiles.json', 'r+') as f:
        txt = json.dumps(json.load(f))
    requests_mock.get('https://example.com/api/v3/catalog/by-path/adls/profiles', text=txt)

    token = '12345'
    c = catalog(token, 'https://example.com', lambda x: x)
    sql = c.adls.profiles.sql
    assert sql('hello') == 'hello'
