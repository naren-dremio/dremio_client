#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `dremio_client` package."""


import pytest
import json
from click.testing import CliRunner

from dremio_client.auth import basic_auth
from dremio_client.model.catalog import catalog
from dremio_client import cli


def test_command_line_interface():
    """Test the CLI."""
    runner = CliRunner()
    result = runner.invoke(cli.main)
    assert result.exit_code == 0
    assert 'dremio_client.cli.main' in result.output
    help_result = runner.invoke(cli.main, ['--help'])
    assert help_result.exit_code == 0
    assert '--help  Show this message and exit.' in help_result.output


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
    c = catalog(token, 'https://example.com', lambda x: print(x))
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
