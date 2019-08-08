#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import print_function
from __future__ import division

import os
from dremio_client.conf import build_config


def test_config():
    config = build_config(None)
    assert config['auth']['username'].get() == 'dremio'
    assert config['auth']['password'].get() == 'dremio123'
    assert config['auth']['type'].get() == 'basic'
    assert config['hostname'].get() == 'localhost'
    assert config['port'].get(int) == 9047
    assert config['ssl'].get(bool) is False


def test_config_with_override():
    config = build_config({'hostname': 'dremio.org', 'ssl': True})
    assert config['hostname'].get() == 'dremio.org'
    assert config['ssl'].get(bool) is True
    assert config['auth']['password'].get() == 'dremio123'


def test_config_with_env_override():
    os.environ['DREMIO_AUTH_USERNAME'] = 'furby'
    config = build_config()
    assert config['auth']['username'].get() == 'furby'
    assert config['auth']['password'].get() == 'dremio123'
