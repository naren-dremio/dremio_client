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

"""Console script for dremio_client."""
import sys
import os
import click
import simplejson as json

from .conf import get_base_url_token
from .util.query import run
from .model.endpoints import sql as _sql
from .model.endpoints import job_status as _job_status
from .model.endpoints import job_results as _job_results
from .model.endpoints import catalog as _catalog
from .model.endpoints import catalog_item as _catalog_item


@click.group()
@click.option('--config', type=click.Path(exists=True, dir_okay=True, file_okay=False),
              help='Custom config file.')
@click.option('-h', '--hostname', help='Hostname if different from config file')
@click.option('-p', '--port', type=int, help='Hostname if different from config file')
@click.option('--ssl', is_flag=True, help='Use SSL if different from config file')
@click.option('-u', '--username', help='username if different from config file')
@click.option('-p', '--password', help='password if different from config file')
@click.pass_context
def cli(ctx, config, hostname, port, ssl, username, password):
    if config:
        os.environ['DREMIO_CLIENTDIR'] = config
    ctx.obj = dict()
    if hostname:
        ctx.obj['hostname'] = hostname
    if port:
        ctx.obj['port'] = port
    if ssl:
        ctx.obj['ssl'] = ssl
    if username:
        ctx.obj['auth.username'] = username
    if password:
        ctx.obj['auth.password'] = password


@cli.command()
@click.option('--sql', help='sql query to execute.', required=True)
@click.pass_obj
def query(args, sql):
    """
    execute a query given by sql and print results
    """
    base_url, token = get_base_url_token(args)
    results = list()
    for x in run(token, base_url, sql):
        results.extend(x['rows'])
    click.echo(json.dumps(results))


@cli.command()
@click.argument('sql-query', nargs=-1, required=True)
@click.option('--context', help='context in which the sql query should execute.')
@click.pass_obj
def sql(args, sql_query, context):
    """
    Execute sql statement and return job id

    """
    base_url, token = get_base_url_token(args)
    x = _sql(token, base_url, ' '.join(sql_query), context)
    click.echo(json.dumps(x))


@cli.command()
@click.argument('jobid', nargs=1, required=True)
@click.pass_obj
def job_status(args, jobid):
    """
    Return status of job for a given job id

    """
    base_url, token = get_base_url_token(args)
    x = _job_status(token, base_url, jobid)
    click.echo(json.dumps(x))


@cli.command()
@click.argument('jobid', nargs=1, required=True)
@click.option('-o', '--offset', type=int, default=0, help="offset of first result")
@click.option('-l', '--limit', type=int, default=100, help="number of results to return")
@click.pass_obj
def job_results(args, jobid, offset, limit):
    """
    return results for a given job id

    pagenated with offset and limit

    """
    base_url, token = get_base_url_token(args)
    x = _job_results(token, base_url, jobid, offset, limit)
    click.echo(json.dumps(x))


@cli.command()
@click.pass_obj
def catalog(args):
    """
    return the root catalog

    """
    base_url, token = get_base_url_token(args)
    x = _catalog(token, base_url)
    click.echo(json.dumps(x))


@cli.command()
@click.option('-i', '--path', help="Path of a given catalog item")
@click.option('-p', '--id', help="id of a given catalog item")
@click.pass_obj
def catalog_item(args):
    """
    return the details of a given catalog item

    if id and path are both specified id is used
    if neither are specified it causes an error

    """
    base_url, token = get_base_url_token(args)
    x = _catalog_item(token, base_url)
    click.echo(json.dumps(x))


if __name__ == "__main__":
    sys.exit(cli())  # pragma: no cover
