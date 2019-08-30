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
from .error import DremioNotFoundException
from .model.endpoints import sql as _sql
from .model.endpoints import job_status as _job_status
from .model.endpoints import job_results as _job_results
from .model.endpoints import catalog as _catalog
from .model.endpoints import catalog_item as _catalog_item
from .model.endpoints import reflections as _reflections
from .model.endpoints import reflection as _reflection
from .model.endpoints import wlm_rules as _wlm_rules
from .model.endpoints import wlm_queues as _wlm_queues
from .model.endpoints import votes as _votes
from .model.endpoints import group as _group
from .model.endpoints import user as _user
from .model.endpoints import personal_access_token as _pat
from .model.endpoints import collaboration_wiki as _collaboration_wiki
from .model.endpoints import collaboration_tags as _collaboration_tags


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
@click.option('-i', '--id', help="Path of a given catalog item")
@click.option('-p', '--path', help="id of a given catalog item")
@click.pass_obj
def catalog_item(args, cid, path):
    """
    return the details of a given catalog item

    if cid and path are both specified id is used
    if neither are specified it causes an error

    """
    base_url, token = get_base_url_token(args)
    x = _catalog_item(token, base_url, cid, [path.replace('.', '/')] if path else None)
    click.echo(json.dumps(x))


@cli.command()
@click.option('--summary', '-s', is_flag=True, help='only return summary reflection info')
@click.pass_obj
def reflections(args, summary):
    """
    return the reflection set

    """
    base_url, token = get_base_url_token(args)
    x = _reflections(token, base_url, summary)
    click.echo(json.dumps(x))


@cli.command()
@click.argument('reflectionid', nargs=1, required=True)
@click.pass_obj
def reflection(args, reflectionid):
    """
    return the reflection set

    """
    base_url, token = get_base_url_token(args)
    x = _reflection(token, base_url, reflectionid)
    click.echo(json.dumps(x))


@cli.command()
@click.pass_obj
def wlm_rules(args):
    """
    return the list of wlm rules

    """
    base_url, token = get_base_url_token(args)
    x = _wlm_rules(token, base_url)
    click.echo(json.dumps(x))


@cli.command()
@click.pass_obj
def wlm_queues(args):
    """
    return the list of wlm queues

    """
    base_url, token = get_base_url_token(args)
    x = _wlm_queues(token, base_url)
    click.echo(json.dumps(x))


@cli.command()
@click.pass_obj
def votes(args):
    """
    return reflection votes

    """
    base_url, token = get_base_url_token(args)
    x = _votes(token, base_url)
    click.echo(json.dumps(x))


@cli.command()
@click.option('--uid', '-u', help='unique id for a user')
@click.option('--name', '-n', help='human readable name of a user')
@click.pass_obj
def user(args, gid, name):
    """
    return user info

    """
    base_url, token = get_base_url_token(args)
    x = _user(token, base_url, gid, name)
    click.echo(json.dumps(x))


@cli.command()
@click.option('--gid', '-g', help='unique id for a group')
@click.option('--name', '-n', help='human readable name of a group')
@click.pass_obj
def group(args, gid, name):
    """
    return group info

    """
    base_url, token = get_base_url_token(args)
    x = _group(token, base_url, gid, name)
    click.echo(json.dumps(x))


@cli.command()
@click.argument('uid', nargs=1, required=True)
@click.pass_obj
def pat(args, uid):
    """
    return personal access token info for a given user id

    """
    base_url, token = get_base_url_token(args)
    x = _pat(token, base_url, uid)
    click.echo(json.dumps(x))


@cli.command()
@click.option('--cid', '-c', help='unique id for a catalog entity')
@click.option('--path', '-p', help='path of a catalog entity')
@click.pass_obj
def tags(args, cid, path):
    """
    returns tags for a given catalog entity id or path
    only cid or path can be specified. path incurs a second lookup to get the id

    """
    base_url, token = get_base_url_token(args)
    if path:
        res = _catalog_item(token, base_url, None, [path.replace('.', '/')])
        cid = res['id']
    try:
        x = _collaboration_tags(token, base_url, cid)
        click.echo(json.dumps(x))
    except DremioNotFoundException:
        click.echo("Wiki not found or entity does not exist")


@cli.command()
@click.option('--cid', '-c', help='unique id for a catalog entity')
@click.option('--path', '-p', help='path of a catalog entity')
@click.option('--pretty-print', '-v', is_flag=True, help='format markdown for terminal')
@click.pass_obj
def wiki(args, cid, path, pretty_print):
    """
    returns wiki for a given catalog entity id or path
    only cid or path can be specified. path incurs a second lookup to get the id

    activating the pretty-print flag will attempt to convert the text field to plain-text for the console

    """
    base_url, token = get_base_url_token(args)
    if path:
        res = _catalog_item(token, base_url, None, [path.replace('.', '/')])
        cid = res['id']
    try:
        x = _collaboration_wiki(token, base_url, cid)
        if pretty_print:
            try:
                text = _to_text(x['text'])
                click.echo(text)
            except ImportError:
                click.echo("Can't convert text to console, please install markdown and BeautifulSoup")
                click.echo(json.dumps(x))
        else:
            click.echo(json.dumps(x))
    except DremioNotFoundException:
        click.echo("Wiki not found or entity does not exist")


def _to_text(text):
    from markdown import Markdown
    from io import StringIO

    def unmark_element(element, stream=None):
        if stream is None:
            stream = StringIO()
        if element.text:
            stream.write(element.text)
        for sub in element:
            unmark_element(sub, stream)
        if element.tail:
            stream.write(element.tail)
        return stream.getvalue()

    # patching Markdown
    Markdown.output_formats["plain"] = unmark_element
    __md = Markdown(output_format="plain")
    __md.stripTopLevelTags = False

    return __md.convert(text)


if __name__ == "__main__":
    sys.exit(cli())  # pragma: no cover
