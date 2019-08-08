# -*- coding: utf-8 -*-

"""Console script for dremio_client."""
import sys
import os
import click

from .conf import get_base_url_token
from .util.query import run
from .model.endpoints import sql as _sql
from .model.endpoints import job_status as _job_status
from .model.endpoints import job_results as _job_results
from .model.endpoints import catalog as _catalog
from .model.endpoints import catalog_item as _catalog_item


@click.group()
@click.option('--config', type=click.Path(exists=True, dir_okay=False),
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
    ctx.obj = {'hostname': hostname,
               'port': port,
               'ssl': ssl,
               'auth.username': username,
               'auth.password': password}


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
        results.append(x)
    click.echo(x)


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
    click.echo(x)


@cli.command()
@click.argument('jobid', nargs=1, required=True)
@click.pass_obj
def job_status(args, jobid):
    """
    Return status of job for a given job id

    """
    base_url, token = get_base_url_token(args)
    x = _job_status(token, base_url, jobid)
    click.echo(x)


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
    click.echo(x)


@cli.command()
@click.pass_obj
def catalog(args):
    """
    return the root catalog

    """
    base_url, token = get_base_url_token(args)
    x = _catalog(token, base_url)
    click.echo(x)


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
    click.echo(x)


if __name__ == "__main__":
    sys.exit(cli())  # pragma: no cover
