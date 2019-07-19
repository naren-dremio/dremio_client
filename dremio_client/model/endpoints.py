import requests


def _get_headers(token):
    headers = {'Authorization': '_dremio{}'.format(token), 'content-type': 'application/json'}
    return headers


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
        raise TypeError("both id and path can't be None for a catalog_item call")

    endpoint = '/{}'.format(id) if id else '/by-path/{}'.format('/'.join(path).replace('"', ''))
    r = requests.get(base_url + "/api/v3/catalog{}".format(endpoint), headers=_get_headers(token))
    r.raise_for_status()
    data = r.json()
    return data


def catalog(token, base_url):
    """
    https://docs.dremio.com/rest-api/catalog/get-catalog.html populate the root dremio catalog
    :param token: auth token from previous login attempt
    :param base_url: base Dremio url
    :return: json of root resource
    """
    r = requests.get(base_url + "/api/v3/catalog", headers=_get_headers(token))
    r.raise_for_status()
    data = r.json()
    return data


def sql(token, base_url, query, context=None):
    """
    submit job w/ given sql
    https://docs.dremio.com/rest-api/sql/post-sql.html
    :param token: auth token
    :param query: sql query
    :param context: optional dremio context
    :return: job id json object
    """
    r = requests.post(base_url + '/api/v3/sql', headers=_get_headers(token), json={
        'sql': query,
        'context': context
    })
    r.raise_for_status()
    data = r.json()
    return data


def job_status(token, base_url, job_id):
    """
    fetch job status
    https://docs.dremio.com/rest-api/jobs/get-job.html
    :param token: auth token
    :param base_url: sql query
    :param job_id: job id (as returned by sql)
    :return: status object
    """
    r = requests.get(base_url + '/api/v3/job/{}'.format(job_id), headers=_get_headers(token))
    r.raise_for_status()
    data = r.json()
    return data


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
    r = requests.get(base_url + '/api/v3/job/{}/results?offset={}&limit={}'.format(job_id, offset, limit),
                      headers=_get_headers(token))
    r.raise_for_status()
    data = r.json()
    return data
