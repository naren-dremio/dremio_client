
import requests


def catalog_item(token, base_url, id=None, path=None):
    if id is None and path is None:
        raise TypeError("both id and path can't be None for a catalog_item call")

    endpoint = '/{}'.format(id) if id else '/by-path/{}'.format('/'.join(path).replace('"', ''))
    r = requests.get(base_url + "/api/v3/catalog{}".format(endpoint),
                     headers={'Authorization': '_dremio{}'.format(token), 'content-type': 'application/json'})
    r.raise_for_status()
    data = r.json()
    return data


def catalog(token, base_url):
    r = requests.get(base_url + "/api/v3/catalog", headers={'Authorization':'_dremio{}'.format(token), 'content-type':'application/json'})
    r.raise_for_status()
    data = r.json()
    return data
