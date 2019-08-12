from .data import Root
from .endpoints import catalog as _catalog


def catalog(token, base_url, flight_endpoint):
    cat = Root(token, base_url, flight_endpoint)
    data = _catalog(token, base_url)
    for item in data['data']:
        cat.add(item)
    return cat
