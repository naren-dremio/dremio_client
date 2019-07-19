from .data import Root
from .endpoints import catalog as _catalog


def catalog(token, base_url, flight_endpoint):
    cat = Root(token, base_url, flight_endpoint)
    data = _catalog(token, base_url)
    [cat.add(item) for item in data['data']]
    return cat
