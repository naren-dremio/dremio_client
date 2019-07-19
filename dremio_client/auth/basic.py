import requests


def login(base_url, username, password, timeout=10):
    """
    Log into dremio using basic auth
    :param base_url: Dremio url
    :param username: username
    :param password: password
    :param timeout: optional timeout
    :return: auth token
    """
    url = base_url + '/apiv2/login'

    r = requests.post(url,json={"userName": username, "password": password}, timeout=timeout)
    r.raise_for_status()
    return r.json()['token']
