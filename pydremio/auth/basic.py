import requests

def login(base_url, username, password, timeout=10):
    url = base_url + '/apiv2/login'

    r = requests.post(url,json={"userName": username, "password": password}, timeout=timeout)
    r.raise_for_status()
    return r.json()['token']
