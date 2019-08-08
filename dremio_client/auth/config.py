from .basic import login as _login


def login(base_url, config_dict, timeout=10):
    """
    Log into dremio using basic auth and looking for config
    :param base_url: Dremio url
    :param config_dict: config dict
    :param timeout: optional timeout
    :return: auth token
    """
    username = config_dict['auth']['username'].get()
    if not username:
        raise RuntimeError("No username available, can't login")
    password = config_dict['auth']['password'].get()
    if not password:
        raise RuntimeError("No password available, can't login")
    return _login(base_url, username, password, timeout)
