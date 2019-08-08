

from .basic import login as basic_auth  # NOQA
from .config import login as config_auth


def auth(base_url, config_dict, timeout=10):
    auth_type = config_dict['auth']['type'].get()
    if auth_type == 'basic':
        return config_auth(base_url, config_dict, timeout)
    raise NotImplementedError("Auth type is unsupported " + auth_type)
