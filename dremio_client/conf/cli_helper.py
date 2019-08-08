from .config_parser import build_config
from ..auth import auth
from confuse import NotFoundError


def get_base_url_token(args=None):
    config = build_config(args)
    ssl = 's' if config['ssl'].get(bool) else ''
    host = config['hostname']
    port = ":" + str(config['port'].get(int))
    base_url = 'http{}://{}{}'.format(ssl, host, port)
    try:
        timeout = config['auth']['timeout'].get(int)
    except NotFoundError:
        timeout = 10
    token = auth(base_url, config, timeout)
    return base_url, token
