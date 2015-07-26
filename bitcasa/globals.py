import logging

from werkzeug.local import LocalStack, LocalProxy

class BITCASA(object):
    BASE_URL = 'https://drive.bitcasa.com'

    class ENDPOINTS(object):
        login = '/login'
        user_account = '/portal/useraccount'
        root_folder = '/portal/v2/folders/' #?media-metadata=true'

    @classmethod
    def url_from_endpoint(cls, endpoint):
        return '%s%s' % (cls.BASE_URL, endpoint)

def connect(username=None, password=None):
    logger.info('making new connection')
    if not username:
        username = _username
    if not password:
        password = _password
    auth = _manager(username, password)
    return auth


def get_connection():
    if not all((_username, _password, _manager)):
        raise RuntimeError('No username and password set.')

    c = _connection_stack.pop()
    if not c:
        return connect()

    return c

def set_connection_params(username, password, manager):
    global _username, _password, _manager
    _manager = manager
    _username = username
    _password = password


def setup_logger(name=None):
    global _logger
    if not _logger:
        logging.basicConfig()
        _logger = logging.getLogger(name or __name__)
        _logger.setLevel(logging.INFO)

    return _logger

_connection_stack = LocalStack()
connection = LocalProxy(get_connection)
_username = None
_password = None
_manager = None
_logger = None
logger = LocalProxy(setup_logger)
