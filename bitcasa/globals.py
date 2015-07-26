import logging

from werkzeug.local import LocalProxy

class BITCASA(object):
    BASE_URL = 'https://drive.bitcasa.com'

    class ENDPOINTS(object):
        login = '/login'
        logout = '/portal/logout'
        user_account = '/portal/useraccount'
        root_folder = '/portal/v2/folders/' #?media-metadata=true'

    @classmethod
    def url_from_endpoint(cls, endpoint):
        return '%s%s' % (cls.BASE_URL, endpoint)

def setup_logger(name=None, config=None):
    global _logger
    if not _logger:
        logging.basicConfig()
        _logger = logging.getLogger(name or __name__)
        _logger.setLevel(logging.DEBUG)

    return _logger

_logger = None

logger = LocalProxy(setup_logger)
