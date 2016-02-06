from functools import partial
from werkzeug import LocalProxy, LocalStack

_app_ctx_err_msg = '''\
Working outside of application context.
This typically means that you attempted to use functionality that needed
to interface with the current application object in a way.  To solve
this set up an application context with app.get_context().\
'''


class BITCASA(object):
    BASE_URL = 'https://drive.bitcasa.com'

    class ENDPOINTS(object):
        download = '/portal/v2/files/'
        login = '/login'
        logout = '/portal/logout'
        user_account = '/portal/useraccount'
        root_folder = '/portal/v2/folders/' #?media-metadata=true'

    @classmethod
    def url_from_endpoint(cls, endpoint):
        return '%s%s' % (cls.BASE_URL, endpoint)


def _get_app_attr(name):
    top = _app_ctx_stack.top
    if top is None:
        raise RuntimeError(_app_ctx_err_msg)

    return getattr(top, name)

_app_ctx_stack = LocalStack()
connection_pool = LocalProxy(partial(_get_app_attr, 'connection_pool'))
current_app = LocalProxy(partial(_get_app_attr, 'app'))
drive = LocalProxy(partial(_get_app_attr, 'drive'))
scheduler = LocalProxy(partial(_get_app_attr, 'scheduler'))
rq = LocalProxy(partial(_get_app_attr, 'rq'))
