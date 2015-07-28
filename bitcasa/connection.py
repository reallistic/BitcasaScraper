import json
import traceback

from threading import Lock
from Queue import Queue, Empty

from werkzeug.local import LocalProxy

from .authentication import AuthenticationManager
from .exceptions import AuthenticationError
from .logger import logger

class ConnectionContext(object):
    _pool = None
    _valid = None

    def __init__(self, pool):
        self._pool = pool
        self.auth = self._pool._connect()
        self._valid = True

    def __enter__(self):
        if self._valid:
            return self.auth

    def __exit__(self, exc_type, exc_value, tb):
        if self._valid:
            self._pool.push(self)

    def clear(self):
        self._valid = False
        self.auth = None


class ConnectionPool(object):
    auth_class = None
    max_connections = None
    using_cookie_file = None

    _connection_stack = None
    _connections = None
    _cookies = None
    _connect_lock = None
    _password = None
    _username = None

    def __init__(self, username=None, password=None, auth_class=None,
                 max_connections=None, config=None, blocking=True):
        self._username = username or config.username
        self._password = password or config.password
        if not all((self._username, self._password)):
            self._cookies = self._cookies_from_file(config.cookie_file)
        self.max_connections = max_connections or config.max_connections

        self.auth_class = auth_class or AuthenticationManager
        self._connection_stack = Queue()
        self._connections = []

        self._connect_lock = Lock()

    def _cookies_from_file(self, fp):
        cookies = None
        if not fp:
            return None

        try:
            fp.seek(0)
            json_data = json.loads(fp.read())
            self.using_cookie_file = True
            return json_data
        except Exception as err:
            logger.exception('failed loading cookies from file. %s', err.message)

    def _store_cookies(self, fp):
        try:
            fp.seek(0)
            fp.truncate()
            fp.write(json.dumps(self._cookies))
            self.using_cookie_file = True
        except:
            traceback.print_exc()
            raise AuthenticationError('Failed storing cookies to file')

    def _connect(self, username=None, password=None):
        if self._cookies and not all((username, password)):
            auth = self.auth_class(cookies=self._cookies)
        else:
            if not username:
                username = self._username
            if not password:
                password = self._password
            auth = self.auth_class(username, password)
            self._cookies = auth.get_cookies()

        return auth

    def logout(self):
        csrf_token = None
        if self._cookies:
            csrf_token = self._cookies.get('tkey_csrf0portal')
        conn = self.pop(force=True)
        with conn as auth:
            auth.logout()

        self._cookies = None
        for conn in self._connections:
            conn.clear()

    def can_make_connection(self):
        if not self.max_connections:
            return True

        if len(self._connections) < self.max_connections:
            return True

    def pop(self, force=False):
        try:
            return self._connection_stack.get_nowait()
        except Empty:
            pass

        with self._connect_lock:
            if force or self.can_make_connection():
                conn = ConnectionContext(self)
                self._connections.append(conn)
                return conn

        return self._connection_stack.get()

    def push(self, conn):
        self._connection_stack.put(conn)

_connection_pool = None
def get_connection():
    assert _connection_pool, 'Connection pool not setup'

    return _connection_pool

def setup_connection(*args, **kwargs):
    global _connection_pool
    connection_class = kwargs.pop('connection_class', ConnectionPool)
    _connection_pool = connection_class(*args, **kwargs)

connection_pool = LocalProxy(get_connection)
