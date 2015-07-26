from threading import Lock
from Queue import Queue, Empty

from .authentication import AuthenticationManager
from .globals import logger

class ConnectionContext(object):
    _pool = None
    _valid = None

    def __init__(self, pool):
        self._pool = pool
        self.auth = self._pool._connect()
        self._valid = True

    def __enter__(self):
        if self._valid:
            logger.debug('%s entering context manager' % self.auth.id)
            return self.auth

    def __exit__(self, exc_type, exc_value, tb):
        if self._valid:
            logger.debug('%s releasing connection' % self.auth.id)
            self._pool.push(self)

    def clear(self):
        self._valid = False
        self.auth = None


class ConnectionPool(object):
    auth_class = None
    max_connections = None

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
        self.max_connections = max_connections or config.max_connections

        self.auth_class = auth_class or AuthenticationManager
        self._connection_stack = Queue()
        self._connections = []

        self._connect_lock = Lock()

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
                logger.debug('%s making new connection' % conn.auth.id)
                return conn

        return self._connection_stack.get()

    def push(self, conn):
        self._connection_stack.put(conn)
