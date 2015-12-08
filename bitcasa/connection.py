import logging
import json
import traceback

from threading import Lock
from Queue import Queue, Empty

from .authentication import AuthenticationManager
from .ctx import ConnectionContext
from .exceptions import AuthenticationError

logger = logging.getLogger(__name__)


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
        logger.debug('Making new connection pool')

    def _cookies_from_file(self, filename):
        if not filename:
            return None

        cookies = None

        logger.debug('Getting cookies from %s', filename)
        try:
            with open(filename, 'r') as fp:
                cookies = json.loads(fp.read())
        except Exception as err:
            logger.exception('failed loading cookies from file. %s', err.message)
        self.using_cookie_file = True
        return cookies

    def _store_cookies(self, filename):
        logger.debug('Saving cookies to %s', filename)
        try:
            with open(filename, 'w+') as fp:
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
                logger.debug('Making new connection')
                conn = ConnectionContext(self)
                self._connections.append(conn)
                return conn

        return self._connection_stack.get()

    def push(self, conn):
        self._connection_stack.put(conn)

    def get_context(self):
        return ConnectionContext(self)
