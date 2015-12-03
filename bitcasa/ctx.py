import traceback

from functools import update_wrapper
from .globals import _app_ctx_stack, _app_ctx_err_msg

class BitcasaDriveAppContext(object):
    app = None
    connection_pool = None
    drive = None
    logger = None
    scheduler = None

    logout_on_exit = None

    def __init__(self, app, _connection_pool=None, _drive=None, _logger=None,
                 _scheduler=None):
        self.app = app

        if _logger:
            self.logger = _logger
        else:
            self.logger = app.setup_logger()

        if _connection_pool:
            self.connection_pool = _connection_pool
        #else:
        #    self.connection_pool = app.setup_connection_pool()

        if _drive:
            self.drive = _drive

        if _scheduler:
            self.scheduler = _scheduler
        #else:
        #    self.scheduler = app.setup_scheduler()

        self.logout_on_exit = True

    def copy(self):
        return self.__class__(self.app, self.connection_pool, self.drive,
                              self.logger, self.scheduler)

    def push(self):
        _app_ctx_stack.push(self)

    def pop(self):
        rv = _app_ctx_stack.pop()
        message = 'Popped wrong app context.  (%r instead of %r)'
        message = message % (rv, self)
        assert rv is self, message

    def __enter__(self):
        self.push()

        if not self.connection_pool:
            self.connection_pool = self.app.setup_connection_pool()

        if not self.scheduler:
            self.scheduler = self.app.setup_scheduler()

        if not self.drive:
            self.drive = self.app.setup_drive()

        return self

    def __exit__(self, exc_type, exc_value, tb):
        exception_raised = exc_type is not None
        pdb = self.app.config is not None and self.app.config.pdb

        if (self.logout_on_exit and self.connection_pool and
            not self.connection_pool.using_cookie_file):
            self.connection_pool.logout()

        self.pop()
        if exception_raised:
            traceback.print_exception(exc_type, exc_value, tb)
            if pdb:
                import pdb; pdb.set_trace()


class ConnectionContext(object):
    _pool = None
    _valid = None

    def __init__(self, pool):
        self._pool = pool
        self.auth = self._pool._connect()
        self._valid = True

    def __getattr__(self, name):
        return getattr(self._pool, name)

    def __enter__(self):
        if self._valid:
            return self.auth

    def __exit__(self, exc_type, exc_value, tb):
        if self._valid:
            self._pool.push(self)

    def clear(self):
        self._valid = False
        self.auth = None


def copy_current_app_ctx(f):

    top = _app_ctx_stack.top
    if top is None:
        raise RuntimeError(_app_ctx_err_msg)

    appctx = top.copy()
    appctx.logout_on_exit = False
    def wrapper(*args, **kwargs):
        with appctx:
            return f(*args, **kwargs)

    return update_wrapper(wrapper, f)