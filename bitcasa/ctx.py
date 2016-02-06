import traceback

from functools import update_wrapper
from .globals import _app_ctx_stack, _app_ctx_err_msg

class BitcasaDriveAppContext(object):
    app = None
    connection_pool = None
    drive = None
    scheduler = None
    rq = None

    logout_on_exit = None

    def __init__(self, app, connection_pool=None, drive=None,
                 scheduler=None, rq=None):
        self.app = app

        if connection_pool:
            self.connection_pool = connection_pool

        if drive:
            self.drive = drive

        if scheduler:
            self.scheduler = scheduler

        if rq:
            self.rq = rq

        self.logout_on_exit = True

    def copy(self):
        return self.__class__(self.app, self.connection_pool, self.drive,
                              self.scheduler, self.rq)

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

        if not self.rq:
            self.rq = self.app.setup_rq()

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
