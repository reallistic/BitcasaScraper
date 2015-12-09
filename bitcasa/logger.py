import os
import gevent
import logging
from logging.handlers import RotatingFileHandler


lFormat = logging.Formatter(('%(asctime)s [%(gThreadId)s][%(name)s]'
                             '[%(levelname)s]: %(message)s'),
                            '%m/%d %H:%M:%S')


class ContextFilter(logging.Filter):
    """
    This is a filter which injects the gevent thread id as context
    information.
    """

    def filter(self, record):
        greenlet = gevent.getcurrent()
        func_name = 'Main'
        if greenlet and hasattr(greenlet, '_run'):
            func_name = greenlet._run.__name__
        record.gThreadId = getattr(greenlet, 'gid', func_name)
        return True

context_filter = ContextFilter()

# Configure a root logger at runtime
rlogger = logging.getLogger()
rlogger.setLevel(logging.INFO)
rlogger.addFilter(context_filter)

consolehandler = logging.StreamHandler()
consolehandler.setFormatter(lFormat)
consolehandler.addFilter(context_filter)
rlogger.addHandler(consolehandler)

rlogger.info('Finished configuring root logger')

logger = logging.getLogger(__name__)

def _configure_logger(l, config=None, level=None):
    verbose = 0
    log_file = None
    quiet = False

    if config:
        verbose = config.verbose
        log_file = config.log_file
        quiet = config.quiet

    if level:
        log_level = level
    else:
        if verbose == 0:
            log_level = logging.ERROR
        if verbose == 1:
            log_level = logging.WARN
        if verbose == 2:
            log_level = logging.INFO
        if verbose > 2:
            log_level = logging.DEBUG

    if log_file:
        max_size = 1 * 1024 * 1024 # 1MB
        filehandler = RotatingFileHandler(log_file, maxBytes=max_size,
                                          backupCount=5)
        filehandler.addFilter(context_filter)
        l.addHandler(filehandler)
        if os.path.getsize(log_file) > max_size/2:
            filehandler.doRollover()

    if quiet:
        l.removeHandler(consolehandler)

    l.setLevel(log_level)

    logger.info('Finished setting up logger %s %s %s', l.name, log_level,
                log_file)



def setup_misc_loggers():
    for app in ('requests',):
        l = logging.getLogger(app)
        _configure_logger(l, level=logging.WARN)

def setup_scheduler_loggers(config=None):
    l = logging.getLogger('apscheduler')
    verbose = 0
    level = logging.WARN
    if config and config.verbose:
        verbose = config.verbose
    if verbose == 3:
        level = logging.INFO
    if verbose == 4:
        level = logging.DEBUG
    _configure_logger(l, config=config, level=level)

def setup_logger(name=None, config=None):
    logger = logging.getLogger(name)
    _configure_logger(logger, config=config)
