import os
import logging
from logging.handlers import RotatingFileHandler

def _configure_logger(l, config=None, level=None):
    log_level = logging.ERROR
    lFormat = logging.Formatter('%(asctime)s [%(name)s][%(levelname)s]: %(message)s', '%m/%d %H:%M:%S')
    if not config:
        l.setLevel(log_level)
        return

    if level:
        log_level = level
    elif config.verbose == 0:
        log_level = logging.WARN
    elif config.verbose == 1:
        log_level = logging.INFO
    elif config.verbose > 1:
        log_level = logging.DEBUG

    if config.log_file:
        logfile = config.log_file
        max_size = 1 * 1024 * 1024 # 1MB
        filehandler = RotatingFileHandler(logfile, maxBytes=max_size,
                                          backupCount=5)
        filehandler.setLevel(log_level)
        filehandler.setFormatter(lFormat)
        l.addHandler(filehandler)
        if os.path.getsize(logfile) > max_size/2:
            l.handlers[0].doRollover()

    if not config.quiet:
        consolehandler = logging.StreamHandler()
        consolehandler.setLevel(log_level)
        consolehandler.setFormatter(lFormat)
        l.addHandler(consolehandler)

    l.setLevel(log_level)


def setup_scheduler_loggers(config=None):
    for app in ['scheduler', 'executors', 'jobstores']:
        for store in ['download', 'list', 'move', 'upload']:
            l = logging.getLogger('.'.join(['apscheduler', app, store]))
            _configure_logger(l, config=config, level=logging.WARN)

def setup_logger(name=None, config=None):
    logger = logging.getLogger(name)
    _configure_logger(logger, config=config)
    logger.debug('Logging loaded')

    return logger
