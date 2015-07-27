import logging

from werkzeug.local import LocalProxy

def setup_logger(name=None, config=None):
    global _logger
    if not _logger:
        maxsize = 1 * 1024* 1024 #1mb

        _logger = logging.getLogger(name)

        log_level = logging.ERROR
        lFormat = logging.Formatter('%(asctime)s [%(threadName)s][%(levelname)s]: %(message)s', '%m/%d %H:%M:%S')
        if config:
            if config.verbose == 0:
                log_level = logging.WARN
            elif config.verbose == 1:
                log_level = logging.INFO
            elif config.verbose > 1:
                log_level = logging.DEBUG

            if False:
                logfile = './logs/bitcasadrive.log'
                if config.logfile:
                    logfile = config.logfile
                filehandler = RotatingFileHandler(logfile, maxBytes=maxsize, backupCount=5)
                filehandler.setLevel(log_level)
                filehandler.setFormatter(lFormat)
                _logger.addHandler(filehandler)
                if os.path.getsize(logfile) > maxsize/2:
                    _logger.handlers[0].doRollover()

        if True: # config.console_logging.
            consolehandler = logging.StreamHandler()
            consolehandler.setLevel(log_level)
            consolehandler.setFormatter(lFormat)
            _logger.addHandler(consolehandler)

        _logger.setLevel(log_level)

        _logger.info('Logging loaded')

    return _logger

_logger = None

logger = LocalProxy(setup_logger)
