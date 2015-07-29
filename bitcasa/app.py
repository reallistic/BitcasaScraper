from .args import BitcasaParser
from .config import ConfigManager
from .connection import ConnectionPool
from .ctx import BitcasaDriveAppContext
from .download import list_folder
from .drive import BitcasaDrive
from .globals import (scheduler, drive, connection_pool, current_app,
                      logger)
from .logger import setup_logger, setup_scheduler_loggers
from .jobs import setup_scheduler

class BitcasaDriveApp(object):
    """Simple app to use for context management"""

    def __init__(self, connection_class=ConnectionPool,
                 drive_class=BitcasaDrive):
        parser = BitcasaParser()
        self.args = parser.parse_args()
        self.config = ConfigManager(self.args).get_config()
        self.connection_class = connection_class
        self.drive_class = drive_class

    def get_context(self):
        return BitcasaDriveAppContext(self)

    def run(self):
        message = 'Working in wrong app context. (%r instead of %r)'
        message = message % (current_app, self)
        assert current_app == self, message

        if self.config.command in ['list', 'download']:
            logger.warn('starting scheduler')
            scheduler.start()

        if self.config.command == 'shell':
            import code
            local_params = dict(drive=drive, config=self.config,
                                pool=connection_pool, scheduler=scheduler)
            code.interact(local=local_params)

        if self.config.command == 'list':
            logger.warn('doing list')
            list_folder.async(max_depth=self.config.max_depth,
                              print_files=True)

            executor = scheduler._lookup_executor('list')
            executor.wait()

        if self.config.command == 'authenticate':
            message = 'Username and password must be specified'
            assert all((self.config.username, self.config.password)), message
            connection_pool._store_cookies(self.config.cookie_file)

    def setup_connection_pool(self):
        return self.connection_class(config=self.config).get_context()

    def setup_drive(self):
        if not self.config.auth:
            return

        if self.config.command in ['shell']:
            _drive = BitcasaDrive(*args, **kwargs)
            return self.drive_class(config=self.config)
        else:
            return self.drive_class(config=self.config, auto_fetch_root=False)

    def setup_logger(self):
        app_logger = setup_logger('BitcasaConsole', config=self.config)
        setup_scheduler_loggers(config=self.config)
        return app_logger

    def setup_scheduler(self):
        app_scheduler = setup_scheduler(config=self.config)

        return app_scheduler
