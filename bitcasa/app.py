
import logging

from .args import BitcasaParser
from .config import ConfigManager
from .connection import ConnectionPool
from .ctx import BitcasaDriveAppContext
from .download import download_folder
from .list import list_folder
from .drive import BitcasaDrive
from .globals import scheduler, drive, connection_pool, current_app
from .jobs import setup_scheduler
from .logger import setup_logger, setup_misc_loggers, setup_scheduler_loggers
from .results import ResultRecorder

logger = logging.getLogger(__name__)

class BitcasaDriveApp(object):
    """Simple app to use for context management"""
    results = None

    def __init__(self, connection_class=ConnectionPool,
                 drive_class=BitcasaDrive):
        self.shutdown_start = False
        self.shutdown_finished = False
        self._running = False
        parser = BitcasaParser()
        self.args = parser.parse_args()
        self.config = ConfigManager(self.args).get_config()
        self.connection_class = connection_class
        self.drive_class = drive_class
        self.setup_logger()

    def get_context(self):
        return BitcasaDriveAppContext(self)

    @property
    def running(self):
        return self._running

    def run(self):
        """Wrapper to make putting things in a huge try catch easier"""
        self._running = True
        try:
            self._run()
        finally:
            self._running = False
            while not self.shutdown_finished:
                try:
                    self.tear_down()
                except KeyboardInterrupt:
                    pass

    def tear_down(self):
        if self.shutdown_start:
            return

        self.shutdown_start = True
        if scheduler and scheduler.running:
            logger.info('Shutting down scheduler')
            scheduler.shutdown()

        if self.results:
            logger.info('Closing results')
            self.results.close()
        logger.info('goodbye')
        self.shutdown_finished = True

    def _run(self):
        message = 'Working in wrong app context. (%r instead of %r)'
        message = message % (current_app, self)
        assert current_app == self, message

        if self.config.command in ['list', 'download']:
            scheduler.start()
            self.results = ResultRecorder(self.config)
            self.results.listen()

        if self.config.command == 'shell':
            import code
            local_params = dict(drive=drive, config=self.config,
                                pool=connection_pool, scheduler=scheduler)
            code.interact(local=local_params)

        if self.config.command == 'download':
            logger.debug('doing download')
            if self.config.max_retries <= 0:
                logger.warn('Trying to disable max retries could cause '
                            'the program to run forever. '
                            'Setting max retries to 3')
                # Note this override is done in the FileDownload class
            download_folder.async(url=self.config.bitcasa_folder,
                                  destination=self.config.download_folder,
                                  max_depth=self.config.max_depth,
                                  max_attempts=self.config.max_attempts,
                                  max_retries=self.config.max_retries)

            scheduler.wait()

        if self.config.command == 'list':
            logger.debug('doing list')
            list_folder.async(max_depth=self.config.max_depth,
                              url=self.config.bitcasa_folder)

            scheduler.wait()
            self.results.list_results()

        if self.config.command == 'logout':
            connection_pool.logout()
            with open(self.config.cookie_file, 'w+'):
                pass

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
        setup_logger('bitcasa', config=self.config)
        setup_scheduler_loggers(config=self.config)
        setup_misc_loggers()

    def setup_scheduler(self):
        app_scheduler = setup_scheduler(config=self.config)
        return app_scheduler
