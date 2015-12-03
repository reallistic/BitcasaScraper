from sqlalchemy import create_engine

from apscheduler.events import EVENT_JOB_EXECUTED
from sqlalchemy.orm import Session

from .args import BitcasaParser
from .config import ConfigManager
from .connection import ConnectionPool
from .ctx import BitcasaDriveAppContext
from .download import download_folder
from .list import list_folder
from .drive import BitcasaDrive
from .globals import scheduler, drive, connection_pool, current_app, logger
from .jobs import setup_scheduler
from .logger import setup_logger, setup_scheduler_loggers
from .models import Base, BitcasaItem

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
            self.listen_for_results()
            scheduler.start()
            self.setup_models()

        if self.config.command == 'shell':
            import code
            local_params = dict(drive=drive, config=self.config,
                                pool=connection_pool, scheduler=scheduler)
            code.interact(local=local_params)

        if self.config.command == 'download':
            logger.debug('doing download')
            download_folder.async(url=self.config.bitcasa_folder,
                                  destination=self.config.download_folder,
                                  max_depth=self.config.max_depth)

            executor = scheduler._lookup_executor('download')
            executor.wait()

        if self.config.command == 'list':
            logger.debug('doing list')
            list_folder.async(max_depth=self.config.max_depth,
                              url=self.config.bitcasa_folder)

            executor = scheduler._lookup_executor('list')
            executor.wait()
            self.list_results()

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
        app_logger = setup_logger('BitcasaConsole', config=self.config)
        setup_scheduler_loggers(config=self.config)
        return app_logger

    def setup_scheduler(self):
        app_scheduler = setup_scheduler(config=self.config)

        return app_scheduler

    def setup_models(self):
        engine = create_engine(self.config.results_uri)
        Base.metadata.create_all(engine)
        self.db = Session(engine)

    def listen_for_results(self):
        scheduler.add_listener(self.record_results, mask=EVENT_JOB_EXECUTED)

    def record_results(self, event):
        logger.debug('Received event with result: %r', event.retval)
        if event.retval:
            for item in event.retval:
                exists = self.db.query(BitcasaItem).\
                    filter(BitcasaItem.id == item.id).count()
                if not exists:
                    self.db.add(item)

            try:
                self.db.commit()
            except:
                logger.exception('Error commiting results to db')

    def list_results(self):
        for item in self.db.query(BitcasaItem).order_by(BitcasaItem.path).all():
            #print '%s%s - %s' % (''.join(['   '] * item.level), item.name,
            #                     item.id)
            print '%s - %s' % (item.path_name, item.name)
