
import logging

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

from .exceptions import DownloadError
from .globals import scheduler
from .models import Base, BitcasaItem, FileDownloadResult, FolderListResult

logger = logging.getLogger(__name__)

class ResultRecorder(object):
    db = None
    engine = None

    def __init__(self, config):
        self.engine = engine = create_engine(config.results_uri)
        Base.metadata.create_all(engine)
        self.db = Session(engine)

    def listen(self):
        scheduler.add_listener(self.record_success, mask=EVENT_JOB_EXECUTED)
        scheduler.add_listener(self.record_error, mask=EVENT_JOB_ERROR)

    def record_error(self, event):
        logger.debug('Received event with error: %r', event.exception)
        if isinstance(event.exception, DownloadError):
            item = event.exception.item
            self.save_download_result(item)

    def get_download(self, item_id):
        return self.db.query(FileDownloadResult).get(item_id)

    def add_list_result(self, item):
        if not self.db.query(BitcasaItem).get(item.id):
            self.db.add(item)

    def save_list_result(self, item):
        try:
            self.add_list_result(item)
            self.db.commit()
        except:
            logger.exception('Error commiting results to list db')

    def save_list_results(self, results):
        try:
            for item in results:
                self.add_list_result(item)
            self.db.commit()
        except:
            logger.exception('Error commiting results to list db')

    def save_download_result(self, item):
        try:
            db_item = self.db.query(FileDownloadResult).get(item.id)
            if db_item:
                db_item.attempts += 1
            else:
                self.db.add(item)
            self.db.commit()
        except:
            logger.exception('Error commiting results to download result db')

    def record_success(self, event):
        if not event.retval:
            return

        logger.debug('Received event with result: %r', event.retval)
        if isinstance(event.retval, FolderListResult):
            self.save_list_results(event.retval.items)
        elif isinstance(event.retval, BitcasaItem):
            self.save_list_result(event.retval)
        elif isinstance(event.retval, FileDownloadResult):
            self.save_download_result(event.retval)

    def list_results(self):
        q = self.db.query(BitcasaItem).order_by(BitcasaItem.path_name).all()
        for item in q:
            print item.path_name

    def close(self):
        self.db.close()
        self.engine.dispose()
