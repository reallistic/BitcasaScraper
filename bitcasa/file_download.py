
import logging
import gevent
import os
import time
import traceback

from requests.exceptions import ChunkedEncodingError, RequestException
from requests.packages.urllib3.exceptions import ProtocolError

from . import utils

from .exceptions import ConnectionError, SizeMismatchError, DownloadError
from .globals import BITCASA, drive, connection_pool, current_app
from .models import FileDownloadResult

logger = logging.getLogger(__name__)


def get_gid():
    greenlet = gevent.getcurrent()
    func_name = 'n/a'
    if greenlet and hasattr(greenlet, '_run'):
        func_name = greenlet._run.__name__
    return getattr(greenlet, 'gid', func_name)

class FileDownload(object):

    PROGRESS_INTERVAL = 20
    chunk_size = None
    destination = None
    gid = None
    job_id = False

    mode = None
    seek = None
    size_copied = None
    st = None
    url = None
    progress_greenlet = None


    def __init__(self, file_id, destination, size, chunk_size=None,
                 max_retries=None, job_id=None):
        self.chunk_size = chunk_size or drive.config.chunk_size or 1024
        self.destination = destination
        self.job_id = job_id
        self.size = size
        self.path = file_id

        self.num_retries = max_retries or 3
        self.num_size_retries = 3

        self._finished = False

    @property
    def alive(self):
        return current_app and current_app.running

    def run(self):
        logger.info('downloading file %s', self.destination)
        self.mode = 'wb'
        self.seek = 0
        self.size_copied = 0
        self.st = 0

        try:
            self.seek = os.path.getsize(self.destination)
        except:
            pass

        if self.seek > self.size:
            self.seek = 0
        elif self.seek == self.size:
            logger.info('File of equal name and size exist. '
                         'Nothing to download')
            self._finished = True
        elif self.seek > 0:
            self.size_copied += self.seek
            logger.debug('continuing download from %s', self.seek)
            self.mode = 'ab'

        return self._run()

    def _run(self):
        error = None
        error_message = None
        max_retries = self.num_retries
        while (self.alive and not self._finished and self.num_retries > 0 and
               self.num_size_retries > 0):
            try:
                ctx = connection_pool.pop()
                with ctx as conn:
                    # throw away this connection
                    self._download_file(conn)
            except SizeMismatchError:
                self.num_size_retries -= 1
                if self.num_size_retries <= 0:
                    error = traceback.format_exc()
                    error_message = 'Max retries reached'
                    logger.exception(error_message)
                else:
                    self.seek = self.size_copied
                    self.mode = 'ab'
                    logger.exception('Retrying download for %s',
                                     self.destination)
                    gevent.sleep(5)
            except (ConnectionError, RequestException):
                ctx.clear()
                self.num_retries -= 1
                if self.num_retries <= 0:
                    error = traceback.format_exc()
                    error_message = 'Max retries reached'
                    logger.exception(error_message)
                else:
                    self.seek = self.size_copied
                    self.mode = 'ab'
                    logger.exception('Retrying download for %s',
                                     self.destination)
                    gevent.sleep(max_retries - self.num_retries)
            except:
                error = traceback.format_exc()
                logger.exception('Exception downloading %s', self.destination)
                error_message = 'Exception downloading %s' % self.destination

        if not self.alive:
            return None

        item = FileDownloadResult(id=self.path,
                                  destination=self.destination,
                                  size=self.size,
                                  size_downloaded=self.size_copied,
                                  name=os.path.split(self.destination)[-1],
                                  attempts=1,
                                  success=True,
                                  error=error)
        if error:
            item.success = False
            raise DownloadError(error_message, item=item)

        if self.seek != self.size:
            cr = time.time()
            speed = utils.get_speed(self.size_copied-self.seek, (cr-self.st))
            logger.info('Finished downloading file %s at %s',
                         self.destination, speed)
        else:
            logger.info('Finished downloading file %s', self.destination)

        return item

    def save_next_chunk(self, tmpfile, content):
        try:
            chunk = content.next()
        except ProtocolError as e:
            chunk = e.args[1].partial
            logger.warn('Using partial chunk of length: %s',
                        len(chunk))
        except ChunkedEncodingError as e:
            chunk = e.args[0].args[1].partial
            logger.warn('Using partial chunk of length: %s',
                        len(chunk))
        except StopIteration:
            return False
        if not chunk:
            return False

        tmpfile.write(chunk)
        self.size_copied += len(chunk)
        return True

    def _download_file(self, conn):
        if self.progress_greenlet:
            self.progress_greenlet.kill(block=False)
        self.progress_greenlet = gevent.spawn_later(self.PROGRESS_INTERVAL,
                                                    self.report_progress)
        gid = get_gid()
        self.progress_greenlet.gid = gid
        self.st = time.time()
        url = os.path.join(BITCASA.ENDPOINTS.download, self.path.lstrip('/'))
        req = conn.make_download_request(url, seek=self.seek)
        # We probably won't be able to download anything if this is True,
        # but that will get caught below.
        if req.raw._fp and not req.raw._fp.isclosed():
            req.raw._fp.fp._sock.settimeout(100)

        content = req.iter_content(self.chunk_size)

        with open(self.destination, self.mode) as tmpfile:
            while self.alive:
                if not self.save_next_chunk(tmpfile, content):
                    break

        self.progress_greenlet.kill(block=False)
        self.progress_greenlet = None
        size_copied_str = utils.convert_size(self.size_copied)
        size_str = utils.convert_size(self.size)

        if self.alive and self.size_copied < self.size:
            message = 'Expected %s downloaded %s - %s'
            message = message % (size_str, size_copied_str, req.url)
            raise SizeMismatchError(message)
        elif self.alive and self.size_copied > self.size:
            logger.warn('Final size more than expected. Got %s expected %s',
                        size_copied_str, size_str)
        self._finished = True

    def report_progress(self):
        cr = time.time()
        speed = utils.get_speed(self.size_copied-self.seek, (cr-self.st))
        size_copied_str = utils.convert_size(self.size_copied)
        size_str = utils.convert_size(self.size)
        time_left = utils.get_remaining_time(self.size_copied-self.seek,
                                             self.size-self.size_copied,
                                             (cr-self.st))
        logger.info(self.destination)
        logger.info('Downloaded %s of %s at %s. %s left.',
                    size_copied_str, size_str, speed, time_left)
        self.progress_greenlet = gevent.spawn_later(self.PROGRESS_INTERVAL,
                                                    self.report_progress)
        gid = get_gid()
        self.progress_greenlet.gid = gid
