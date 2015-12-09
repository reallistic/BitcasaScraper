
import logging
import gevent
import os
import time
import select
import traceback

from requests.exceptions import ChunkedEncodingError, RequestException
from requests.packages.urllib3.exceptions import ProtocolError

from . import utils

from .exceptions import ConnectionError, SizeMismatchError, DownloadError
from .globals import BITCASA, drive, connection_pool, scheduler
from .models import FileDownloadResult

logger = logging.getLogger(__name__)


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


    def __init__(self, file_id, destination, size, chunk_size=None,
                 job_id=None):
        self.chunk_size = chunk_size or drive.config.chunk_size or None
        self.destination = destination
        self.job_id = job_id
        self.size = size
        self.path = file_id

        self._finished = False

    def run(self):
        logger.debug('downloading file %s', self.destination)
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
            logger.debug('File of equal name and size exist. '
                         'Nothing to download')
            self._finished = True
        elif self.seek > 0:
            self.size_copied += self.seek
            logger.info('continuing download')
            self.mode = 'ab'

        return self._run()

    def _run(self):
        self.num_retries = 3
        error = None
        error_message = None
        while not self._finished and self.num_retries > 0:
            try:
                ctx = connection_pool.pop()
                with ctx as conn:
                    # throw away this connection
                    ctx.clear()
                    self._download_file(conn)
            except (ConnectionError, RequestException, SizeMismatchError):
                self.num_retries -= 1
                if self.num_retries <= 0:
                    error = traceback.format_exc()
                    error_message = 'Max retries reached'
                else:
                    self.seek = self.size_copied
                    self.mode = 'ab'
                    logger.exception('Retrying download for %s',
                                     self.destination)
                    gevent.sleep(5)
            except:
                error = traceback.format_exc()
                logger.exception('Exception downloading %s', self.destination)
                error_message = 'Exception downloading %s' % self.destination

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
            logger.debug('Finished downloading file %s at %s',
                         self.destination, speed)
        else:
            logger.debug('Finished downloading file %s', self.destination)

        return item

    def _download_file(self, conn):
        url = os.path.join(BITCASA.ENDPOINTS.download, self.path.lstrip('/'))
        req = conn.make_download_request(url, seek=self.seek)
        # We probably won't be able to download anything, but that
        # will get caught below.
        if req.raw._fp and not req.raw._fp.isclosed():
            req.raw._fp.fp._sock.settimeout(100)

        self.st = time.time()
        content = req.iter_content(self.chunk_size)
        progress_time = self.st + self.PROGRESS_INTERVAL
        timespan = 0

        size_str = utils.convert_size(self.size)
        size_copied_str = utils.convert_size(self.size_copied)

        last_chunk = None
        chunk = None
        num_iterations = 0
        with open(self.destination, self.mode) as tmpfile:
            while scheduler and scheduler.running:
                last_chunk = chunk
                chunk = None
                num_iterations += 1
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
                    break
                if not chunk:
                    break
                tmpfile.write(chunk)
                cr = time.time()
                self.size_copied += len(chunk)
                size_copied_str = utils.convert_size(self.size_copied)

                if self.size_copied > self.size:
                    logger.warn('Downloaded %s expected %s',
                                size_copied_str, size_str)

                if progress_time < cr:
                    progress_time = cr + self.PROGRESS_INTERVAL
                    self.report_progress(cr)

        if self.size_copied < self.size:
            message = 'Expected %s downloaded %s - %s'
            message = message % (size_str, size_copied_str, req.url)
            raise SizeMismatchError(message)
        elif self.size_copied > self.size:
            logger.warn('Final size more than expected. Got %s expected %s',
                        size_copied_str, size_str)
        self._finished = True

        conn.request_lock.release()

    def report_progress(self, cr):
        speed = utils.get_speed(self.size_copied-self.seek, (cr-self.st))
        size_copied_str = utils.convert_size(self.size_copied)
        size_str = utils.convert_size(self.size)
        time_left = utils.get_remaining_time(self.size_copied-self.seek,
                                             self.size-self.size_copied,
                                             (cr-self.st))
        logger.info(self.destination)
        logger.info('Downloaded %s of %s at %s. %s left',
                    size_copied_str, size_str, speed, time_left)
