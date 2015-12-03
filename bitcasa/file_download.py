
import gevent
import os
import time
import select

from requests.exceptions import ChunkedEncodingError
from requests.packages.urllib3.exceptions import ProtocolError

from . import utils

from .exceptions import ConnectionError, SizeMismatchError
from .globals import BITCASA, drive, logger, connection_pool, scheduler


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
                 job_id=None, gid=None):
        self.chunk_size = chunk_size or drive.config.chunk_size or None
        self.destination = destination
        self.gid = gid
        self.job_id = job_id
        self.size = size
        self.path = file_id

        self._finished = False

    def run(self):
        logger.debug('%s downloading file %s', self.gid, self.destination)
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
            logger.debug('%s Found temp. Nothing to download', self.gid)
            logger.debug('%s finished downloading file %s', self.gid,
                         self.destination)
            return
        elif self.seek > 0:
            self.size_copied += self.seek
            logger.info('%s continuing download', self.gid)
            self.mode = 'ab'

        self._run()

    def _run(self):
        self.num_retries = 3
        while not self._finished and self.num_retries > 0:
            try:
                ctx = connection_pool.pop()
                with ctx as conn:
                    # throw away this connection
                    ctx.clear()
                    self._download_file(conn)
            except (ConnectionError, SizeMismatchError):
                self.num_retries -= 1
                if self.num_retries <= 0:
                    logger.exception('%s Max retries reached', self.gid)
                    return
                else:
                    #self.mode = 'wb'
                    #self.seek = 0
                    #self.size_copied = 0
                    self.seek = self.size_copied
                    self.mode = 'ab'
                    logger.exception('%s Retrying download for %s',
                                     self.gid, self.destination)
                    gevent.sleep(5)
                    logger.debug('%s woke up from retry sleep', self.gid)

        cr = time.time()
        speed = utils.get_speed(self.size_copied-self.seek, (cr-self.st))
        logger.debug('%s finished downloading file %s at %s', self.gid,
                     self.destination, speed)

    def _download_file(self, conn):
        url = os.path.join(BITCASA.ENDPOINTS.download, self.path.lstrip('/'))
        req = conn.make_download_request(url, seek=self.seek)
        #req.raw._fp.fp._sock.socket.setblocking(0)
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
                    logger.warn('%s Using partial chunk of length: %s',
                                self.gid, len(chunk))
                except ChunkedEncodingError as e:
                    chunk = e.args[0].args[1].partial
                    logger.warn('%s Using partial chunk of length: %s',
                                self.gid, len(chunk))
                except StopIteration:
                    break
                if not chunk:
                    break
                tmpfile.write(chunk)
                cr = time.time()
                self.size_copied += len(chunk)
                size_copied_str = utils.convert_size(self.size_copied)

                if self.size_copied > self.size:
                    logger.warn('%s Downloaded %s expected %s', self.gid,
                                size_copied_str, size_str)

                if progress_time < cr:
                    progress_time = cr + self.PROGRESS_INTERVAL
                    self.report_progress(cr)

        if self.size_copied < self.size:
            message = 'Expected %s downloaded %s - %s'
            message = message % (size_str, size_copied_str, req.url)
            raise SizeMismatchError(message)
        elif self.size_copied > self.size:
            logger.warn('%s Final size more than expected. Got %s expected %s',
                        self.gid, size_copied_str, size_str)
        self._finished = True

        conn.request_lock.release()

    def report_progress(self, cr):
        speed = utils.get_speed(self.size_copied-self.seek, (cr-self.st))
        size_copied_str = utils.convert_size(self.size_copied)
        size_str = utils.convert_size(self.size)
        time_left = utils.get_remaining_time(self.size_copied-self.seek,
                                             self.size-self.size_copied,
                                             (cr-self.st))
        logger.info('%s %s', self.gid, self.destination)
        logger.info('%s Downloaded %s of %s at %s. %s left', self.gid,
                    size_copied_str, size_str, speed, time_left)