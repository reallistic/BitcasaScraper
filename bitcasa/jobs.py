
import logging
import gevent
import functools
import sys
import time
import uuid

from gevent.lock import Semaphore
from gevent.pool import Pool as BasePool, Group
from gevent.queue import Queue

from apscheduler.util import obj_to_ref

from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import BasePoolExecutor
from apscheduler.executors.base import (MaxInstancesReachedError,
                                        run_job as base_run_job)

from .ctx import copy_current_app_ctx
from .globals import scheduler, _app_ctx_stack
from .scheduler import GeventScheduler

logger = logging.getLogger(__name__)


class Pool(BasePool):
    def start(self, greenlet, blocking=True):
        rv = self.add(greenlet, blocking=blocking)
        if rv:
            greenlet.start()
        return rv

    def add(self, greenlet, blocking=True):
        acquired = self._semaphore.acquire(blocking=blocking)
        if not acquired:
            return False
        try:
            Group.add(self, greenlet)
        except:
            self._semaphore.release()
            raise
        return True


def run_job(job, *args, **kwargs):
    return base_run_job(job, *args, **kwargs)


class GeventPoolExecutor(BasePoolExecutor):
    def __init__(self, max_workers=10):
        gevent_pool = Pool(size=max_workers)
        super(GeventPoolExecutor, self).__init__(gevent_pool)
        self.__count_lock = Semaphore()
        self.__greenlets_spawned = 0
        self.__greenlets_died = 0
        self._queue = Queue()
        self._monitor = None
        self._shutdown = False

    def _monitor_pool(self):
        while True:
            g = self._queue.get()
            self._pool.start(g)

            if self._shutdown:
                break

    def _queue_spawn(self, greenlet):
        self._queue.put_nowait(greenlet)
        if not self._monitor:
            self._monitor = gevent.spawn(copy_current_app_ctx(self._monitor_pool))
            self._monitor.gid = 'queue monitor'

    def _do_submit_job(self, job, run_times):
        with self.__count_lock:
            self.__greenlets_spawned += 1

        @copy_current_app_ctx
        def callback(greenlet):
            with self.__count_lock:
                self.__greenlets_died += 1

            try:
                events = greenlet.get()
            except:
                self._run_job_error(job.id, *sys.exc_info()[1:])
            else:
                self._run_job_success(job.id, events)

            self._scheduler._job_done()


        g = self._pool.greenlet_class(copy_current_app_ctx(run_job), job,
                                      job._jobstore_alias, run_times,
                                      self._logger.name)
        g.gid = 'Thread-%s' % self.__greenlets_spawned
        g.link(callback)


        if not self._pool.start(g, False):
            self._queue_spawn(g)


    def shutdown(self, wait=True):
        self._shutdown = True
        if wait and self.__greenlets_spawned > self.__greenlets_died:
            logger.debug('%s greenlets spawned, %s died. Waiting..',
                         self.__greenlets_spawned, self.__greenlets_died)
        if wait:
            self._pool.join()

    def wait(self):
        if not self.__greenlets_spawned:
            gevent.sleep(5)

        if not self.__greenlets_spawned:
            logger.warn('No greenlets spawned before timeout. Exiting')
            return

        while self.__greenlets_spawned > self.__greenlets_died:
            logger.debug('%s greenlets spawned, %s died. Waiting..',
                         self.__greenlets_spawned, self.__greenlets_died)
            self._pool.join()

        logger.debug('%s greenlets spawned, %s died. Ending',
                     self.__greenlets_spawned, self.__greenlets_died)

        self._pool.join()


def async(jobstore):
    def wrapper(f):
        #TODO: assert the function accepts the job_id kwarg

        @functools.wraps(f)
        def inner(*args, **kwargs):
            return f(*args, **kwargs)

        @functools.wraps(f)
        def delay(*args, **kwargs):
            job_id = uuid.uuid4().hex
            kwargs['job_id'] = job_id
            scheduler.add_job(obj_to_ref(inner), args=args, kwargs=kwargs,
                              executor=jobstore, jobstore=jobstore, id=job_id,
                              misfire_grace_time=None)
            return job_id
        inner.async = delay
        inner.original_func = f
        return inner
    return wrapper


def setup_scheduler(config=None):
    list_workers = 4
    download_workers = 4
    move_workers = 2
    upload_workers = 2

    total_data_workers = 0

    sqlite_uri = 'sqlite:///bitcasajobs.sqlite'
    if config:
        if config.list_workers:
            list_workers = config.list_workers
        """
        if config.upload_workers:
            upload_workers = config.upload_workers
        """
        if config.move_workers:
            move_workers = config.move_workers
        if config.download_workers:
            download_workers = config.download_workers
        if config.jobs_uri:
            sqlite_uri = config.jobs_uri

        total_data_workers = list_workers + download_workers
        if (config.max_connections and
            total_data_workers > config.max_connections):
            logger.warn('Using more workers than available connections: %s/%s',
                        total_data_workers, config.max_connections)

    jobstores = {'list': SQLAlchemyJobStore(url=sqlite_uri,
                                            tablename='list_jobs'),
                 'upload': SQLAlchemyJobStore(url=sqlite_uri,
                                              tablename='upload_jobs'),
                 'move': SQLAlchemyJobStore(url=sqlite_uri,
                                            tablename='move_jobs'),
                 'download': SQLAlchemyJobStore(url=sqlite_uri,
                                                tablename='download_jobs')}
    executors = {'list': GeventPoolExecutor(list_workers),
                 'download': GeventPoolExecutor(download_workers),
                 'move': GeventPoolExecutor(move_workers),
                 'upload': GeventPoolExecutor(upload_workers)}
    job_defaults = {'coalesce': False, 'max_instances': 1}
    return GeventScheduler(jobstores=jobstores, executors=executors,
                           job_defaults=job_defaults)
