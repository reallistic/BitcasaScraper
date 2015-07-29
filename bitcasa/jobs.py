import gevent
import functools
import sys
import time
import uuid

from gevent import pool
from gevent.event import Event

from apscheduler.util import obj_to_ref

from apscheduler.schedulers.gevent import GeventScheduler as GeventSchedulerBase
from apscheduler.schedulers.base import BaseScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import BasePoolExecutor
from apscheduler.executors.base import run_job

from .ctx import copy_current_app_ctx
from .globals import logger, scheduler, _app_ctx_stack


class GeventPoolExecutor(BasePoolExecutor):
    def __init__(self, max_workers=10):
        gevent_pool = pool.Pool(size=max_workers)
        super(GeventPoolExecutor, self).__init__(gevent_pool)
        self.__greenlets_spawned = 0
        self.__greenlets_died = 0

    def _do_submit_job(self, job, run_times):
        self.__greenlets_spawned += 1
        def callback(greenlet):
            self.__greenlets_died += 1
            try:
                events = greenlet.get()
            except:
                self._run_job_error(job.id, *sys.exc_info()[1:])
            else:
                self._run_job_success(job.id, events)

        @copy_current_app_ctx
        def run_job_in_ctx(*args, **kwargs):
            return run_job(*args, **kwargs)

        g = self._pool.spawn(run_job_in_ctx, job, job._jobstore_alias, run_times,
                             self._logger.name)
        g.num = self.__greenlets_spawned
        g.link(callback)

    def shutdown(self, wait=True):
        if wait:
            self._pool.join()

    def wait(self):
        while not self.__greenlets_spawned:
            time.sleep(1)

        while self.__greenlets_spawned > self.__greenlets_died:
            time.sleep(0.1)
            self._pool.join()

        self._pool.join()


class GeventScheduler(GeventSchedulerBase):
    def start(self):
        BaseScheduler.start(self)
        self._event = Event()

        @copy_current_app_ctx
        def run_main_loop():
            self._main_loop()

        self._greenlet = gevent.spawn(run_main_loop)
        return self._greenlet


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
        if config.sqlite_uri:
            sqlite_uri = config.sqlite_uri

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
