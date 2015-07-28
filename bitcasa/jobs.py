from gevent import pool

import functools
import time
import uuid

from werkzeug.local import LocalProxy

from apscheduler.util import obj_to_ref

from apscheduler.schedulers.gevent import GeventScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import BasePoolExecutor
from apscheduler.executors.base import run_job

from .logger import logger

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

        g = self._pool.spawn(run_job, job, job._jobstore_alias, run_times,
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


_scheduler = None

def setup_scheduler(config=None):
    global _scheduler

    max_list_workers = 4
    max_download_workers = 2
    max_move_workers = 2
    max_upload_workers = 2

    total_data_workers = 0

    sqlite_uri = 'sqlite:///bitcasajobs.sqlite'
    if config:
        """
        if config.max_list_workers:
            max_list_workers = config.max_list_workers
        if config.max_upload_workers:
            max_upload_workers = config.max_upload_workers
        if config.max_move_workers:
            max_move_workers = config.max_move_workers
        if config.max_download_workers:
            max_download_workers = config.max_download_workers
        if config.sqlite_uri:
            sqlite_uri = config.sqlite_uri
        """

        total_data_workers = max_list_workers + max_download_workers
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
    executors = {'list': GeventPoolExecutor(max_list_workers),
                 'download': GeventPoolExecutor(max_download_workers),
                 'move': GeventPoolExecutor(max_move_workers),
                 'upload': GeventPoolExecutor(max_upload_workers)}
    job_defaults = {'coalesce': False, 'max_instances': 1}
    _scheduler = GeventScheduler(jobstores=jobstores, executors=executors,
                                job_defaults=job_defaults)
    


def get_scheduler():
    return _scheduler

scheduler = LocalProxy(get_scheduler)
