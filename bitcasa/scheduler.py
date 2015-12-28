
import gevent
import logging

from gevent.event import Event
from gevent.queue import JoinableQueue

from apscheduler.schedulers.base import BaseScheduler
from apscheduler.schedulers.gevent import GeventScheduler as GeventSchedulerBase

from .ctx import copy_current_app_ctx


logger = logging.getLogger(__name__)


class GeventScheduler(GeventSchedulerBase):
    def start(self):
        BaseScheduler.start(self)
        self._event = Event()
        self._queue = JoinableQueue()

        @copy_current_app_ctx
        def run_main_loop():
            self._main_loop()

        self._greenlet = gevent.spawn(run_main_loop)
        return self._greenlet

    def _main_loop(self):
        while self.running:
            self.add_queued_jobs()
            wait_seconds = self._process_jobs()
            if wait_seconds is None:
                wait_seconds = self.MAX_WAIT_TIME
            self._event.wait(wait_seconds)
            self._event.clear()

    def _job_done(self):
        self._queue.task_done()

    def add_job(self, *args, **kwargs):
        self._queue.put_nowait((args, kwargs))
        self.wakeup()

    def add_queued_jobs(self):
        logger.info('Adding queued job')
        while True:
            try:
                args, kwargs = self._queue.get_nowait()
            except:
                break
            super(GeventScheduler, self).add_job(*args, **kwargs)

    def wait(self):
        if not self.running:
            return

        while self._queue.qsize():
            self._queue.join()
