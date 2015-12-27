
import gevent
import logging

from gevent.event import Event

from apscheduler.schedulers.base import BaseScheduler
from apscheduler.schedulers.gevent import GeventScheduler as GeventSchedulerBase

from .ctx import copy_current_app_ctx


logger = logging.getLogger(__name__)


class GeventScheduler(GeventSchedulerBase):
    def start(self):
        BaseScheduler.start(self)
        self._event = Event()

        @copy_current_app_ctx
        def run_main_loop():
            self._main_loop()

        self._greenlet = gevent.spawn(run_main_loop)
        return self._greenlet

    def _main_loop(self):
        while self.running:
            wait_seconds = self._process_jobs()
            if wait_seconds is None:
                wait_seconds = self.MAX_WAIT_TIME
            self._event.wait(wait_seconds)
            self._event.clear()
