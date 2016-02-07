# -*- coding: utf-8 -*-
"""RQ extension for Flask"""

import logging
import sys
import redis
import random
import traceback
#import newrelic.agent

# Disable log handler setup before importing rq related code.
import rq.logutils
rq.logutils.setup_loghandlers = lambda: None

from collections import OrderedDict
from datetime import datetime, timedelta
from io import BytesIO
from rq import Queue
from rq_gevent_worker import GeventWorker
from rq.job import Job, _job_stack, Status
from rq.queue import FailedQueue
from rq.timeouts import BaseDeathPenalty
from rq.utils import utcnow

from .ctx import _app_ctx_stack, _app_ctx_err_msg
from .exceptions import DownloadError
from .jobs import copy_current_app_ctx


logger = logging.getLogger(__name__)



class JobResult(object):
    exception = None
    retval = None

    def __init__(self, job, exc=None):
        self.exception = exc
        self.retval = job.result
        self.job = job


class BitcasaJob(Job):

    """Subclassing RQ Job to customize behavior"""

    def get_loggable_dict(self):
        """Returns a dictionary for logging purposes"""
        rv = dict((('job_id', self.id),
                   ('queue_name', self.origin),
                   ('created_at', self.created_at),
                   ('enqueued_at', self.enqueued_at),
                   ('func', self.func.__name__),
                   ('args', self.args),
                   ('kwargs', self.kwargs)))
        if 'failures' in self.meta:
            rv['failures'] = self.meta['failures']
        return rv

    # Job execution
    def perform(self):  # noqa
        """Invokes the job function with the job arguments"""
        _job_stack.push(self.id)
        try:
            self.set_status(Status.STARTED)
            #newrelic_decorated_func = newrelic.agent.background_task()(self.func)
            #self._result = newrelic_decorated_func(*self.args, **self.kwargs)
            self.kwargs.update(job_id=self.id)
            self._result = self.func(*self.args, **self.kwargs)
            self.ended_at = utcnow()
            self.set_status(Status.FINISHED)
        finally:
            assert self.id == _job_stack.pop()

        return self._result


class BitcasaQueue(Queue):

    """Subclassing RQ Job to customize behavior"""

    job_class = BitcasaJob

    def enqueue_job(self, job, set_meta_data=True):
        """Override enqueue job to insert meta data without saving twice"""
        request_environ = {}
        job.meta['request_environ'] = request_environ

        # Add Queue key set
        added = self.connection.sadd(self.redis_queues_keys, self.key)

        # The rest of this function is copied from the RQ library.
        if set_meta_data:
            job.origin = self.name
            job.enqueued_at = utcnow()

        if job.timeout is None:
            job.timeout = self.DEFAULT_TIMEOUT
        job.save()

        if self._async:
            self.push_job_id(job.id)
        else:
            job.perform()
            job.save()

        return job


class MessageFailedQueue(BitcasaQueue, FailedQueue):

    """Subclassing RQ Failed Queue to customize behavior"""

    job_class = BitcasaJob


class NullDeathPenalty(BaseDeathPenalty):
    def setup_death_penalty(self):
        pass
    def cancel_death_penalty(self):
        pass

class BitcasaWorker(GeventWorker):

    """Subclassing RQ Job to customize behavior"""

    death_penalty_class = NullDeathPenalty
    job_class = BitcasaJob
    queue_class = BitcasaQueue
    max_attempts = None
    discard_on = (DownloadError, )
    _queues = None
    _success_listeners = None
    _failed_listeners = None

    def __init__(self, *args, **kwargs):
        self.max_attempts = kwargs.pop('max_attempts')
        self._queues = {}
        self._success_listeners = set()
        self._failed_listeners = set()
        super(BitcasaWorker, self).__init__(*args, **kwargs)

    @property
    def queues(self):
        """Returns queues in random order while giving priority to the
        default queue by always returning it in the front"""
        queue_names = self.connection.smembers(Queue.redis_queues_keys)
        queue_names = list(queue_names)
        queue_names.sort()
        for queue_name in queue_names:
            if queue_name not in self._queues and not queue_name.endswith(
                    'failed'):
                queue = BitcasaQueue.from_queue_key(
                    queue_name,
                    connection=self.connection)
                self._queues[queue_name] = queue

        return self._queues.values()

    @queues.setter
    def queues(self, value):
        if isinstance(value, BitcasaQueue):
            value = [value]
        if isinstance(value, list):
            for item in value:
                self._queues[item.key] = item

    def execute_job(self, job, queue):
        """Copied form rq_gevent_worker.py to add ctx"""
        def job_done(child):
            self.children.remove(child)
            self.did_perform_work = True
            self.heartbeat()
            if job.get_status() == Status.FINISHED:
                queue.enqueue_dependents(job)

        perform_job = copy_current_app_ctx(self.perform_job)
        child_greenlet = self.gevent_pool.spawn(perform_job, job)
        child_greenlet.link(job_done)
        self.children.append(child_greenlet)

    def on_job_fail(self, cb):
        self._failed_listeners.add(cb)

    def on_job_success(self, cb):
        self._success_listeners.add(cb)

    def execute_listeners(self, job, result, exc=None):
        if result:
            for listener in self._success_listeners:
                listener(JobResult(job))
        else:
            for listener in self._failed_listeners:
                listener(JobResult(job, exc=exc))

    def perform_job(self, job):
        # Without this try catch statement we would not see any tracebacks
        # on errors raised outside of the queued function. In other words,
        # bugs in flask-rq inside of greenlets would fail silently.
        try:
            rv = super(BitcasaWorker, self).perform_job(job)
            if rv:
                self.execute_listeners(job, rv)
            return rv
        except Exception as err:
            logger.exception('Error performing job %r',
                             job.get_loggable_dict())
            self.execute_listeners(job, False, exc=err)
            return False

    def handle_exception(self, job, *exc_info):
        """Overrides handler for failed jobs

        When a job fails we retry a few more times before we let the job be moved
        to the failed queue.

        It's important to note that there is a default exception handler, and that
        this function forms part of a chain. If the default handler is reached,
        the job gets moved to the failed queue. When this function returns None or
        True, we move up the chain of handlers. If this function returns False
        then the chain execution is halted.

        As a result, we return False when a job has been retried the maximum
        number of times.
        """

        # If the job fails and we no longer want to retry then save job with
        # latest retry count and move it to the failed queue.
        exc_type, exc_value, tb = exc_info
        job.meta.setdefault('failures', 0)
        job.meta['failures'] += 1
        job.meta['exception'] = str(exc_value.message)

        # Format the traceback string.
        exc_string = ''.join(traceback.format_exception(*exc_info))

        # Compute conditions first to keep if statements clean.
        max_attempts_reached = job.meta['failures'] >= self.max_attempts
        discard_immediately = isinstance(exc_type, self.discard_on)
        too_old = job.created_at < datetime.utcnow() - timedelta(seconds=60)

        if False and discard_immediately:
            # There is no need to retry, just log the error.
            logger.exception('Error performing job %r',
                             job.get_loggable_dict(), exc_info=exc_info)
        if (max_attempts_reached or too_old):
            # This is likely an important job, put it in the failed queue.
            logger.exception('Error performing job %r',
                             job.get_loggable_dict(), exc_info=exc_info)
            self.failed_queue.quarantine(job, exc_info=exc_string)
            self.execute_listeners(job, False, exc=exc_value)
        else:
            # Otherwise we mark the job as queued again and resubmit it to
            # the queue it came from.
            job.set_status(Status.QUEUED)
            queue_lookup = dict([(q.name, q) for q in self.queues])
            queue = queue_lookup.get(job.origin)
            if queue:
                queue.enqueue_job(job)
            else:
                logger.error('Queue disappeared. job=%r',
                             job.get_loggable_dict())


class RQ(object):

    _queue = None
    _failed_queue = None
    _timeout = None
    _connection = None
    max_attempts = None
    result_ttl = 1800

    def __init__(self, config):
        self._redis_url = config.jobs_uri
        self._timeout = 0
        self.max_attempts = 1

    def clear_empty_queues(self):
        queues = self.get_all_queues()
        empty_queues = [queue for queue in queues if queue.is_empty()]
        for queue in empty_queues:
            if queue.name not in ['failed', 'default']:
                self.connection.delete(queue.key)
                self.connection.srem(queue.redis_queues_keys, queue.key)

    @property
    def connection(self):
        # Re-use connections if they are determined equivalent.
        if not self._connection:
            self._connection = redis.from_url(self._redis_url)

        return self._connection

    @property
    def queue(self):
        if not self._queue:
            self._queue = BitcasaQueue('default',
                                  connection=self.connection,
                                  default_timeout=self._timeout)
        return self._queue

    def get_queue(self, queue_name):
        queue = BitcasaQueue(queue_name,
                        connection=self.connection,
                        default_timeout=self._timeout)
        return queue

    def get_all_queues(self):
        return BitcasaQueue.all(self.connection)

    @property
    def failed_queue(self):
        if not self._failed_queue:
            self._failed_queue = MessageFailedQueue(connection=self.connection)
        return self._failed_queue

    def create_worker(self, **kwargs):
        """Creates a gevent worker to consume a queue

        Also note that we do not initialize the worker with a specific set
        of queues since it is itself responsible for discovering all
        queues on the given connection.
        """
        return BitcasaWorker(self.queue, connection=self.connection,
                             max_attempts=self.max_attempts,
                             default_result_ttl=self.result_ttl, **kwargs)
