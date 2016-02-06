# -*- coding: utf-8 -*-
"""Code for asyncronous functions"""

import uuid

from functools import wraps
from apscheduler.util import obj_to_ref

from .globals import scheduler, current_app, rq


def async(jobstore=None, queue=None):
    if not all((jobstore, queue)):
        raise RuntimeError('Expected both jobstore and queue')

    def wrapper(fn):
        @wraps(fn)
        def inner(*args, **kwargs):
            return fn(*args, **kwargs)

        @wraps(fn)
        def delay(*args, **kwargs):
            worker = current_app.config.worker
            if worker == 'apscheduler':
                return apscheduler_delay(*args, **kwargs)
            elif worker == 'rq':
                return rq_delay(*args, **kwargs)
            else:
                raise RuntimeError('Unknown scheduler type %r' % worker)

        def rq_delay(*args, **kwargs):
            # Enqueue the job and relax.
            q = rq.get_queue(queue) if queue \
                else rq.queue
            return q.enqueue(fn, *args, **kwargs)

        def apscheduler_delay(*args, **kwargs):
            job_id = uuid.uuid4().hex
            kwargs['job_id'] = job_id
            scheduler.add_job(obj_to_ref(inner), args=args, kwargs=kwargs,
                              executor=jobstore, jobstore=jobstore, id=job_id,
                              misfire_grace_time=None)
            return job_id

        inner.async = delay
        inner.original_func = fn

        return inner
    return wrapper
