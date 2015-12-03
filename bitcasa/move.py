import os
import errno
import time

from . import utils

from .globals import logger, scheduler
from .jobs import async


@async(jobstore='move')
def _move_file(src, destination, job_id=None):
    with open(destination, 'rb') as srcfile, open(move_to, 'wb') as destfile:
        while not scheduler or scheduler.running:
            piece = srcfile.read(1024)
            if piece:
                destfile.write(piece)
            else:
                break
