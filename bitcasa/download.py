import os
import errno
import gevent
import time
import logging

from . import utils

from .file_download import FileDownload
from .globals import BITCASA, scheduler, connection_pool, drive, current_app
from .jobs import async
from .models import BitcasaFile, BitcasaFolder, FolderListResult
from .move import _move_file

logger = logging.getLogger(__name__)


@async(jobstore='download')
def download_folder(folder=None, url=None, level=0, max_depth=1, job_id=None,
                    parent=None, destination='./', chunk_size=None,
                    move_to=None, max_retries=None, max_attempts=None):
    if folder:
        url = folder.path
    elif not url:
        url = BITCASA.ENDPOINTS.root_folder

    if not parent:
        parent = '/'.join(url.split('/')[:-1])

    url = os.path.join(BITCASA.ENDPOINTS.root_folder, url.lstrip('/'))

    with connection_pool.pop() as conn:
        data = conn.request(url)

    if folder:
        child_items = data['result'].get('items')
        folder.items_from_data(child_items)
    else:
        folder = BitcasaFolder.from_meta_data(data['result'], parent=parent,
                                              level=level)

    logger.info('Listing folder %s', folder.path_name)
    logger.debug('Folder path is %s', folder.path)

    destination = os.path.join(destination, folder.name)
    logger.debug('Making dirs for %s', destination)
    try:
        os.makedirs(destination)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(destination):
            pass
        else:
            raise

    results = [folder]
    for item in folder.items.values():
        if not current_app.running:
            break

        logger.info('List item %s', item.name)
        results.append(item)

        if ((not max_depth or level + 1 < max_depth) and
            isinstance(item, BitcasaFolder)):
            if job_id:
                logger.debug('Creating new download folder job %s',
                             item.path)
                download_folder.async(url=item.path, level=level+1,
                                      max_depth=max_depth, parent=folder.path,
                                      destination=destination,
                                      chunk_size=chunk_size,
                                      move_to=move_to,
                                      max_retries=max_retries,
                                      max_attempts=max_attempts)
            else:
                download_folder(folder=item, level=level+1, max_depth=max_depth,
                                parent=folder, destination=destination,
                                chunk_size=chunk_size, move_to=move_to,
                                max_retries=max_retries,
                                max_attempts=max_attempts)

        elif isinstance(item, BitcasaFile):
            file_path = os.path.join(destination, item.name)
            download = current_app.results.get_download(item.path)
            if download and download.success:
                logger.debug('File download already exist. Skipping %s',
                             item.name)
                continue
            if (not max_attempts or
                (download and download.attempts >= max_attempts)):
                logger.debug(('File download failed more than allowed. '
                             'Skipping %s'), item.name)
                continue

            if job_id:
                logger.debug('Creating new download file job %s',
                             item.name)
                download_file.async(item.path, item.size, file_path,
                                    chunk_size=chunk_size, move_to=move_to,
                                    max_retries=max_retries)
            else:
                download_file(item.path, item.size, file_path,
                              chunk_size=chunk_size, move_to=move_to,
                              max_retries=max_retries)

    logger.info('Finished listing folder %s', folder.path_name)
    gevent.sleep(0.1)
    return FolderListResult(results)


@async(jobstore='download')
def download_file(file_id, size, destination, chunk_size=None, move_to=None,
                  max_retries=None, job_id=False):

    logger.info('Download item %s', destination)
    download = FileDownload(file_id, destination, size, chunk_size=chunk_size,
                            max_retries=max_retries, job_id=job_id)
    result = download.run()

    if move_to:
        if job_id:
            _move_file.async(destination, move_to)
        else:
            _move_file(destination, move_to)
    return result
