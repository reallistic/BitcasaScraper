import os
import errno
import time

from . import utils

from .file_download import FileDownload
from .globals import BITCASA, logger, scheduler, connection_pool, drive
from .jobs import async
from .models import BitcasaFile, BitcasaFolder
from .move import _move_file


@async(jobstore='download')
def download_folder(folder=None, url=None, level=0, max_depth=1, job_id=None,
                    parent=None, destination='./', chunk_size=None,
                    move_to=None, gid=None):
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

    logger.debug('%s, Folder path is %s', gid, folder.path)

    destination = os.path.join(destination, folder.name)
    logger.debug('%s Making dirs for %s', gid, destination)
    try:
        os.makedirs(destination)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(destination):
            pass
        else:
            raise

    for item in folder.items.values():
        logger.debug('%s found item %s', gid, item.name)
        if level + 1 < max_depth and isinstance(item, BitcasaFolder):
            if job_id:
                logger.debug('%s Creating new download folder job %s', gid,
                             item.path)
                download_folder.async(url=item.path, level=level+1,
                                      max_depth=max_depth, parent=folder.path,
                                      destination=destination,
                                      chunk_size=chunk_size,
                                      move_to=move_to)
            else:
                download_folder(folder=item, level=level+1, max_depth=max_depth,
                                parent=folder, destination=destination,
                                chunk_size=chunk_size, move_to=move_to)

        elif isinstance(item, BitcasaFile):
            file_path = os.path.join(destination, item.name)
            if job_id:
                logger.debug('%s Creating new download file job %s', gid,
                             item.name)
                download_file.async(item.path, item.size, file_path,
                                    chunk_size=chunk_size, move_to=move_to)
            else:
                download_file(item.path, item.size, file_path,
                              chunk_size=chunk_size, move_to=move_to)
    logger.debug('%s Finished folder list', gid)


@async(jobstore='download')
def download_file(file_id, size, destination, chunk_size=None, move_to=None,
                  job_id=False, gid=None):

    download = FileDownload(file_id, destination, size, chunk_size=chunk_size,
                            job_id=job_id, gid=gid)
    download.run()

    if move_to:
        if job_id:
            _move_file.async(destination, move_to)
        else:
            _move_file(destination, move_to)
