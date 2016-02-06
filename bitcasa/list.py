import os
import logging
import gevent

from .globals import BITCASA, connection_pool, current_app
from .jobs import async
from .models import BitcasaFolder, FolderListResult


logger = logging.getLogger(__name__)


@async(jobstore='list')
def list_folder(folder=None, url=None, level=0, max_depth=1, job_id=None,
                parent=None, gid=None):
    if folder:
        url = folder.path
    elif not url:
        url = '/'

    if not parent:
        parent = '/'.join(url.split('/')[:-1])

    url = os.path.join(BITCASA.ENDPOINTS.root_folder.rstrip('/'), url.lstrip('/'))

    num_retries = 30
    while num_retries > 0:
        try:
            with connection_pool.pop() as conn:
                data = conn.request(url)
            break
        except:
            logger.exception('Retrying list')
            num_retries -= 1
            gevent.sleep(30-num_retries)

    if num_retries <= 0:
        logger.error('Listing folder at url %s failed', url)
        return

    if folder:
        child_items = data['result'].get('items')
        folder.items_from_data(child_items)
    else:
        folder = BitcasaFolder.from_meta_data(data['result'], parent=parent,
                                              level=level)

    results = [folder]
    items = folder.items.values()
    items.sort(key=lambda item: item.name.lower())
    for item in items:
        if not current_app.running:
            break
        results.append(item)

        #if job_id:
            #workflow = WorkflowHelper.from_job(job_id)
            #workflow.do_next_step()

        if ((not max_depth or level + 1 < max_depth) and
            isinstance(item, BitcasaFolder)):
            if job_id:
                list_folder.async(url=item.path, level=level+1,
                                  max_depth=max_depth, parent=folder.path)
            else:
                results += list_folder(folder=item, level=level+1, max_depth=max_depth,
                                       parent=folder)

    logger.info('Finished listing folder %s', folder.path_name)
    return FolderListResult(results)
