from .globals import BITCASA, logger, scheduler, connection_pool
from .jobs import async
from .models import BitcasaFile, BitcasaFolder


@async(jobstore='list')
def list_folder(folder=None, url=None, level=0, max_depth=1, job_id=None,
                parent=None, print_files=False):
    if folder:
        url = folder.get_full_url()
    elif not url:
        url = BITCASA.ENDPOINTS.root_folder

    with connection_pool.pop() as conn:
        data = conn.request(url)

    if folder:
        child_items = data['result'].get('items')
        folder.items_from_data(child_items)
    else:
        folder = BitcasaFolder.from_meta_data(data['result'], parent=parent,
                                              level=level)

    if print_files:
        print '%s%s - %s' % (''.join(['   ']*level), folder.name, folder.id)
    results = []
    items = folder.items.values()
    items.sort(key=lambda item: item.name.lower())
    for item in items:
        if not job_id:
            results.append(item)

        if level + 1 < max_depth and isinstance(item, BitcasaFolder):
            if job_id:
                list_folder.async(url=item.get_full_url(), level=level+1,
                                  max_depth=max_depth, parent=folder.path,
                                  print_files=print_files)
            else:
                results += list_folder(folder=item, level=level+1, max_depth=max_depth,
                                       parent=folder, print_files=print_files)
        elif print_files and not item.name.startswith('.'):
            print '%s%s - %s' % (''.join(['   '] * item.level), item.name, item.id)

    if not job_id:
        return results


@async(jobstore='download')
def download_file(url, destination, chunk_size, move_to=None, job_id=False):
    with connection_pool.pop() as conn:
        req = conn.make_download_request(url)
        with open(destination, 'w+') as tmpfile:
            content = req.iter_content(chunk_size=chunk_size)
            while not scheduler or scheduler.running:
                try:
                    chunk = content.next()
                except StopIteration:
                    break
                tmpfile.write(chunk)
        conn.request_lock.release()

    if move_to:
        if job_id:
            _move_file.async(destination, move_to)
        else:
            _move_file(destination, move_to)


@async(jobstore='move')
def _move_file(src, destination, job_id=None):
    with open(destination, 'rb') as srcfile, open(move_to, 'wb') as destfile:
        while not scheduler or scheduler.running:
            piece = srcfile.read(1024)
            if piece:
                destfile.write(piece)
            else:
                break
