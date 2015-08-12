import os

from .base import BitcasaItem

from ..globals import BITCASA, logger, connection_pool


class BitcasaFolder(BitcasaItem):

    _keys = ['is_root']
    items = None

    _data_types = ['root', 'folder']

    @classmethod
    def from_meta_data(cls, data, parent=None, path=None, level=0):
        # Inject is_root.
        if 'meta' in data:
            meta_data = data.get('meta', {})
        else:
            meta_data = data

        meta_data['is_root'] = (meta_data.get('type') == 'root')
        return super(BitcasaFolder, cls).from_meta_data(data,
                                                        parent=parent,
                                                        path=path,
                                                        level=level)

    def get_full_url(self):
        path = self.path.lstrip('/')
        return os.path.join(BITCASA.ENDPOINTS.root_folder, path)

    def fetch_items(self, force=False):
        if not self.items or force:
            url = self.get_full_url()
            logger.debug('fetching folder: %s | %s', path, url)
            with connection_pool.pop() as conn:
                folder_meta = conn.request(url)
            self.items_from_data(folder_meta['result'].get('items'))

        items_sorted = self.items
        return sorted(items_sorted.values(),
                       key=lambda item: item.name.lower())

    def list(self):
        return self.fetch_items()
