import os

from .globals import BITCASA

class LaxObjectMetaClass(type):
    def __new__(mcs, name, bases, attrs):
        master_keys = attrs['_keys'] or []
        master_keys = set(master_keys)

        for base in bases:
            if base.__class__ == type:
                continue

            keys = base._keys
            if keys:
                master_keys |= set(keys)

        attrs['_keys'] = list(master_keys)
        return type.__new__(mcs, name, bases, attrs)


class LaxObject(object):
    __metaclass__ = LaxObjectMetaClass

    _keys = None
    def __init__(self, **kwargs):
        if not self._keys:
            raise NotImplemented

        self.update_data(store_missing=True, **kwargs)


    @classmethod
    def trim_dict(cls, data):
        new_dict = {}
        for key in cls._keys:
            new_dict[key] = data.get(key)
        return new_dict

    def update_data(self, **kwargs):
        store_missing = kwargs.pop('store_missing', False)

        for key in self._keys:
            if store_missing or key in kwargs:
                setattr(self, key, kwargs.get(key))



class BitcasaUser(LaxObject):

    _keys = ['account_id', 'account_state', 'created_at', 'email',
             'first_name', 'last_name', 'id', 'last_login', 'username',
             'syncid', 'storage_limit', 'storage_usage', 'content_base_url']

    @classmethod
    def from_account_data(cls, data):
        data = data['result']
        user_data = data.get('user', {})
        account_data = data.get('account', {})
        session_data = user_data.get('session', {})
        storage_data = user_data.get('storage', {})

        user = data.get('user', {}).copy()
        user['account_plan'] = user_data.get('account_plan',
                                             {}).get('display_name')
        user['account_state'] = user_data.get('account_state',
                                              {}).get('display_name')
        user['storage_limit'] = storage_data.get('limit')
        user['storage_usage'] = storage_data.get('usage')
        user['syncid'] = session_data.get('syncid')
        user['content_base_url'] = account_data.get('usercontent_domain')

        return cls(**user)

class BitcasaItem(LaxObject):

    _keys = ['modified', 'created', 'id', 'name', 'parent_id',
             'version', 'path', 'path_name', 'drive']

    @classmethod
    def from_meta_data(cls, data, drive, parent=None, path=None):
        if 'meta' in data:
            meta_data = data.get('meta', {})
        else:
            meta_data = data

        app_data = meta_data.get('application_data')

        item = cls.trim_dict(meta_data)
        item['modified'] = meta_data.get('date_content_last_modified')
        item['created'] = meta_data.get('date_created')

        item['path_name'] = app_data.get('running_path_name')
        item['drive'] = drive

        if path:
            item['path'] = path
        else:
            if parent:
                item['path'] = os.path.join(parent.path, meta_data.get('id'))
            else:
                item['path'] = os.path.join('/', meta_data.get('parent_id'),
                                            meta_data.get('id'))

        item_cls = BitcasaItemFactory.class_from_data(meta_data, drive)
        return item_cls(**item)

    def __str__(self):
        return self.id or '<root>'


class BitcasaFile(BitcasaItem):

    _keys = ['extension', 'mime', 'size', 'nonce', 'payload', 'digest',
             'blid']

    @classmethod
    def from_meta_data(cls, data, drive, parent=None, path=None):
        # inject file data.
        app_data = data.get('application_data')
        nebula = app_data.get('_server', {}).get('nebula', {})

        data['nonce'] = nebula.get('nonce')
        data['blid'] = nebula.get('blid')
        data['digest'] = nebula.get('digest')
        data['payload'] = nebula.get('payload')
        ins = super(BitcasaFile, cls).from_meta_data(data, drive,
                                                     parent=parent,
                                                     path=path)
        return ins


class BitcasaFolder(BitcasaItem):

    _keys = ['is_root']
    items = None

    @classmethod
    def from_meta_data(cls, data, drive, parent=None, path=None):
        # Inject is_root.
        if 'meta' in data:
            meta_data = data.get('meta', {})
        else:
            meta_data = data

        meta_data['is_root'] = (meta_data.get('type') == 'root')
        ins = super(BitcasaFolder, cls).from_meta_data(data, drive,
                                                       parent=parent,
                                                       path=path)

        child_items = data.get('items')
        ins.items_from_data(child_items)

        return ins

    def items_from_data(self, data):
        if self.items is None:
            self.items = {}

        if not data:
            return

        for item in data:
            bitcasa_item = BitcasaItemFactory.make_item(item, self.drive,
                                                        parent=self)
            self.items[bitcasa_item.id] = bitcasa_item

    def fetch_items(self, force=False):
        if not self.items or force:
            path = self.path.rstrip('/')
            url = os.path.join(BITCASA.ENDPOINTS.root_folder, path)
            with self.drive.connection_pool.pop() as conn:
                folder_meta = conn.request(url)
            self.items_from_data(folder_meta['result'].get('items'))

        items_sorted = self.items
        return sorted(items_sorted.values(),
                       key=lambda item: item.name.lower())

    def list(self):
        return self.fetch_items()


class BitcasaItemFactory(object):
    class_map = {'root': BitcasaFolder,
                 'folder': BitcasaFolder,
                 'file': BitcasaFile}
    @classmethod
    def class_from_data(cls, data, drive):
        return cls.class_map.get(data.get('type'), BitcasaItem)

    @classmethod
    def make_item(cls, data, drive, parent=None):
        item_class = cls.class_map.get(data.get('type'), BitcasaItem)
        return item_class.from_meta_data(data, drive, parent=parent)
