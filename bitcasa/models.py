
import logging
import os

from sqlalchemy import Column, ForeignKey, types
from sqlalchemy.orm import backref, relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.collections import attribute_mapped_collection

from .globals import BITCASA, drive

Base = declarative_base()

logger = logging.getLogger(__name__)

class SimpleObject(object):
    def __init__(self, **kwargs):
        self.update_data(**kwargs)

    def update_data(self, **kwargs):
        for key, val in kwargs.items():
            setattr(self, key, val)


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
            raise NotImplementedError('Missing _keys')

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

class BitcasaItem(Base):

    __tablename__ = 'drive'
    id = Column(types.Text(), primary_key=True)
    parent_id = Column(types.Text(), ForeignKey(id))
    name = Column(types.Text())
    modified = Column(types.Integer)
    created = Column(types.Integer)
    version = Column(types.Integer)
    path_name = Column(types.Text())
    path = Column(types.Text())
    level = Column(types.Integer)
    extension = Column(types.Text())
    mime = Column(types.Text())
    size = Column(types.Integer)
    nonce = Column(types.Text())
    payload = Column(types.Text())
    digest = Column(types.Text())
    blid = Column(types.Text())
    is_root = Column(types.Boolean())
    is_folder = Column(types.Boolean())

    children = relationship('BitcasaItem',
                            cascade='all, delete-orphan',
                            backref=backref('parent', remote_side=id),
                            collection_class=attribute_mapped_collection('id'))

    def __repr__(self):
        return '<%s %s:%s>' % (self.__class__.__name__, self.id, self.name)

    @classmethod
    def from_meta_data(cls, data, parent=None, path=None, level=0):
        if 'meta' in data:
            meta_data = data.get('meta', {})
        else:
            meta_data = data

        app_data = meta_data.get('application_data', {})

        item = {}
        item['name'] = meta_data.get('name')
        item['parent_id'] = meta_data.get('parent_id')
        item['id'] = meta_data.get('id')
        item['version'] = meta_data.get('version')

        item['modified'] = meta_data.get('date_content_last_modified')
        item['created'] = meta_data.get('date_created')

        item['path_name'] = app_data.get('_server', {}).get('running_path_name')

        if isinstance(parent, BitcasaItem):
            item['level'] = parent.level + 1
        else:
            item['level'] = level

        if path:
            item['path'] = path
        else:
            if parent:
                if isinstance(parent, BitcasaItem):
                    item['path'] = os.path.join(parent.path,
                                                meta_data.get('id'))
                    if not item['path_name']:
                        item['path_name'] = os.path.join(parent.path_name, item['name'])
                elif isinstance(parent, basestring):
                    item['path'] = os.path.join(parent,
                                                meta_data.get('id'))
            else:
                item['path'] = os.path.join('/', meta_data.get('parent_id'),
                                            meta_data.get('id'))

        item_cls = BitcasaItemFactory.class_from_data(meta_data)
        return item_cls(**item)

    def __str__(self):
        return self.id or '<root>'


class BitcasaFile(BitcasaItem):

    @classmethod
    def from_meta_data(cls, data, parent=None, path=None, level=0):
        # inject file data.
        app_data = data.get('application_data', {})
        nebula = app_data.get('_server', {}).get('nebula', {})
        ins = super(BitcasaFile, cls).from_meta_data(data,
                                                     parent=parent,
                                                     path=path,
                                                     level=level)
        ins.nonce = nebula.get('nonce')
        ins.blid = nebula.get('blid')
        ins.digest = nebula.get('digest')
        ins.payload = nebula.get('payload')
        ins.extension = data.get('extension')
        ins.mime = data.get('mime')
        ins.size = data.get('size')
        ins.is_root = False
        ins.is_folder = False

        return ins

    def download(self, destination_dir, name=None):
        destination = os.path.join(destination_dir, name or self.name)
        drive.download_file(self, destination)


class BitcasaFolder(BitcasaItem):

    items = None

    @classmethod
    def from_meta_data(cls, data, parent=None, path=None, level=0):
        ins = super(BitcasaFolder, cls).from_meta_data(data,
                                                       parent=parent,
                                                       path=path,
                                                       level=level)

        if 'meta' in data:
            meta_data = data.get('meta', {})
        else:
            meta_data = data

        ins.is_root = (meta_data.get('type') == 'root')
        ins.is_folder = True

        child_items = data.get('items')
        ins.items_from_data(child_items)

        return ins

    def items_from_data(self, data):
        if self.items is None:
            self.items = {}

        if not data:
            return

        for item in data:
            bitcasa_item = BitcasaItemFactory.make_item(item, parent=self)
            self.items[bitcasa_item.id] = bitcasa_item

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


class BitcasaItemFactory(object):
    class_map = {'root': BitcasaFolder,
                 'folder': BitcasaFolder,
                 'file': BitcasaFile}
    @classmethod
    def class_from_data(cls, data):
        return cls.class_map.get(data.get('type'), BitcasaItem)

    @classmethod
    def make_item(cls, data, parent=None):
        item_class = cls.class_map.get(data.get('type'), BitcasaItem)
        return item_class.from_meta_data(data, parent=parent)


class FileDownloadResult(Base):
    __tablename__ = 'downloads'
    id = Column(types.Text(), primary_key=True)
    name = Column(types.Text())
    size = Column(types.Integer)
    size_downloaded = Column(types.Integer)
    destination = Column(types.Text())
    attempts = Column(types.Integer)
    error = Column(types.Text())
    success = Column(types.Boolean())

class FolderListResult(object):
    """Helper class to properly route results"""
    items = None

    def __init__(self, items):
        self.items = items
