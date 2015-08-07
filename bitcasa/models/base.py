import os

from .helpers import LaxObject


_registered_models = {}

class BitcasaItemMeta(type):
    def __new__(mcs, name, bases, attrs):
        global _registered_models

        for base in bases:
            if base.__class__ == type:
                continue
            if base.__class__.__name__ in _registered_models:
                continue

            _registered_models[base.__class__.__name__] = base

        return type.__new__(mcs, name, bases, attrs)


class BitcasaItem(LaxObject):

    _keys = ['modified', 'created', 'id', 'name', 'parent_id',
             'version', 'path', 'path_name', 'level']

    def __repr__(self):
        return '<%s %s:%s>' % (self.__class__.__name__, self.id, self.name)

    @classmethod
    def from_meta_data(cls, data, parent=None, path=None, level=0):
        if 'meta' in data:
            meta_data = data.get('meta', {})
        else:
            meta_data = data

        item = cls.trim_dict(meta_data)

        app_data = meta_data.get('application_data', {})
        item['path_name'] = app_data.get('running_path_name')

        item['modified'] = meta_data.get('date_content_last_modified')
        item['created'] = meta_data.get('date_created')


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
                elif isinstance(parent, basestring):
                    item['path'] = os.path.join(parent,
                                                meta_data.get('id'))
            else:
                item['path'] = os.path.join('/', meta_data.get('parent_id'),
                                            meta_data.get('id'))

        item_cls = BitcasaItemFactory.class_from_data(meta_data)
        return item_cls(**item)

    def __str__(self):
        return '<%s>' % (self.id or 'root')



class BitcasaItemFactory(object):
    @classmethod
    def class_from_data(cls, data):
        class_map = {'root': _registered_models['BitcasaFolder'],
                     'folder': _registered_models['BitcasaFolder'],
                     'file': _registered_models['BitcasaFile']}
        return class_map.get(data.get('type'), BitcasaItem)

    @classmethod
    def make_item(cls, data, parent=None):
        item_class = cls.class_map.get(data.get('type'), BitcasaItem)
        return item_class.from_meta_data(data, parent=parent)

