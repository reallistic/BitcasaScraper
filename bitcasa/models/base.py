import os

from .helpers import LaxObject, LaxObjectMetaClass



class BitcasaItemFactory(object):

    _class_map = {}

    @classmethod
    def register_model(cls, model, data_type=None):
        cls.class_map[model.__name__] = model
        if data_type:
            cls.class_map[data_type] = model

    @classmethod
    def class_map(cls, key, default=None):
        if default is not None:
            return cls._class_map.get(key, default)
        else:
            return cls._class_map[key]

    @classmethod
    def class_from_data(cls, data):
        return cls.class_map(data.get('type'), BitcasaItem)

    @classmethod
    def make_item(cls, data, parent=None):
        item_class = cls.class_map.get(data.get('type'), BitcasaItem)
        ins = item_class.from_meta_data(data, parent=parent)

        if isinstance(ins, cls.class_map('BitcasaFolder')) and data.get('items'):
            ins.items.update(cls.items_from_data(data.get('items'), ins))

    @classmethod
    def items_from_data(self, data, parent):
        items = {}

        if not data:
            return items

        for item in data:
            bitcasa_item = BitcasaItemFactory.make_item(item, parent=parent)
            items[bitcasa_item.id] = bitcasa_item


class BitcasaItemMetaClass(LaxObjectMetaClass):
    def __new__(mcs, name, bases, attrs):

        for base in bases:
            if not hasattr(base, '_data_types '):
                continue

            for data_type in base._data_types:
                BitcasaItemFactory.register_model(base, data_type)

        return super(BitcasaItemMetaClass, mcs).__new__(mcs, name, bases, attrs)


class BitcasaItem(LaxObject):

    __metaclass__ = BitcasaItemMetaClass

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
