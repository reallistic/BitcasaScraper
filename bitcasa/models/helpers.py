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


class SimpleObject(object):
    def __init__(self, **kwargs):
        self.update_data(**kwargs)

    def update_data(self, **kwargs):
        for key, val in kwargs.items():
            setattr(self, key, val)
