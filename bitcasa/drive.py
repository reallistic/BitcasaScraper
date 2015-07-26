from .connection import ConnectionPool
from .exceptions import *
from .globals import logger, BITCASA
from .models import BitcasaFolder, BitcasaUser

class BitcasaDrive(object):
    config = None
    root = None
    user = None

    def __init__(self, config=None, auto_fetch_root=True, connection_pool=None):
        self.config = config
        self.connection_pool = connection_pool or ConnectionPool()
        self.get_user()
        if auto_fetch_root:
            self.fetch_drive()

    def get_user(self):
        if not self.user:
            with self.connection_pool.pop() as conn:
                response_data = conn.request(BITCASA.ENDPOINTS.user_account)
            self.user = BitcasaUser.from_account_data(response_data)
        return self.user

    def fetch_drive(self):
        with self.connection_pool.pop() as conn:
            root_meta = conn.request(BITCASA.ENDPOINTS.root_folder)

        self.root = BitcasaFolder.from_meta_data(root_meta['result'], self)
        return self.root

    def list(self, auto_fetch_drive=True):
        if not (auto_fetch_drive or self.root):
            raise BitcasaError('Root not fetched')

        if not self.root and auto_fetch_drive:
            self.fetch_drive()

        return self.root.list()

    def make_request(self, *args, **kwargs):

        with self.connection_pool.pop() as conn:
            data = conn.request(*args, **kwargs)

        return data
