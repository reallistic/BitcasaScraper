import os

from .download import download_file
from .exceptions import BitcasaError
from .globals import BITCASA, connection_pool
from .models import BitcasaFolder, BitcasaUser


class BitcasaDrive(object):
    config = None
    root = None
    user = None

    def __init__(self, config=None, auto_fetch_root=True):
        self.config = config

        self.get_user()
        if auto_fetch_root:
            self.fetch_drive()

    def get_user(self):
        if not self.user:
            response_data = self.make_request(BITCASA.ENDPOINTS.user_account)
            self.user = BitcasaUser.from_account_data(response_data['result'])
        return self.user

    def fetch_drive(self):
        root_meta = self.make_request(BITCASA.ENDPOINTS.root_folder)
        self.root = BitcasaFolder.from_meta_data(root_meta['result'])
        return self.root

    def make_download_url(self, bfile):
        return os.path.join(BITCASA.ENDPOINTS.download, bfile.path)

    def download_file(self, bfile, destination):
        download_file(bfile.path, bfile.size, destination)

    def list(self, auto_fetch_drive=True):
        if not (auto_fetch_drive or self.root):
            raise BitcasaError('Root not fetched')

        if not self.root and auto_fetch_drive:
            self.fetch_drive()

        return self.root.list()

    def make_request(self, *args, **kwargs):

        with connection_pool.pop() as conn:
            data = conn.request(*args, **kwargs)

        return data
