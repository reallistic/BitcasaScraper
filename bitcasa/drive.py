import os

from .download import download_file
from .exceptions import BitcasaError
from .globals import BITCASA, connection_pool, logger
from .models import BitcasaUser
from .models import BitcasaItemFactory


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
        self.root = BitcasaItemFactory.from_meta_data(root_meta['result'])
        return self.root

    def make_download_url(self, bfile):
        url = os.path.join(self.user.content_base_url,
                           BITCASA.ENDPOINTS.download, bfile.digest,
                           bfile.nonce, bfile.payload)
        return url

    def download_file(self, bfile, destination):
        chunk_size = self.config.chunk_size or 1024 * 1024

        url = self.make_download_url(bfile)
        return download_file(url, destination, chunk_size)

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
