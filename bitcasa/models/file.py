import os

from .base import BitcasaItem

from ..download import download_file
from ..globals import drive


class BitcasaFile(BitcasaItem):

    _keys = ['extension', 'mime', 'size', 'nonce', 'payload', 'digest',
             'blid']

    @classmethod
    def from_meta_data(cls, data, parent=None, path=None, level=0):
        # inject file data.
        app_data = data.get('application_data', {})
        nebula = app_data.get('_server', {}).get('nebula', {})

        data['nonce'] = nebula.get('nonce')
        data['blid'] = nebula.get('blid')
        data['digest'] = nebula.get('digest')
        data['payload'] = nebula.get('payload')
        ins = super(BitcasaFile, cls).from_meta_data(data,
                                                     parent=parent,
                                                     path=path,
                                                     level=level)
        return ins

    def download(self, destination_dir, name=None):
        destination = os.path.join(destination_dir, name or self.name)
        drive.download_file(self, destination)

