import argparse

from .globals import connection
from .exceptions import *
from .models import BITCASA, BitcasaUser, BitcasaFolder


class BitcasaParser(argparse.ArgumentParser):

    def __init__(self, *args, **kwargs):
        kwargs.setdefault('prog', 'Bitcasa Scraper')
        kwargs.setdefault('description', ('List and download files '
                                          'from bitcasa'))
        super(BitcasaParser, self).__init__(*args, **kwargs)

        self.actions = self.add_subparsers(dest='command',
            parser_class=argparse.ArgumentParser)

        self.create_base_parser()
        self.create_authentication_parser()
        self.create_shell_parser()

    def create_authentication_parser(self):
        self.authentication_parser = self.actions.add_parser('authenticate',
           parents=[self.base_parser], help='Store bitcasa authentication')

    def create_shell_parser(self):
        self.shell_parser = self.actions.add_parser('shell',
           parents=[self.base_parser],
           help='Launch an interactive authenticated shell')

        self.shell_parser.add_argument('--noauth', dest='auth',
           action='store_false', default=True,
           help='Do not attempt authentication')

        self.shell_parser.add_argument('--pdb', dest='pdb',
           action='store_true', help='Go into a pdb trace on error')

    def create_base_parser(self):
        self.base_parser = argparse.ArgumentParser(add_help=False)
        self.base_parser.set_defaults(auth=True, pdb=False)

        self.base_parser.add_argument('-u', '--username', dest='username',
            required=True, help='The username/email for authentication')

        self.base_parser.add_argument('-p', '--password', dest='password',
            required=True, help='The password for authentication')

        self.base_parser.add_argument('-c', '--config', dest='config_file',
            type=argparse.FileType('a+'), default='bitcasa.ini', nargs='?',
            help='Path to the config file. ')

        self.base_parser.add_argument('-v', '--verbose', dest='verbose',
            action='count')


class BitcasaDrive(object):
    config = None
    root = None

    def __init__(self, args=None, auto_fetch_root=True):
        self.config = args
        if auto_fetch_root:
            self.fetch_drive()

    def fetch_drive(self, retry=True):
        root_meta = connection.request(BITCASA.ENDPOINTS.root_folder)
        self.root = BitcasaFolder.from_meta_data(root_meta['result'])

    def list(self, auto_fetch_drive=True, retry=True):
        if not (auto_fetch_drive or self.root):
            raise BitcasaError('Root not fetched')

        if not self.root and auto_fetch_drive:
            self.fetch_drive()

        return self.root.list(retry=retry)

