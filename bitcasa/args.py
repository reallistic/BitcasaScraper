import argparse

class BitcasaParser(argparse.ArgumentParser):

    def __init__(self, *args, **kwargs):
        kwargs.setdefault('prog', 'Bitcasa Scraper')
        kwargs.setdefault('description', ('List and download files '
                                          'from bitcasa'))
        super(BitcasaParser, self).__init__(*args, **kwargs)

        self.actions = self.add_subparsers(dest='command',
            parser_class=argparse.ArgumentParser)

        self.create_base_parser()
        self.create_iobase_parser()
        self.create_authentication_parser()
        self.create_action_parsers()
        self.create_shell_parser()

    def create_authentication_parser(self):
        self.authentication_parser = self.actions.add_parser('authenticate',
            parents=[self.base_parser], help='Store bitcasa authentication')

    def create_action_parsers(self):
        self.list_parser = self.actions.add_parser('list',
            parents=[self.base_parser, self.iobase_parser],
            help='List the root of your bitcasa drive')

        self.download_parser = self.actions.add_parser('download',
            parents=[self.base_parser, self.iobase_parser],
            help='Recursively download your bitcasa drive')

        self.download_parser.add_argument('--download-workers',
            dest='download_workers', type=int,
            help=('The number of workers that can download at one time. '
                  '(default: 4)'))

        self.download_parser.add_argument('--move-workers',
            dest='move_workers', type=int,
            help=('The number of workers that can move at one time. '
                  '(default: 4)'))

        self.download_parser.add_argument('--download-folder',
            dest='download_folder',
            help=('The base folder for downloaded files. '
                  '(default: ./downloads)'))

        self.download_parser.add_argument('--move-to',
            dest='move_to',
            help='The base folder to move completed downloads')

    def create_shell_parser(self):
        self.shell_parser = self.actions.add_parser('shell',
            parents=[self.base_parser],
            help='Launch an interactive authenticated shell')

        self.shell_parser.add_argument('--noauth', dest='auth',
            action='store_false', help='Do not attempt authentication')

        self.shell_parser.add_argument('--pdb', dest='pdb',
            action='store_true', help='Go into a pdb trace on error')

    def create_iobase_parser(self):
        self.iobase_parser = argparse.ArgumentParser(add_help=False)

        self.iobase_parser.add_argument('-f', '--bitcasa-folder',
            dest='bitcasa_folder',
            help='Set the base64 folder path to start list/download')

        self.iobase_parser.add_argument('--sqlite-uri', dest='sqlite_uri',
            help=('sqlite connection string for SQLAlchemy to connect. '
                  '(default: sqlite:///bitcasajobs.sqlite'))

        self.iobase_parser.add_argument('--list-workers',
            dest='list_workers', type=int,
            help=('How many workers will traverse folders at the same time. '
                  '(default: 4)'))

        self.iobase_parser.add_argument('-d', '--max-depth', dest='max_depth',
            type=int, help='The maximum folder traversal depth. (default: 1)')

    def create_base_parser(self):
        self.base_parser = argparse.ArgumentParser(add_help=False)

        self.base_parser.add_argument('-u', '--username', dest='username',
            help='The username/email for authentication')

        self.base_parser.add_argument('-p', '--password', dest='password',
            help='The password for authentication')

        self.base_parser.add_argument('--chunk-size', type=int,
            dest='chunk_size',
            help='Size in bytes to download at a time. (default: 1048576)')

        self.base_parser.add_argument('-n', '--connections', type=int,
            dest='max_connections',
            help='The maximum number of connections to make to bitcasa')

        self.base_parser.add_argument('--cookie-file', dest='cookie_file',
            nargs='?', help='Path to the cookie file. (default: ./cookies)')

        self.base_parser.add_argument('-c', '--config', dest='config_file',
            nargs='?', help='Path to the config file. (default: ./bitcasa.ini)')

        self.base_parser.add_argument('--log-file', dest='log_file',
            nargs='?', help='Path to the log file. (default: no logging)')

        self.base_parser.add_argument('-q', '--quiet', dest='quiet',
            action='store_true',
            help='Quiet mode: Disables console logging.')

        self.base_parser.add_argument('-v', '--verbose', dest='verbose',
            action='count', help='Increase verbosity')
