import json
import re
import traceback

from ConfigParser import ConfigParser, Error

from .exceptions import ConfigError
from .models import SimpleObject


class Config(SimpleObject):
    _keys = []

    def __contains__(self, key):
        return hasattr(self, key)

class ConfigManager(object):

    def __init__(self, cli_args, config_file=None):
        cli_args = cli_args.__dict__.copy()

        self.config = Config(**self.get_defaults())
        print self.config.__dict__
        self.cli_args = cli_args

        cli_config_file = cli_args.pop('config_file', None)
        self.config_file = (config_file or cli_config_file or
                            self.config.config_file)
        if self.config_file:
            data = self.read_config()
            self.config.update_data(**data)
            print self.config.__dict__

        updated = dict([(key, val) for key, val in cli_args.items()
                        if key not in self.config or val is not None])
        self.config.update_data(**updated)
        print self.config.__dict__

    @classmethod
    def get_defaults(cls):
        defaults = dict(auth=True, pdb=False, move_workers=4, list_workers=4,
                        download_workers=4,max_depth=1, cookie_file='./cookies',
                        chunk_size=1024*1024, config_file='./bitcasa.ini',
                        quiet=False, download_folder='./downloads',
                        sqlite_uri='sqlite:///bitcasajobs.sqlite')
        return defaults

    def _read_sections(self, config):
        data = {}
        for section in config.sections():
            for key, val in config.items(section):
                dash_key = key.replace('-', '_')
                if val.lower() in ['true', 'false']:
                    val = config.getboolean(section, key)
                elif re.match(r'^[0-9]+$', val):
                    val = config.getint(section, key)

                data[dash_key] = val

        return data

    def read_config(self):
        config = ConfigParser()
        config.read(self.config_file)

        try:
            data = self._read_sections(config)
        except Error as err:
            traceback.print_exc()
            raise ConfigError('Error reading config')

        return data

    def get_config(self):
        return self.config
