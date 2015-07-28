import json
import traceback

from ConfigParser import ConfigParser, Error

from .exceptions import ConfigError
from .models import SimpleObject


class Config(SimpleObject):
    _keys = []

class ConfigManager(object):

    def __init__(self, cli_args, config_file=None):
        cli_args = cli_args.__dict__.copy()

        self.config = Config(**cli_args)
        self.cli_args = cli_args

        cli_config_file = cli_args.pop('config_file', None)
        self.config_file = config_file or cli_config_file
        if self.config_file:
            data = self.read_config()
            self.config.update_data(**data)

    def _read_sections(self, config):
        data = {}
        for section in config.sections():
            for key, val in config.items(section):
                if key in ['cookie_file']:
                    val = open(val, 'a+')
                if key in ['pdb', 'auth']:
                    val = config.getboolean(section, key)
                if key in ['chunk_size', 'max_connections', 'max_depth',
                           'verbose']:
                    val = config.getint(section, key)

                data[key] = val

        return data

    def read_config(self):
        config = ConfigParser()
        self.config_file.seek(0)
        config.read(self.config_file.name)

        try:
            data = self._read_sections(config)
        except Error as err:
            traceback.print_exc()
            raise ConfigError('Error reading config')

        return data

    def get_config(self):
        return self.config
