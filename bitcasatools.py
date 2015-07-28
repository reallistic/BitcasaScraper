import sys
if 'threading' in sys.modules:
    raise Exception('threading module loaded before patching!')

import gevent.monkey
gevent.monkey.patch_all()


pyssl_version = (2, 7, 9)
cur_version = sys.version_info
if cur_version < pyssl_version:
    import urllib3.contrib.pyopenssl
    urllib3.contrib.pyopenssl.inject_into_urllib3()

import time
import traceback

from bdb import BdbQuit

from bitcasa import (BitcasaParser, ConfigManager, drive, connection_pool,
                     list_folder, scheduler)

from bitcasa.connection import setup_connection
from bitcasa.drive import setup_drive
from bitcasa.logger import setup_logger, setup_scheduler_loggers
from bitcasa.jobs import setup_scheduler

args = None

def main():
    global args

    config = None

    try:
        parser = BitcasaParser()
        args = parser.parse_args()
        config = ConfigManager(args).get_config()
        cli_args = args

        setup_logger('BitcasaConsole', config=config)
        setup_scheduler_loggers(config=config)
        setup_connection(config=config)
        setup_scheduler(config=config)
        scheduler.start()

        if config.auth:
            if args.command in ['shell']:
                setup_drive(config=config)
            else:
                setup_drive(config=config, auto_fetch_root=False)
    except BdbQuit:
        pass
    except:
        if config and config.pdb:
            traceback.print_exc()
            import pdb; pdb.set_trace()
        else:
            raise

    if args.command == 'shell':
        import code
        code.interact(local=dict(drive=drive, config=config, pool=connection_pool,
                                 scheduler=scheduler, cli_args=args))

    if args.command == 'list':
        items = list_folder.async(max_depth=config.max_depth, print_files=True)

        executor = scheduler._lookup_executor('list')
        executor.wait()

    if args.command == 'authenticate':
        message = 'Username and password must be specified'
        assert all((config.username, config.password)), message
        connection_pool._store_cookies(config.cookie_file)

if __name__ == '__main__':
    try:
        main()
    except Exception as err:
        if not isinstance(err, (SystemExit, KeyboardInterrupt, BdbQuit)):
            traceback.print_exc()
    finally:
        try:
            if not connection_pool.using_cookie_file:
                connection_pool.logout()
        except AssertionError:
            pass
