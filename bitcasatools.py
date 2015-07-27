import sys
pyssl_version = (2, 7, 9)
cur_version = sys.version_info
if cur_version < pyssl_version:
    import urllib3.contrib.pyopenssl
    urllib3.contrib.pyopenssl.inject_into_urllib3()

import traceback

from bitcasa import BitcasaParser, ConfigManager, BitcasaDrive
from bitcasa.logger import setup_logger

pool = None
args = None

def main():
    global pool, args

    drive = None
    config = None

    try:
        parser = BitcasaParser()
        args = parser.parse_args()
        config = ConfigManager(args).get_config()
        cli_args = args

        setup_logger('BitcasaConsole', config=config)

        if config.auth:
            if args.command in ['shell', 'list']:
                drive = BitcasaDrive(config=config)
            else:
                drive = BitcasaDrive(config=config, auto_fetch_root=False)
    except:
        if config and config.pdb:
            traceback.print_exc()
            import pdb; pdb.set_trace()
        else:
            raise

    if args.command == 'shell':
        import code
        code.interact(local=dict(drive=drive, config=config, pool=pool,
                                 cli_args=args))

    if args.command == 'list':
        items = drive.root.list()
        for item in items:
            print item.name, item.id

    if args.command == 'authenticate':
        message = 'Username and password must be specified'
        assert all((config.username, config.password)), message
        pool._store_cookies(config.cookie_file)

if __name__ == '__main__':
    try:
        main()
    except Exception as err:
        if not isinstance(err, (SystemExit, KeyboardInterrupt)):
            traceback.print_exc()
    finally:
        if pool and not pool.using_cookie_file:
            pool.logout()
