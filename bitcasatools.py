import sys
pyssl_version = (2, 7, 9)
cur_version = sys.version_info
if cur_version < pyssl_version:
    import urllib3.contrib.pyopenssl
    urllib3.contrib.pyopenssl.inject_into_urllib3()

import traceback

from bitcasa import BitcasaParser
from bitcasa.authentication import AuthenticationManager
from bitcasa.connection import ConnectionPool
from bitcasa.drive import BitcasaDrive
from bitcasa.globals import setup_logger

pool = None

def main():
    global pool
    drive = None

    parser = BitcasaParser()
    args = parser.parse_args()
    setup_logger('BitcasaConsole', config=args)

    if args.auth:
        try:
            pool = ConnectionPool(config=args)
            drive = BitcasaDrive(config=args, connection_pool=pool)
        except Exception as err:
            if args.pdb:
                traceback.print_exc()
                cl_args = args
                import pdb; pdb.set_trace()
            else:
                raise

    if args.command == 'shell':
        import code
        code.interact(local=dict(drive=drive, args=args, pool=pool))
    if args.command == 'list':
        items = drive.root.list()
        for item in items:
            print item.name, item.id


if __name__ == '__main__':
    try:
        main()
    finally:
        pool.logout()
