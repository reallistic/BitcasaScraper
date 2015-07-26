import sys
pyssl_version = (2, 7, 9)
cur_version = sys.version_info
if cur_version < pyssl_version:
    import urllib3.contrib.pyopenssl
    urllib3.contrib.pyopenssl.inject_into_urllib3()

import traceback

from bitcasa import BitcasaDrive, BitcasaParser

from bitcasa.authentication import AuthenticationManager
from bitcasa.globals import (connection, connect, set_connection_params,
                             setup_logger)

def main():
    setup_logger('BitcasaConsole')
    parser = BitcasaParser()
    args = parser.parse_args()
    drive = None
    if args.auth:
        try:
            set_connection_params(args.username, args.password, AuthenticationManager)
            connect()
            drive = BitcasaDrive()
        except Exception as err:
            if args.pdb:
                traceback.print_exc()
                cl_args = args
                import pdb; pdb.set_trace()
            else:
                raise

    if args.command == 'shell':
        import code
        code.interact(local=dict(drive=drive, connection=connection,
                                 args=args))
    if args.command == 'list':
        drive.root.list_items()


if __name__ == '__main__':
    main()


