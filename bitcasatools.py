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

import traceback
from bdb import BdbQuit

from bitcasa import BitcasaDriveApp, current_app

def main():
    with BitcasaDriveApp().get_context():
        current_app.run()

if __name__ == '__main__':
    try:
        main()
    except (SystemExit, KeyboardInterrupt, BdbQuit):
        pass
    except:
        traceback.print_exc()
