import sys
if 'threading' in sys.modules:
    raise Exception('threading module loaded before patching!')

import gevent.monkey
gevent.monkey.patch_all()

import traceback
from bdb import BdbQuit

from bitcasa import BitcasaDriveApp, current_app

def main():
    try:
        app = BitcasaDriveApp()
    except (SystemExit, KeyboardInterrupt, BdbQuit):
        return
    except:
        traceback.print_exc()
        return

    with app.get_context():
        current_app.run()

if __name__ == '__main__':
    try:
        main()
    except:
        pass
