import os
import autocontrol.support
from multiprocessing import freeze_support

if __name__ == '__main__':
    cfd = os.path.dirname(os.path.abspath(__file__))
    storage_path = os.path.join(cfd, '..', 'test_storage')
    autocontrol.support.start(portnumber=5004, storage_path=storage_path)