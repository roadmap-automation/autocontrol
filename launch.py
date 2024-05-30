import autocontrol.start
from multiprocessing import freeze_support

if __name__ == '__main__':
    freeze_support()
    autocontrol.start.start(portnumber=5004, storage_path=None)