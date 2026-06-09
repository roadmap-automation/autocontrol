import os
import pathlib
import time
from autocontrol.support import support

DEFAULT_STORAGE_PATH = pathlib.Path(__file__).parent / 'data'

if __name__ == '__main__':
    storage_path = pathlib.Path(os.environ.get('AUTOCONTROL_DATA_DIR', str(DEFAULT_STORAGE_PATH)))
    storage_path.mkdir(parents=True, exist_ok=True)
    support.start(portnumber=5004, storage_path=str(storage_path))
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        support.stop(portnumber=5004, wait_for_queue_to_empty=False)