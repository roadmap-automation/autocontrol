import os
from autocontrol.support import support

if __name__ == '__main__':
    support.start(portnumber=5004, storage_path=None)
    # Wait for user input
    _ = input("Please enter some text and press Enter to stop server and all processes: ")