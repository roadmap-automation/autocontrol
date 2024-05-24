import multiprocessing
import os
import autocontrol.server as server
import signal
import socket
import subprocess
import time

port = 5004


def start_streamlit_viewer(storage_path):
    viewer_path = os.path.join(os.path.dirname(__file__), 'viewer.py')
    result = subprocess.run(['streamlit', 'run', viewer_path, '--', '--storage_dir', storage_path])


def start(portnumber=5004, storage_path=None):
    """
    Starts the autocontrol server.
    :param portnumber: port number of the server
    :param storage_path: directory to save task databases
    :return: no return value
    """
    print('Preparing test directory')
    if storage_path is None:
        storage_path = os.path.join(os.getcwd(), "atc_test")
        print("Defaulting to current directory for test directory: {}".format(storage_path))
    else:
        print("Path for test directory is {}".format(storage_path))
    if not os.path.isdir(storage_path):
        os.mkdir(storage_path)

    for filename in os.listdir(storage_path):
        file_path = os.path.join(storage_path, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
        except Exception as e:
            print(f'Failed to delete {file_path}. Reason: {e}')

    hostname = socket.gethostname()
    try:
        IPAddr = socket.gethostbyname(hostname)
        print(f"IP Address of {hostname} is {IPAddr}")
    except socket.gaierror:
        print(f"Could not resolve hostname: {hostname}. Check your network settings.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

    # ------------------ Starting Flask Server----------------------------------
    print("Starting Flask Server")
    server.start_server(host='localhost', port=portnumber, storage_path=storage_path)

    print('Waiting for 2 seconds.')
    time.sleep(5)

    # ------------------ Starting Streamlit Monitor----------------------------------
    print("Starting Streamlit Viewer with storage path: {}".format(storage_path))
    process = multiprocessing.Process(target=start_streamlit_viewer, args=(storage_path,))
    process.start()


if __name__ == '__main__':
    start(portnumber=port)
    # UNIX-style termination of all child processes including the test Flask server
    # Get the process group ID of the current process
    pgid = os.getpgid(os.getpid())
    # Send a SIGTERM signal to the process group to terminate all child processes
    os.killpg(pgid, signal.SIGTERM)
