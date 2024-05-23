import json
import multiprocessing
import os
import requests
import task_struct as tsk
import server
import signal
import socket
import subprocess
import time
import uuid

port = 5004

def start_streamlit_viewer():
    viewer_path = os.path.join(os.path.dirname(__file__), 'viewer.py')
    result = subprocess.run(['streamlit', 'run', viewer_path])

def start():
    print('Preparing test directory')
    cfd = os.path.dirname(os.path.abspath(__file__))
    storage_path = os.path.join(cfd, '..', 'test')
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
    server.start_server(host='localhost', port=port, storage_path=storage_path)

    print('Waiting for 2 seconds.')
    time.sleep(5)

    # ------------------ Starting Streamlit Monitor----------------------------------
    print("Starting Streamlit Viewer")
    process = multiprocessing.Process(target=start_streamlit_viewer)
    process.start()


if __name__ == '__main__':
    start()
    # UNIX-style termination of all child processes including the test Flask server
    # Get the process group ID of the current process
    pgid = os.getpgid(os.getpid())
    # Send a SIGTERM signal to the process group to terminate all child processes
    os.killpg(pgid, signal.SIGTERM)