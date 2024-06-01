import autocontrol.server as server
import json
import multiprocessing
import os
import platform
import psutil
import requests
import signal
import socket
import subprocess
import time


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


def stop(portnumber=5004, wait_for_queue_to_empty=True):
    print('\n')
    print('Stopping Flask')
    url = 'http://localhost:' + str(portnumber) + '/shutdown'
    headers = {'Content-Type': 'application/json'}
    data = json.dumps({'wait_for_queue_to_empty': wait_for_queue_to_empty})
    response = requests.post(url, headers=headers, data=data)
    return response


def get_task_status(task_id, port):
    print('\n')
    print('Requesting status for task ID: ' + str(task_id) + '\n')
    url = 'http://localhost:' + str(port) + '/get_task_status'
    headers = {'Content-Type': 'application/json'}
    data = {'task_id': str(task_id)}
    response = requests.get(url, headers=headers, params=data)
    # print(response, response.text)
    return response


def submit_task(task, port):
    print('\n')
    print('Submitting Task: ' + task.tasks[0].device + ' ' + task.task_type + 'Sample: ' + str(task.sample_id) + '\n')
    url = 'http://localhost:' + str(port) + '/put'
    headers = {'Content-Type': 'application/json'}
    data = task.json()
    response = requests.post(url, headers=headers, data=data)
    print(response, response.text)
    return response


def terminate_processes():
    # Get the current platform
    current_platform = platform.system()

    if current_platform == "Windows":
        # On Windows, use psutil to iterate over child processes and terminate them
        current_process = psutil.Process(os.getpid())
        children = current_process.children(recursive=True)
        for child in children:
            child.terminate()
        gone, still_alive = psutil.wait_procs(children, timeout=3)
        for p in still_alive:
            p.kill()
    else:
        # On Unix-based systems, use os.killpg to terminate the process group
        pgid = os.getpgid(os.getpid())
        os.killpg(pgid, signal.SIGTERM)


if __name__ == '__main__':
    port = 5004
    start(portnumber=port)
    # Wait for user input
    print("Autocontrol started.")
    _ = input("Please enter some text and press Enter to stop autocontrol ")
    stop(portnumber=port)
