import autocontrol.server as server
from autocontrol.support import configuration

import json
import multiprocessing
import os
from pathlib import Path
import platform
import psutil
import requests
import signal
import socket
import subprocess
import time


def cancel_task(task_id, url=None, port=None):
    if url is None:
        url = url = 'http://localhost:'
    if port is None:
        url = url + '/cancel'
    else:
        url = url + str(port) + '/cancel'

    data = {'task_id': task_id}
    headers = {'Content-Type': 'application/json'}
    response = requests.post(url, headers=headers, data=json.dumps(data))

    return response.json()


def pause_queue(url=None, port=None):
    if url is None:
        url = url = 'http://localhost:'
    if port is None:
        url = url + '/pause'
    else:
        url = url + str(port) + '/pause'

    headers = {'Content-Type': 'application/json'}
    response = requests.post(url, headers=headers)
    return response


def resubmit_task(task_id=None, task=None, url=None, port=None):
    """
    Resubmits a task either from the priority or active queue to the priority queue with the task ID as provided in the
    task_id argument. When providing a task object, it will replace the original one except for the task priority.
    :param task_id: (UUID or str) The ID of the task to resubmit.
    :param task: (Task), optional, The task replacing the original task.
    :param url: (str) The URL of the flask server running autocontrol. Default is 'http://localhost:'
    :param port: (int) The port of the flask server running autocontrol.

    :return: A dictionary containing the submission response, task id and sample id.
    """
    if url is None:
        url = url = 'http://localhost:'
    if port is None:
        url = url + '/resubmit'
    else:
        url = url + str(port) + '/resubmit'

    data = {'task_id': task_id}
    if task is not None:
        data['task'] = task.json()
    headers = {'Content-Type': 'application/json'}
    response = requests.post(url, headers=headers, data=json.dumps(data))

    return response.json()


def resume_queue(url=None, port=None):
    if url is None:
        url = url = 'http://localhost:'
    if port is None:
        url = url + '/resume'
    else:
        url = url + str(port) + '/resume'

    headers = {'Content-Type': 'application/json'}
    response = requests.post(url, headers=headers)
    return response


def start_streamlit_viewer(storage_path, server_address, server_port):
    viewer_path = Path(__file__).parent.parent / 'streamlit' / 'Main.py'
    server_addr = server_address + ':' + str(server_port)
    if storage_path is not None:
        _ = subprocess.run(['streamlit', 'run', str(viewer_path), '--', '--storage_dir', storage_path, '--atc_address',
                            server_addr],)
    else:
        _ = subprocess.run(['streamlit', 'run', str(viewer_path), '--', '--atc_address', server_addr], )


def start(portnumber=5004, storage_path=None, delete_contents=False):
    """
    Starts the autocontrol server. If a storage path is provided, this path will be used to store the autocontrol
    files. If not, the File System dialog of the autocontrol app should be used to set a storage path, which will
    be the selected Experiment folder / 'autocontrol' by default.
    :param portnumber: (int) port number of the server
    :param storage_path: (str | Path) directory to save task databases
    :param delete_contents: (bool) whether to delete contents from the storage folder
    :return: no return value
    """
    print('Checking autocontrol storage directory ...')
    if storage_path is None:
        print("No Autocontrol storage path provided. Use Autocontrol File System Tab to select directory.")
    else:
        storage_path = Path(storage_path).expanduser().resolve()
        print("Autocontrol storage Path provided: {}".format(storage_path))
        storage_path.mkdir(parents=True, exist_ok=True)

        if delete_contents:
            (storage_path / 'active_queue.sqlite3').unlink(missing_ok=True)
            (storage_path / 'history_queue.sqlite3').unlink(missing_ok=True)
            (storage_path / 'priority_queue.sqlite3').unlink(missing_ok=True)
            (storage_path / 'channel_po.json').unlink(missing_ok=True)

    # ------------------ Starting Streamlit Monitor----------------------------------
    # set flag in autocontrol configuration that startup is in progress (gives the user the chance to potentially
    # select an autocontrol storage directory
    config = configuration.load_persistent_cfg()
    config.autocontrol_startup = True
    configuration.save_persistent_cfg(config)

    print("Starting Streamlit Viewer with storage path: {}".format(storage_path))
    process = multiprocessing.Process(target=start_streamlit_viewer, args=(storage_path, 'http://localhost',
                                                                           portnumber))
    process.start()

    # ------------------ Starting Flask Server----------------------------------
    # waiting for the autocontrol app to release the startup flag
    sleep_counter = 0
    while True:
        time.sleep(2)
        sleep_counter += 1
        config = configuration.load_persistent_cfg()
        if not config.autocontrol_startup:
            storage_path=config.autocontrol_dir
            break
        if sleep_counter % 10 == 0:
            print("Waiting for the Autocontrol App to authorize server startup ...")

    hostname = socket.gethostname()
    try:
        IPAddr = socket.gethostbyname(hostname)
        print(f"IP Address of {hostname} is {IPAddr}")
    except socket.gaierror:
        print(f"Could not resolve hostname: {hostname}. Check your network settings.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

    print("Starting Flask Server ...")
    server.start_server(hostname='localhost', port=portnumber, storage_path=storage_path)


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
    url = 'http://localhost:' + str(port) + '/get_task_status/' + str(task_id)
    response = requests.get(url)
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
    return response.json()


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
