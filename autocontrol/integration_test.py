import json
import multiprocessing
import os
import requests
import server
import signal
import socket
import subprocess
import time

port = 5003


def print_queue():
    print('\n')
    print('Current Queue')
    url = 'http://localhost:' + str(port) + '/queue_inspect'
    headers = {'Content-Type': 'application/json'}
    data = json.dumps({})
    response = requests.get(url, headers=headers, data=data)


def start_streamlit_viewer():
    viewer_path = os.path.join(os.path.dirname(__file__), 'viewer.py')
    result = subprocess.run(['streamlit', 'run', viewer_path])


def submit_task(task):
    print('\n')
    print('Submitting Task')
    url = 'http://localhost:' + str(port) + '/put'
    headers = {'Content-Type': 'application/json'}
    data = json.dumps(task)
    response = requests.post(url, headers=headers, data=data)


def integration_test():
    print('Starting integration test')

    hostname = socket.gethostname()
    IPAddr = socket.gethostbyname(hostname)
    print("The IP address of your machine is:", IPAddr)
    print('\n')

    # ------------------ Starting Flask Server----------------------------------
    print("Starting Flask Server")
    server.start_server(host='localhost', port=port, storage_path='../test/')

    print('Waiting for 2 seconds.')
    time.sleep(5)

    # ------------------ Starting Streamlit Monitor----------------------------------
    print("Starting Streamlit Viewer")
    process = multiprocessing.Process(target=start_streamlit_viewer)
    process.start()

    # ------------------ Submitting Task ----------------------------------
    task = {
        'task': {'description': 'QCMD init'},
        'sample_number': 1,
        'channel': 1,
        'md':  {},
        'task_type': 'init',
        'device': 'qcmd'
    }
    submit_task(task)
    time.sleep(5)

    task = {
        'task': {'description': 'QCMD measurement'},
        'sample_number': 1,
        'channel': 1,
        'md':  {},
        'task_type': 'measure',
        'device': 'qcmd'
    }
    submit_task(task)
    time.sleep(5)

    task = {
        'task': {'description': 'QCMD measurement'},
        'sample_number': 2,
        'channel': 1,
        'md':  {},
        'task_type': 'measure',
        'device': 'qcmd'
    }
    submit_task(task)
    time.sleep(5)

    # ------------------ Stopping Flask Server ----------------------------------
    print('\n')
    print('Stopping Flask')
    url = 'http://localhost:' + str(port) + '/shutdown'
    headers = {'Content-Type': 'application/json'}
    data = json.dumps({'wait_for_queue_to_empty': True})
    response = requests.post(url, headers=headers, data=data)
    time.sleep(5)

    print('Integration test done.')
    print('Program exit.')

    # Wait for user input
    user_input = input("Please enter some text and press Enter to stop all process: ")


if __name__ == '__main__':
    integration_test()
    # UNIX-style termination of all child processes including the test Flask server
    # Get the process group ID of the current process
    pgid = os.getpgid(os.getpid())
    # Send a SIGTERM signal to the process group to terminate all child processes
    os.killpg(pgid, signal.SIGTERM)
