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
    print('Submitting Task: ' + task['device'] + ' ' + task['task_type'] + 'Sample: ' + str(task['sample_number']) +
          '\n')
    url = 'http://localhost:' + str(port) + '/put'
    headers = {'Content-Type': 'application/json'}
    data = json.dumps(task)
    response = requests.post(url, headers=headers, data=data)


def integration_test():
    print('Starting integration test')

    print('Preparing test directory')
    storage_path = '../test/'
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

    # ------------------ Submitting Task ----------------------------------
    task = {
        'task': {'description': 'QCMD init',
                 'number_of_channels': 1},
        'sample_number': 0,
        'channel': None,
        'md':  {},
        'task_type': 'init',
        'device': 'qcmd'
    }
    submit_task(task)
    time.sleep(5)

    task = {
        'task': {'description': 'lh init',
                 'number_of_channels': 10},
        'sample_number': 0,
        'channel': None,
        'md':  {},
        'task_type': 'init',
        'device': 'lh'
    }
    submit_task(task)
    time.sleep(5)

    task = {
        'task': {'description': 'Sample1 preparation'},
        'sample_number': 1,
        'channel': None,
        'md':  {},
        'task_type': 'prepare',
        'device': 'lh'
    }
    submit_task(task)
    time.sleep(5)

    task = {
        'task': {'description': 'Sample2 preparation'},
        'sample_number': 2,
        'channel': None,
        'md':  {},
        'task_type': 'prepare',
        'device': 'lh'
    }
    submit_task(task)
    time.sleep(5)

    task = {
        'task': {'description': 'Sample1 transfer'},
        'sample_number': 1,
        'channel': None,
        'md':  {},
        'task_type': 'transfer',
        'device': 'lh',
        'target_channel': None,
        'target_device': 'qcmd'
    }
    submit_task(task)
    time.sleep(5)

    task = {
        'task': {'description': 'Sample2 transfer',
                 'force': True
                 },
        'sample_number': 2,
        'channel': None,
        'md':  {},
        'task_type': 'transfer',
        'device': 'lh',
        'target_channel': None,
        'target_device': 'qcmd'
    }
    submit_task(task)
    time.sleep(5)

    task = {
        'task': {'description': 'QCMD measurement'},
        'sample_number': 1,
        'channel': None,
        'md':  {},
        'task_type': 'measure',
        'device': 'qcmd'
    }
    submit_task(task)
    time.sleep(5)

    task = {
        'task': {'description': 'QCMD measurement'},
        'sample_number': 2,
        'channel': None,
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
    user_input = input("Please enter some text and press Enter to stop all processes: ")


if __name__ == '__main__':
    integration_test()
    # UNIX-style termination of all child processes including the test Flask server
    # Get the process group ID of the current process
    pgid = os.getpgid(os.getpid())
    # Send a SIGTERM signal to the process group to terminate all child processes
    os.killpg(pgid, signal.SIGTERM)
