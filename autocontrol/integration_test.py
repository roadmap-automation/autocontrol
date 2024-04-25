import json
import multiprocessing
import os
import requests
import task as tsk
import server
import signal
import socket
import subprocess
import time
import uuid

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
    print('Submitting Task: ' + task.tasks[0].device + ' ' + task.task_type + 'Sample: ' + str(task.sample_id) + '\n')
    url = 'http://localhost:' + str(port) + '/put'
    headers = {'Content-Type': 'application/json'}
    data = task.json()
    response = requests.post(url, headers=headers, data=data)
    print(response)


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

    task = tsk.Task(
        task_type=tsk.TaskType('init'),
        tasks=[tsk.TaskData(
            device='qcmd',
            number_of_channels=1,
            md={'description': 'QCMD init'}
        )]
    )
    submit_task(task)
    time.sleep(5)

    task = tsk.Task(
        task_type=tsk.TaskType('init'),
        tasks=[tsk.TaskData(
            device='lh',
            number_of_channels=10,
            md={'description': 'lh init'}
        )]
    )
    submit_task(task)
    time.sleep(5)

    sample_id1 = uuid.uuid4()
    task = tsk.Task(
        sample_id=sample_id1,
        task_type=tsk.TaskType('prepare'),
        tasks=[tsk.TaskData(
            device='lh',
            md={'description': 'Sample1 preparation'}
        )]
    )
    submit_task(task)
    time.sleep(5)

    sample_id2 = uuid.uuid4()
    task = tsk.Task(
        sample_id=sample_id2,
        task_type=tsk.TaskType('prepare'),
        tasks=[tsk.TaskData(
            device='lh',
            md={'description': 'Sample2 preparation'}
        )]
    )
    submit_task(task)
    time.sleep(5)

    task = tsk.Task(
        sample_id=sample_id1,
        task_type=tsk.TaskType('transfer'),
        tasks=[
            tsk.TaskData(
                device='lh',
                target_device='qcmd',
                md={'description': 'Sample1 transfer'}
            ),
            tsk.TaskData(
                device='qcmd',
                source_device='lh',
                md={'description': 'Sample1 transfer'}
            )
        ]
    )
    submit_task(task)
    time.sleep(5)

    task = tsk.Task(
        sample_id=sample_id2,
        task_type=tsk.TaskType('transfer'),
        tasks=[
            tsk.TaskData(
                device='lh',
                target_device='qcmd',
                md={'description': 'Sample2 transfer'}
            ),
            tsk.TaskData(
                device='qcmd',
                source_device='lh',
                md={'description': 'Sample2 transfer'}
            )
        ]
    )
    submit_task(task)
    time.sleep(5)

    task = tsk.Task(
        sample_id=sample_id1,
        task_type=tsk.TaskType('measure'),
        tasks=[tsk.TaskData(
            device='qcmd',
            md={'description': 'QCMD measurement sample1'}
        )]
    )
    submit_task(task)
    time.sleep(5)

    task = tsk.Task(
        sample_id=sample_id2,
        task_type=tsk.TaskType('measure'),
        tasks=[tsk.TaskData(
            device='qcmd',
            md={'description': 'QCMD measurement sample2'}
        )]
    )
    submit_task(task)
    time.sleep(5)

    task = tsk.Task(
        sample_id=sample_id1,
        task_type=tsk.TaskType('nochannel'),
        tasks=[tsk.TaskData(
            device='lh',
            md={'description': 'lh rinse'}
        )]
    )
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
