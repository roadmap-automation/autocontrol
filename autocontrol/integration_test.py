import autocontrol.task_struct as tsk
import autocontrol.start
import json
import multiprocessing
import os
import platform
import psutil
import requests
import server
import signal
import socket
import subprocess
import time
import uuid

port = 5004


def print_queue():
    print('\n')
    print('Current Queue')
    url = 'http://localhost:' + str(port) + '/queue_inspect'
    headers = {'Content-Type': 'application/json'}
    data = json.dumps({})
    response = requests.get(url, headers=headers, data=data)


def start_streamlit_viewer(storage_path):
    viewer_path = os.path.join(os.path.dirname(__file__), 'viewer.py')
    result = subprocess.run(['streamlit', 'run', viewer_path, '--', '--storage_dir', storage_path])


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
    cfd = os.path.dirname(os.path.abspath(__file__))
    storage_path = os.path.join(cfd, '..', 'test')

    # ----------- Starting Flask Server and Streamlit Viewer ---------------------------
    autocontrol.start.start(portnumber=port, storage_path=storage_path)

    # ------------------ Submitting Task ----------------------------------

    task = tsk.Task(
        task_type=tsk.TaskType('init'),
        tasks=[tsk.TaskData(
            device='qcmd1',
            device_type='qcmd',
            device_address='https:hereitcomes',
            number_of_channels=1,
            simulated=True,
            md={'description': 'QCMD init'}
        )]
    )
    submit_task(task)
    time.sleep(5)

    task = tsk.Task(
        task_type=tsk.TaskType('init'),
        tasks=[tsk.TaskData(
            device='lh1',
            device_type='lh',
            device_address='https:hereitcomes',
            number_of_channels=10,
            simulated=True,
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
            device='lh1',
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
            device='lh1',
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
                device='lh1',
                md={'description': 'Sample1 transfer'}
            ),
            tsk.TaskData(
                device='qcmd1',
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
                device='lh1',
                md={'description': 'Sample2 transfer'}
            ),
            tsk.TaskData(
                device='qcmd1',
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
            device='qcmd1',
            md={'description': 'QCMD measurement sample1'}
        )]
    )
    submit_task(task)
    time.sleep(5)

    task = tsk.Task(
        sample_id=sample_id2,
        task_type=tsk.TaskType('measure'),
        tasks=[tsk.TaskData(
            device='qcmd1',
            md={'description': 'QCMD measurement sample2'}
        )]
    )
    submit_task(task)
    time.sleep(5)

    task = tsk.Task(
        sample_id=sample_id1,
        task_type=tsk.TaskType('nochannel'),
        tasks=[tsk.TaskData(
            device='lh1',
            md={'description': 'lh rinse'}
        )]
    )
    submit_task(task)
    time.sleep(5)

    # ------------------ Stopping Flask Server ----------------------------------
    autocontrol.start.stop(portnumber=port)
    time.sleep(5)

    print('Integration test done.')
    print('Program exit.')

    # Wait for user input
    _ = input("Please enter some text and press Enter to stop all processes: ")


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
    integration_test()
    terminate_processes()
