import autocontrol.atc as autocontrol_atc
from flask import Flask
from flask import abort, request
import json
import os
from pydantic import ValidationError
from threading import Thread
from typing import Optional
from autocontrol.task_struct import Task
import time
from werkzeug.serving import run_simple

app = Flask(__name__)
# shutdown signal
app_shutdown = False
# intialize global variables
atc: Optional[autocontrol_atc.autocontrol] = None
bg_thread: Optional[Thread] = None


def background_task():
    """
    Flask server background task comprising an infinite loop executing one task of the Bluesky queue at a time.
    :return: No return value.
    """
    global atc

    while not app_shutdown:
        wait_time = 5
        # check on all active tasks and handle if they are finished
        if atc.update_active_tasks():
            # one task was succesfully collected, let's not wait that long until checking queue again
            wait_time = 0.1
        # Try to execute one item from the scheduling queue. If all resources are busy or the queue is empty,
        # the method does nothing. We do not need to keep track of this here and will just reattempt again until
        # the server is stopped.
        if not atc.paused:
            if atc.queue_execute_one_item():
                # one task was succesfully submitted, let's not wait that long until checking queue again
                wait_time = 0.1

        time.sleep(wait_time)

@app.route('/get_task_status/<task_id>', methods=['GET'])
def get_task_status(task_id):
    """
    Identifies the status of a task with id <task_id> in the queue.
    :return: dictionary {'queue': 'scheduled', 'active', 'history',
                         'submission response': (str)
                         'subtasks submission response': [str]
                         }
    """

    if task_id is None:
        abort(400, description='No task id provided.')
    if atc is None:
        abort(400, description="No autocontrol instance found.")

    task_scheduled = atc.queue.get_task_by_id(task_id)
    task_active = atc.active_tasks.get_task_by_id(task_id)
    task_history = atc.sample_history.get_task_by_id(task_id)

    retval = {}
    if task_history is not None:
        task = task_history
        retval['queue'] = 'history'
    elif task_active is not None:
        task = task_active
        retval['queue'] = 'active'
    elif task_scheduled is not None:
        task = task_scheduled
        retval['queue'] = 'scheduled'
    else:
        abort(400, description="No task found.")

    if task.md is not None and 'submission_response' in task.md:
        retval['submission_response'] = task.md['submission_response']
    else:
        retval['submission_response'] = ''

    retval['subtasks_submission_response'] = []
    for subtask in task.tasks:
        if subtask.md is not None and 'submission_response' in subtask.md:
            retval['subtasks_submission_response'].append(subtask.md['submission_response'])
        else:
            retval['subtasks_submission_response'].append('')

    return json.dumps(retval)


@app.route('/')
def index():
    """
    Function routed to the default URL, mostly for ensuring that the Flask server started.

    :return: Status string
    """
    return 'Autocontrol Flask Server Started!'


@app.route('/cancel', methods=['POST'])
def queue_cancel():
    """
    POST request to cancel a submitted task from the autocontrol priority queue.

    The POST data must contain the following data fields:
    'task_id' - the id of the task to cancel as a str

    :return: status string
    """
    if request.method != 'POST':
        abort(400, description='Request method is not POST.')

    data = request.get_json()
    if data is None or not isinstance(data, dict):
        abort(400, description='No valid data received.')

    # de-serialize the POST data into a Task object
    if 'task_id' in data:
        # submit autocontral cancel request
        atc.queue_cancel(task_id=data['task_id'])
        description='Success.'
    else:
        abort(400, description='No task id provided.')

    return description


@app.route('/queue_inspect', methods=['GET'])
def queue_inspect():
    """
    Retrieves all queue items without removing them from the queue and returns them as a dict.
    :return: (dict) formatted
    """
    queue_items = atc.queue_inspect()
    retdict = {}
    for number, item in enumerate(queue_items):
        serialized_task = item.json()
        retdict['task_'+str(number)] = serialized_task
    return retdict


@app.route('/put', methods=['POST'])
def queue_put():
    """
    POST request function that puts one task onto the autocontrol priorty queue.

    The POST data must contain the following data fields:
    'task':  (task.Task) The task.

    The queue is automatically processed by a background task of the Flask server. Tasks are executed by their priority.
    The priority is a combination of sample number and submission time. A higher priority is given to samples with lower
    sample number and earlier submission. Measurement tasks that are preparations can bypass higher priority measurement
    tasks. Sample numbers are derived from the sample_id upon first submission

    :return: Status string.
    """
    retdict = {}
    if request.method != 'POST':
        abort(400, description='Request method is not POST.')

    data = request.get_json()
    if data is None or not isinstance(data, dict):
        abort(400, description='No valid data received.')

    # de-serialize the task data into a Task object
    try:
        task = Task(**data)
        # put request in autocontrol queue
        success, task_id, sample_number, response = atc.queue_put(task=task)
        retdict['task_id'] = task_id
        retdict['sample_number'] = sample_number
        retdict['response'] = response
    except ValidationError:
        abort(400, description='Failed to deserialize task.')

    if not success:
        abort(400, description=response)

    return retdict


@app.route('/pause', methods=['POST'])
def pause():
    """
    POST request function that pauses the scheduling queue.
    :return: Status string
    """
    global atc

    if request.method != 'POST':
        abort(400, description='Request method is not POST.')

    if atc is None:
        abort(400, description="No autocontrol instance found.")

    atc.paused = True

    return 'Paused!'


@app.route('/reset', methods=['POST'])
def reset():
    """
    POST request function that wipes all tasks in all queues and the channel occupancy list.
    :return: no return value
    """
    if request.method != 'POST':
        abort(400, description='Request method is not POST.')
    atc.reset()
    return 'Restarted.'


@app.route('/restart', methods=['POST'])
def restart():
    """
    POST request function that wipes all tasks in all queues and the channel occupancy list.
    :return: no return value
    """
    if request.method != 'POST':
        abort(400, description='Request method is not POST.')
    atc.restart()
    return 'Reset.'


@app.route('/resume', methods=['POST'])
def resume():
    """
    POST request function that resumes the scheduling queue after pausing.
    :return: Status string
    """
    global atc

    if request.method != 'POST':
        abort(400, description='Request method is not POST.')

    if atc is None:
        abort(400, description="No autocontrol instance found.")

    atc.paused = False

    return 'Resumed!'


def shutdown_server(wait_for_queue_to_empty=False):
    """
    Helper function for gracefully shutting down the Flask server.

    :param wait_for_queue_to_empty: Boolean, whether to wait for the Bluesky API to process all tasks in the queue.
    :return: Status string.
    """
    global app_shutdown

    while wait_for_queue_to_empty:
        if atc.queue.empty() and atc.active_tasks.empty():
            break
        time.sleep(10)

    # stop background thread
    app_shutdown = True
    while bg_thread.is_alive():
        time.sleep(10)

    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        # raise RuntimeError('Not running with the Werkzeug Server')
        print('Not running with the Werkzeug Server. Server will shut down with program exit.')
    else:
        func()

    return 'Server shut down.'


def start_server(hostname='localhost', port=5003, storage_path=None):
    def app_start():
        run_simple(hostname, port, app)

    if storage_path is None:
        storage_path = os.getcwd()

    # initialize bluesky API
    global atc
    atc = autocontrol_atc.autocontrol(storage_path=storage_path)

    # start the background thread
    global bg_thread
    bg_thread = Thread(target=background_task, daemon=True)
    bg_thread.start()

    # run the Flask app
    server_thread = Thread(target=app_start, daemon=True)
    server_thread.start()


@app.route('/shutdown', methods=['POST'])
def stop_server():
    """
    POST request function that stops the Bluesky Flask server. The POST data may contain the following datafield:
    'wait_for_queue_to_empty': waits for the Bluesky queue to finish all tasks before shutting down the server.

    :return: status string
    """

    if request.method != 'POST':
        abort(400, description='Request method is not POST.')

    data = request.get_json()
    if 'wait_for_queue_to_empty' not in data:
        print('Shutting down server without waiting for queue.')
        response = shutdown_server()
    else:
        print('Shutting down server after waiting for queue to empty.')
        response = shutdown_server(wait_for_queue_to_empty=data['wait_for_queue_to_empty'])
    return response

