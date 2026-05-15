import autocontrol.atc as autocontrol_atc
from flask import Flask
from flask import abort, request
import json
import os
from threading import Thread
from typing import Optional
from autocontrol.task_struct import Task
from autocontrol.broker_worker import BrokerWorker
import time
from werkzeug.serving import run_simple

app = Flask(__name__)
# shutdown signal
app_shutdown = False
# intialize global variables
atc: Optional[autocontrol_atc.autocontrol] = None
broker: Optional[BrokerWorker] = None
bg_thread: Optional[Thread] = None


def background_task():

    """
    Flask server background task comprising an infinite loop executing one task of the priority queue at a time.

    Completion detection is broker-driven: device modules publish task.completed / task.failed events which the
    BrokerWorker consumer places in broker.completion_events.  This loop drains that queue and calls
    atc.post_process_task() instead of polling device HTTP endpoints.

    :return: No return value.
    """
    global atc, broker

    while not app_shutdown:
        wait_time = 5

        # Drain device completion events from the broker consumer (replaces atc.update_active_tasks() polling)
        while not broker.completion_events.empty():
            try:
                task_id, status, envelope = broker.completion_events.get_nowait()
            except Exception:
                break
            task = atc.active_tasks.get_task_by_id(task_id)
            if task is None:
                continue
            if status == "completed":
                if atc.post_process_task(task):
                    broker.publish_task_completed(task)
                    _publish_channel_events_after_completion(task)
                    wait_time = 0.1
            else:
                error = envelope.payload.get("error", "Device reported failure.")
                broker.publish_task_failed(task, error)
                wait_time = 0.1

        # Try to execute one item from the scheduling queue. If all resources are busy or the queue is empty,
        # the method does nothing. atc.task_dispatch_hook fires inside queue_execute_one_item() and handles
        # the broker publish + audit log for each dispatched subtask.
        if not atc.paused:
            try:
                if atc.queue_execute_one_item():
                    wait_time = 0.1
            except Exception:
                import logging
                logging.getLogger(__name__).exception(
                    "Unhandled exception in queue_execute_one_item(); dispatch loop continuing."
                )

        broker.wakeup.wait(timeout=wait_time)
        broker.wakeup.clear()


def _publish_channel_events_after_completion(task: Task) -> None:
    """Publish channel lock/release broker events mirroring the channel_po changes made in post_process_task."""
    from autocontrol.task_struct import TaskType
    if task.task_type == TaskType.TRANSFER:
        src = task.tasks[0]
        if src.channel is not None:
            broker.publish_channel_released(src.device, src.channel)
        if len(task.tasks) > 1:
            dst = task.tasks[-1]
            if dst.channel is not None:
                broker.publish_channel_locked(dst.device, dst.channel)
    elif task.task_type in (TaskType.PREPARE, TaskType.MEASURE):
        subtask = task.tasks[0]
        if subtask.channel is not None:
            broker.publish_channel_locked(subtask.device, subtask.channel)


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

@app.route('/get_subtask_results/<task_id>/<subtask_id>', methods=['GET'])
def get_subtask_results(task_id, subtask_id):
    """
    Returns the execution data of a subtask with id <subtask_id> from a task with id <task_id> in the history.
    :return: dictionary {'result': any
                         }
    """

    if task_id is None:
        abort(400, description='No task id provided.')
    if atc is None:
        abort(400, description="No autocontrol instance found.")

    task = atc.sample_history.get_task_by_id(task_id)

    retval = {}
    if task is None:
        abort(400, description="Task is not found or not yet complete.")

    subtask = next((s for s in task.tasks if str(s.id) == subtask_id), None)

    if subtask is None:
        abort(400, description="Subtask not found")

    retval = subtask.md.get('task_execution_data', {})

    return json.dumps(retval)


@app.route('/')
def index():
    """
    Function routed to the default URL, mostly for ensuring that the Flask server started.

    :return: Status string
    """
    return 'Autocontrol Flask Server Started!'


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

    # initialize autocontrol API
    global atc, broker
    atc = autocontrol_atc.autocontrol(storage_path=storage_path)

    # Start broker worker and wire up hooks
    broker = BrokerWorker(atc)

    # Enable broker mode on each device as it is initialized so that
    # standard_task() skips the HTTP POST.
    def _on_device_created(device_object) -> None:
        device_object.broker_mode = True

    atc.device_created_hook = _on_device_created

    # task_dispatch_hook is called by process_task() after each successful
    # subtask dispatch and publishes command.<device_id>.submit_task + audit log.
    atc.task_dispatch_hook = broker.task_dispatch_hook

    broker.start()

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

