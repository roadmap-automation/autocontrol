import autocontrol
from flask import Flask
from flask import request
import json
from pydantic import ValidationError
from threading import Thread
from typing import Optional
from task import Task
import time
from werkzeug.serving import run_simple

app = Flask(__name__)
# shutdown signal
app_shutdown = False
# intialize global variables
atc: Optional[autocontrol.autocontrol] = None
bg_thread: Optional[Thread] = None


def background_task():
    """
    Flask server background task comprising an infinite loop executing one task of the Bluesky queue at a time.
    :return: No return value.
    """
    while not app_shutdown:
        # check on all active tasks and handle if they are finished
        atc.update_active_tasks()
        # Try to execute one item from the bluesky queue.
        # If all resources are busy or the queue is empty, the method returns without doing anything.
        # We do not need to keep track of this here and will just reattempt again until the server is stopped.
        atc.queue_execute_one_item()
        # sleep for some time before
        time.sleep(10)


def shutdown_server(wait_for_queue_to_empty=False):
    """
    Helper function for gracefully shutting down the Flask server.

    :param wait_for_queue_to_empty: Boolean, whether to wait for the Bluesky API to process all tasks in the queue.
    :return: Status string.
    """
    global app_shutdown

    # TODO: Add a shut down task into the queue (will two shutdown tasks be a problem?)

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


@app.route('/')
def index():
    """
    Function routed to the default URL, mostly for ensuring that the Flask server started.

    :return: Status string
    """
    return 'Bluesky Flask Server Started!'


@app.route('/queue_inspect', methods=['GET'])
def queue_inspect():
    """
    Retrieves all queue items without removing them from the queue and prints them in the terminal.
    :return: (str) status
    """
    queue_items = atc.queue_inspect()
    print('Inspecting queue...')
    print('Received {} queued items.'.format(len(queue_items)))
    for number, item in enumerate(queue_items):
        print('Item ' + str(number+1) + ', Priority ' + str(item['priority']) + ': ')
        formatted_item = json.dumps(item, indent=4)
        print(formatted_item)

    return 'Queue successfully printed.'


@app.route('/put', methods=['POST'])
def queue_put():
    """
    POST request function that puts one task onto the Bluesky priorty queue.

    The POST data must contain the following data fields:
    'task':  (task.Task) The task.

    The queue is automatically processed by a background task of the Flask server. Tasks are executed by their priority.
    The priority is a combination of sample number and submission time. A higher priority is given to samples with lower
    sample number and earlier submission. Measurement tasks that are preparations can bypass higher priority measurement
    tasks. Sample numbers are derived from the sample_id upon first submission

    :return: Status string.
    """

    if request.method != 'POST':
        return 'Error, request method is not POST.'

    data = request.get_json()
    if data is None or not isinstance(data, dict):
        return 'Error, no valid data received.'

    # de-serialize the task data into a Task object
    try:
        task = Task(**data)
        # put request in autocontrol queue
        atc.queue_put(task=task)
    except ValidationError as e:
        print("Failed to deserialize:", e)
        return 'Failed to submit task'

    return 'Request succesfully enqueued.'


def start_server(host='0.0.0.0', port=5003, storage_path=None):
    def app_start():
        run_simple('localhost', port, app)

    # initialize bluesky API
    global atc
    atc = autocontrol.autocontrol(storage_path=None)

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
        return 'Error, request method is not POST.'

    data = request.get_json()
    if 'wait_for_queue_to_empty' not in data:
        print('Shutting down server without waiting for queue.')
        response = shutdown_server()
    else:
        print('Shutting down server after waiting for queue to empty.')
        response = shutdown_server(wait_for_queue_to_empty=data['wait_for_queue_to_empty'])
    return response


if __name__ == '__main__':
    start_server(host='0.0.0.0', port=5003)


"""
import requests

url = "https://example.com/api"
data = {"name": "John", "age": 25}

response = requests.post(url, data=data)

print(response.status_code)
print(response.json())

"""
