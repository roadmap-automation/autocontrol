import device_api
from flask import Flask
from flask import request
import json
from threading import Thread
from typing import Optional
import time
from werkzeug.serving import run_simple

app = Flask(__name__)
# shutdown signal
app_shutdown = False
# intialize global variables
dev_api: Optional[device_api.autocontrol] = None
bg_thread: Optional[Thread] = None


def background_task():
    """
    Flask server background task comprising an infinite loop executing one task of the Bluesky queue at a time.
    :return: No return value.
    """
    while not app_shutdown:
        # check on all active tasks and handle if they are finished
        dev_api.update_active_tasks()

        # Try to execute one item from the bluesky queue.
        # If all resources are busy or the queue is empty, the method returns without doing anything.
        # We do not need to keep track of this here and will just reattempt again until the server is stopped.
        dev_api.queue_execute_one_item()
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
        if dev_api.queue.empty() and dev_api.active_tasks.empty():
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
    queue_items = dev_api.queue_inspect()
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
    'task':                 (dict) A dictionary describing the task to be executed by the instrument. This field will be
                            passed on to the instrument API.
    'sample_number':        (int) An ascending sample ID.
    'channel':              (int) Channel to be used in case parallel measurements are supported.
    'md':                   (dict) Metadata to be saved with the measurement data.
    'task_type':            (str) A generic label for different types of tasks affecting how they are prioritized.
                            Options:
                            'init', 'prepare', 'transfer', 'measure', 'shut down', 'exit'
    'device':               (str) Name of the device executing the task.

    For transfer tasks, additionally the following data fields are required:
    'target_device':        (str) The name of the device the materialed is transferred to;
    'target_channel':       (int) The channel on the target device to be used, auto-select if None.

    The queue is automatically processed by a background task of the Flask server. Tasks are executed by their priority.
    The priority is a combination of sample number and submission time. A higher priority is given to samples with lower
    sample number and earlier submission. Measurement tasks that are preparations can bypass higher priority measurement
    tasks.

    :return: Status string.
    """

    if request.method != 'POST':
        return 'Error, request method is not POST.'

    data = request.get_json()
    if data is None or not isinstance(data, dict):
        return 'Error, no valid data received.'

    # check for mandatory data fields
    dfields = ['task', 'sample_number', 'task_type', 'device']
    if not set(dfields).issubset(data):
        return 'Error, not all mandatory datafields provided.'

    # add defaults for non-mandatory fields:
    if 'md' not in data:
        data['md'] = {}
    if 'channel' not in data:
        data['channel'] = None
    if 'target_device' not in data:
        data['target_device'] = None
    if 'target_channel' not in data:
        data['target_channel'] = None

    # put request in bluesky queue
    dev_api.queue_put(task=data['task'], channel=data['channel'], md=data['md'], sample_number=data['sample_number'],
                      task_type=data['task_type'], device=data['device'], target_device=data['target_device'],
                      target_channel=data['target_channel'])

    return 'Request succesfully enqueued.'


def start_server(host='0.0.0.0', port=5003, storage_path=None):
    def app_start():
        run_simple('localhost', port, app)

    # initialize bluesky API
    global dev_api
    dev_api = device_api.autocontrol(storage_path=None)

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
