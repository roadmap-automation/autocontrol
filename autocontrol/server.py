import bluesky_api
from flask import Flask
from flask import request
from threading import Thread
import time

app = Flask(__name__)
# shutdown signal
app_shutdown = False


def background_task():
    """
    Flask server background task comprising an infinite loop executing one task of the Bluesky queue at a time.
    :return: No return value.
    """
    while not app_shutdown:
        # Try to execute one item from the bluesky queue.
        # If all resources are busy or the queue is empty, the method returns without doing anything.
        # We do not need to keep track of this here and will just reattempt again until the server is stopped.
        bsa.queue_execute_one_item()
        # sleep for some time before
        time.sleep(1)


def shutdown_server(wait_for_queue_to_empty=False):
    """
    Helper function for gracefully shutting down the Flask server.

    :param wait_for_queue_to_empty: Boolean, whether to wait for the Bluesky API to process all tasks in the queue.
    :return: Status string.
    """
    global app_shutdown

    # TODO: Add a shut down task into the queue (will two shutdown tasks be a problem?)

    while wait_for_queue_to_empty:
        if bsa.queue.empty():
            break
        time.sleep(10)

    # stop background thread
    app_shutdown = True
    while bg_thread.is_alive():
        time.sleep(10)

    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError('Not running with the Werkzeug Server')
    func()

    return 'Server shut down.'


@app.route('/')
def index():
    """
    Function routed to the default URL, mostly for ensuring that the Flask server started.

    :return: Status string
    """
    return 'Bluesky Flask Server Started!'


@app.route('/put', methods=['POST'])
def put_queue():
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

    dfields = ['task', 'channel', 'md', 'sample_number', 'task_type', 'device']
    if not set(dfields).issubset(data):
        return 'Error, not all datafields provided.'

    # put request in bluesky queue
    bsa.queue_put(task=data['task'], channel=data['channel'], md=data['md'], sample_number=data['sample_number'],
                  task_type=data['task_type'], device=data['device'])

    return 'Request succesfully enqueued.'


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
        response = shutdown_server()
    else:
        response = shutdown_server(wait_for_queue_to_empty=data['wait_for_queue_to_empty'])
    return response


if __name__ == '__main__':
    # initialize bluesky API
    bsa = bluesky_api.autocontrol()

    # start the background thread
    bg_thread = Thread(target=background_task, daemon=True)
    bg_thread.start()

    # run the Flas app
    app.run(host='0.0.0.0', port=5003)


"""
import requests

url = "https://example.com/api"
data = {"name": "John", "age": 25}

response = requests.post(url, data=data)

print(response.status_code)
print(response.json())

"""
