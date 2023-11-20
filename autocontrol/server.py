import bluesky_api
from flask import Flask
from flask import request
import flask.signals
import json
import sys
import threading
from threading import Thread
import time

app = Flask(__name__)
# shutdown signal
app_shutdown = False


def background_task():
    while not app_shutdown:
        # Try to execute one item from the bluesky queue.
        # If all resources are busy or the queue is empty, the method returns without doing anything.
        # We do not need to keep track of this here and will just reattempt again until the server is stopped.
        bsa.execute_one_item()
        # sleep for some time before
        time.sleep(1)


def shutdown_server(wait_for_queue_to_empty=False):
    global app_shutdown

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


@app.route('/bss/')
def index():
    return 'Server Started!'


@app.route('/bss/put', methods=['POST'])
def put_queue():
    data = request.get_json()
    if data is None:
        result = {'No data received.': True}
        return json.dumps(result)

    dfields = ['sample', 'measurement_channel', 'md', 'sample_number', 'item_type', 'device']
    if not set(dfields).issubset(data):
        result = {'Not all datafields present.': True}
        return json.dumps(result)

    # put request in bluesky queue
    bsa.queue_put(sample=data['sample'], measurement_channel=data['measurement_channel'], md=data['md'],
                  sample_number=data['sample_number'], item_type=data['item_type'], device=data['device'])

    result = {'Request succesfully enqueued.': True}
    return json.dumps(result)


@app.route('/bss/shutdown', methods=['POST'])
def stop_server():
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
    app.run(debug=True)


"""
import requests

url = "https://example.com/api"
data = {"name": "John", "age": 25}

response = requests.post(url, data=data)

print(response.status_code)
print(response.json())

"""
