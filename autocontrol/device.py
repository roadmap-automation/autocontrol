import autocontrol.status
from autocontrol.task_struct import TaskType
from autocontrol.status import Status
import requests
import time as ttime


class Device(object):
    def __init__(self, name=None, address=None, simulated=False):
        self.name = name
        self.address = address
        self.number_of_channels = 1
        self.channel_mode = None

        # hard-coded test flag
        self.test = simulated

    def communicate(self, command, data=None, method='POST'):
        """
        Communicate with device and return response via HTTP POST or GET. Can be replaced by subclasses.

        :param command: HTTP request command field
        :param data: HTTP request data for POST or parameters for GET
        :param method: HTTP method ('POST' or 'GET')
        :return: status, response from HTTP request or None if failed
        """

        if self.address is None:
            return Status.INVALID, 'No address for device.'

        url = self.address + command
        headers = {'Content-Type': 'application/json'}

        try:
            if method.upper() == 'POST':
                response = requests.post(url, headers=headers, data=data)
            elif method.upper() == 'GET':
                response = requests.get(url, headers=headers, params=data)
            else:
                return Status.INVALID, 'Invalid HTTP method specified'
        except requests.exceptions.RequestException:
            return Status.ERROR, 'Exception occurred while communicating with device.'

        if response.status_code != 200:
            return Status.ERROR, response.text

        return Status.SUCCESS, response.text

    def execute_task(self, task, task_type):
        """
        Routes tasks to the appropriate subroutines
        :param task: task to execute
        :param task_type (tsk.TaskType)
        :return: autocontrol status
        """
        if task_type == TaskType.INIT:
            status, resp = self.init(task)
            return status, resp

        if task_type == TaskType.MEASURE:
            status, resp = self.measure(task)
            return status, resp

        if task_type == TaskType.PREPARE:
            status, resp = self.prepare(task)
            return status, resp

        if task_type == TaskType.TRANSFER:
            status, resp = self.transfer(task)
            return status, resp

        if task_type == TaskType.NOCHANNEL:
            status, resp = self.no_channel(task)
            return status, resp

        return Status.INVALID, "Do not recognize task type."

    def get_channel_status(self, channel):
        """
        Retrieves the status of a channel.
        :param channel: 	(int) default=0, the channel to be used.
        :return status: 	(Status) channel status
        """
        if self.test:
            return Status.IDLE

        request_status, device_status = self.get_status()
        if request_status != Status.SUCCESS:
            return Status.ERROR

        channel_status = device_status['channel_status']
        if len(channel_status) <= channel:
            return Status.ERROR

        ret = autocontrol.status.get_status_member(device_status[channel])
        if ret is None:
            ret = Status.ERROR

        return ret

    def get_device_status(self):
        """
        Retrieves the status of a device independent of its channels
        :return status: (Status) device status
        """
        if self.test:
            return Status.IDLE

        request_status, device_status = self.get_status()
        if request_status != Status.SUCCESS:
            return Status.ERROR

        ret = autocontrol.status.get_status_member(device_status['status'])
        if ret is None:
            ret = Status.ERROR
        return ret

    def get_status(self):
        """
        Placeholder for device-specific status retrieval functions.
        :return: status of the request, status dictionary from the device if successful
        """
        return Status.TODO, {}

    def init(self, subtask):
        self.address = subtask.device_address
        self.channel_mode = subtask.channel_mode

        # generic response for testing
        if self.test:
            if subtask.number_of_channels is not None:
                noc = subtask.number_of_channels
                if noc is None or noc < 2:
                    noc = 1
                else:
                    noc = int(noc)
            else:
                noc = 1
            self.number_of_channels = noc
            return Status.SUCCESS, 'Simulated device initialized.'

        return Status.INVALID, 'Method not implemented'

    def measure(self, subtask):
        return self.standard_task(subtask)

    def no_channel(self, subtask):
        return self.standard_task(subtask)

    def prepare(self, subtask):
        return self.standard_task(subtask)

    def read(self, channel=None):
        """
        If a device has no read function, this method provides compatibility to submit measurement methods that
        will yield an empty readout. Thereby, processing steps or wait steps on a per-channel basis can be implemented.

        :return: empty dictionary
        """

        ddict = {}
        return Status.SUCCESS, ddict

    def standard_task(self, subtask):
        if self.test:
            return self.standard_test_response(subtask)

        return Status.INVALID, 'Method not implemented.'

    def standard_test_response(self, subtask):
        if self.test:
            ttime.sleep(5)
            return Status.SUCCESS, ''

        return Status.INVALID, ''

    def transfer(self, subtask):
        return self.standard_task(subtask)
