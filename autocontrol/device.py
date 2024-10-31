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

        # test flag
        self.test = simulated

        # Passive devices cannot actively perform a transfer, as they are only pass-through.
        # Consequently, they cannot be the first device in a transfer chain. Sample occupancy checks
        # are disabled, because any material will be pushed through and out.
        self.passive = False

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
                #print('GET request to {} with {}'.format(url, data))
                response = requests.get(url, headers=headers, data=data)
                #print('Here is the response: ', response.text)
            else:
                return Status.INVALID, 'Invalid HTTP method specified'
        except requests.exceptions.RequestException:
            #print('Exception occurred')
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
        request_status, _, channel_status = self.get_device_and_channel_status()

        if len(channel_status) <= channel:
            return Status.ERROR, None

        return request_status, channel_status[channel]

    def get_device_status(self):
        """
        Retrieves the status of a device independent of its channels
        :return status: (Status, Status) request and device status
        """
        request_status, device_status, _ = self.get_device_and_channel_status()
        return request_status, device_status

    def get_device_and_channel_status(self):
        """
        Retrieves the status of a device and its channels
        :return: (Status, Status, [Status]) request status, device status, list of channel status
        """
        if self.test:
            # temporary hack for testing
            if self.name == 'qcmd1':
                return Status.SUCCESS, Status.IDLE, [Status.BUSY, Status.IDLE]
            return Status.SUCCESS, Status.IDLE, [Status.IDLE] * self.number_of_channels

        request_status, device_and_channel_status = self.get_status()
        if request_status != Status.SUCCESS:
            return request_status, None, None

        device_status = autocontrol.status.get_status_member(device_and_channel_status['status'])
        if device_status is None:
            device_status = Status.ERROR

        channel_status_list = device_and_channel_status['channel_status']
        for i, channel_status in enumerate(channel_status_list):
            channel_status_list[i] = autocontrol.status.get_status_member(channel_status)
            if channel_status_list[i] is None:
                channel_status_list[i] = Status.ERROR

        return request_status, device_status, channel_status_list

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

    def standard_task(self, subtask, endpoint='/SubmitTask'):
        if self.test:
            return self.standard_test_response(subtask)

        request_status, device_status = self.get_device_status()
        if request_status != Status.SUCCESS:
            response = 'Cannot get device status for {}.'.format(self.name)
            return Status.ERROR, response
        if device_status != Status.IDLE:
            response = 'Device {} is not idle.'.format(self.name)
            return Status.ERROR, response
        status, ret = self.communicate(endpoint, subtask.json())
        return status, ret

    def standard_test_response(self, subtask):
        if self.test:
            ttime.sleep(5)
            return Status.SUCCESS, ''

        return Status.INVALID, ''

    def transfer(self, subtask):
        return self.standard_task(subtask)
