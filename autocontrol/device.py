import json
import requests
from task_struct import TaskType
from status import Status


class Device(object):
    def __init__(self, name=None, address=None, simulated=False):
        self.name = name
        self.address = address
        self.number_of_channels = 1
        self.channel_mode = None

        # hard-coded test flag
        self.test = simulated

    def communicate(self, command, value=0):
        """
        Communicate with device and return response.
        :param command: HTTP POST request command field
        :param value: HTTP POST request value field
        :return: status, response from HTTP POST or None if failed
        """
        if self.address is None:
            return Status.INVALID, None
        cmdstr = '{"command": "' + str(command) + '", "value": ' + str(value) + '}'
        try:
            r = requests.post(self.address, cmdstr)
        except requests.exceptions.RequestException:
            return Status.ERROR, None
        if r.status_code != 200:
            return Status.ERROR, None

        rdict = json.loads(r.text)
        response = rdict['result']
        return Status.SUCCESS, response

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
        :return status: 	(Status) Status.IDLE or Status.BUSY
        """
        return Status.INVALID

    def get_device_status(self):
        """
        Retrieves the status of a device independent of its channels
        :return status: (Status) Status.UP, Status.DOWN, Status.ERROR, Status.INVALID
        """
        return Status.INVALID

    def init(self, task):
        return Status.INVALID, ''

    def measure(self, task):
        return Status.INVALID, ''

    def no_channel(self, task):
        return Status.INVALID, ''

    def prepare(self, task):
        return Status.INVALID, ''

    def transfer(self, task):
        return Status.INVALID, ''
