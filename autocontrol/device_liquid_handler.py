import json
import requests
import time as ttime

from status import Status


class lh_device:
    """
    This class implements a liquid handler device interface for autocontrol.
    """

    def __init__(self, name="liquid handler", address=None):
        self.name = name
        self.address = address
        self.number_of_channels = 1
        self.channel_mode = None

        # hard-coded test flag
        self.test = True

    def communicate(self, command, value=0):
        """
        Communicate with liquid handler and return response.

        :param command: HTTP POST request command field
        :param value: HTTP POST request value field
        :return: response from HTTP POST or None if failed
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

    def execute_task(self, task):
        if task['task_type'] == 'init':
            status = self.init(task)
            return status

        if task['task_type'] == 'prepare':
            status = self.prepare(task)
            return status

        if task['task_type'] == 'transfer':
            status = self.transfer(task)
            return status

        return Status.INVALID

    def get_channel_status(self, channel):
        """
        Retrieves the status of a channel.
        :param channel: 	(int) default=0, the channel to be used.
        :return status: 	(Status) Status.IDLE or Status.BUSY
        """
        if self.test:
            return Status.IDLE

        # TODO: Implement for device
        return Status.TODO

    def get_device_status(self):
        """
        Retrieves the status of a device independent of its channels
        :return status: (Status) Status.UP, Status.DOWN, Status.ERROR, Status.INVALID
        """
        if self.test:
            return Status.UP

        # TODO: Implement for device
        return Status.TODO

    def init(self, task):
        self.channel_mode = None

        if self.test:
            if 'number_of_channels'in task['task']:
                noc = task['task']['number_of_channels']
                if noc is None or noc < 2:
                    noc = 1
                else:
                    noc = int(noc)
            else:
                noc = 1
            self.number_of_channels = noc
            return Status.SUCCESS

        # TODO: Implement device initialization
        #  number of channels from task['channel']
        #  any other variables from the task['task'] dictionary
        #  self.communicate can be used or modified for communication with the qcmd device
        self.address = task['device_address']
        self.number_of_channels = task['channel']
        self.channel_mode = task['channel_mode']
        return Status.TODO

    def prepare(self, task):
        if self.test:
            ttime.sleep(5)
            return Status.SUCCESS

        # TODO: Implement prepare
        #  channel to store preparation from task['channel']
        #  any other variables including recipe from the task['md] dictionary
        #  self.communicate can be used or modified for communiction with the qcmd device
        #  make sure to mark the channel of task['device'] as busy during operation

        # if liquid handler is busy, do not start new measurement
        status = self.get_device_status()
        if status != Status.UP:
            return Status.ERROR
        status = self.communicate("start")
        return Status.TODO

    def transfer(self, task):
        if self.test:
            ttime.sleep(5)
            return Status.SUCCESS

        # TODO: Implement transfer
        #  channel from task['channel']
        #  target channel from task['target_channel']
        #  any other variables from the task['md] dictionary
        #  self.communicate can be used or modified for communiction with the qcmd device
        #  make sure to mark the channel of task['device'] and target channel of task['target_device'] as busy during
        #  operation

        # TODO: How does one best mark the target channel as busy as it resides in a different device?
        #   -> discuss with David

        # if liquid handler is busy, do not start new measurement
        status = self.get_device_status()
        if status != Status.UP:
            return Status.ERROR
        status = self.communicate("start")
        return Status.TODO