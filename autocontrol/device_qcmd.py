import json
import time
import requests
from status import Status


class open_QCMD:

    def __init__(self, name="Open QCMD", address=None):
        self.name = name
        self.address = address
        self.number_of_channels = 1
        self.channel_mode = None

        # hard-coded test flag
        self.test = True

    def communicate(self, command, value=0):
        """
        Communicate with QCM-D instrument and return response.

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

        if task['task_type'] == 'measure':
            status = self.measure(task)
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

    def measure(self, task):
        if self.test:
            time.sleep(5)
            return Status.SUCCESS

        # TODO: Implement measurement start
        #  channel from task['channel']
        #  any other variables from the task['md] dictionary
        #  self.communicate can be used or modified for communiction with the qcmd device
        # if QCMD is busy, do not start new measurement
        status = self.get_device_status()
        if status != Status.UP:
            return Status.ERROR
        status = self.communicate("start")
        return Status.TODO

    def read(self, channel=None):
        """
        Establishes an HTTP connection to the QCMD Qt app and retrieves the current data. With the current thinking, it
        is the entire data set collected since start. Thereby, qcmd_read should be called only once after stopping the
        run.

        :return: QCMD data as a dictionary
        """

        # single-tone dummy data for null returns
        ddict = {
            'time': [0., 10., 20., 30.],
            'frequency': [0., -1., -2., -3.],
            'dissipation': [100., 200., 300., 400.],
            'temperature': [300., 300., 300., 300.]
        }

        if self.test:
            return Status.SUCCESS, ddict

        status1 = self.communicate("stop")
        rdict = self.communicate("get_data")
        if rdict is None:
            rdict = ddict

        return Status.SUCCESS, rdict


if __name__ == '__main__':
    open_QCMD1 = open_QCMD(name="Open QCMD", address="http://localhost:5011/QCMD/")
