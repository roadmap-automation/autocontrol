from device import Device
import json
import time as ttime
import requests
from status import Status


class open_QCMD(Device):

    def __init__(self, name="Open QCMD", address=None):
        super().__init__(name, address)

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

    def init(self, subtask):
        self.address = subtask.device_address
        self.number_of_channels = subtask.number_of_channels
        self.channel_mode = subtask.channel_mode

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
            return Status.SUCCESS, ''

        # TODO: Implement device initialization
        #  number of channels from task['channel']
        #  any other variables from the task['task'] dictionary
        #  self.communicate can be used or modified for communication with the qcmd device

        return Status.TODO, ''

    def measure(self, subtask):
        if self.test:
            ttime.sleep(5)
            return Status.SUCCESS, ''

        # TODO: Implement measurement -> see documentation
        # if QCMD is busy, do not start new measurement
        status = self.get_device_status()
        if status != Status.UP:
            return Status.ERROR, ''
        if subtask.acquisition_time is not None:
            acquisition_time = subtask.acquisition_time
        # status = self.communicate("start")
        return Status.TODO, ''

    def no_channel(self, subtask):
        if self.test:
            ttime.sleep(5)
            return Status.SUCCESS, ''

        # TODO: Implement a channel-less task -> see documentation
        #   Make sure to set the entire device to BUSY during task execution and back to UP when done.

        status = self.get_device_status()
        if status != Status.UP:
            return Status.ERROR, ''

        return Status.TODO, ''

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

    def transfer(self, subtask):
        if self.test:
            ttime.sleep(5)
            return Status.SUCCESS, ''

        # The QCMD is a passive device concering transfer. There is no effect of a transfer on any status variable.
        return Status.SUCCESS, ''


if __name__ == '__main__':
    open_QCMD1 = open_QCMD(name="Open QCMD", address="http://localhost:5011/QCMD/")
