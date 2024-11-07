from autocontrol.status import Status
from autocontrol.device import Device
import json
import time as ttime
import requests


class open_QCMD(Device):
    def __init__(self, name=None, address=None, simulated=False):
        super().__init__(name, address, simulated)
        # QCMD is a passive device
        self.passive = True

    def init(self, subtask):
        if self.test:
            return super().init(subtask)

        self.address = subtask.device_address
        self.channel_mode = subtask.channel_mode

        # injection devices have two hard-coded channels
        if subtask.number_of_channels is not None and subtask.number_of_channels != 2:
            return Status.INVALID, 'Number of channels must be 2 for a qcmd device.'
        self.number_of_channels = subtask.number_of_channels if subtask.number_of_channels is not None else 2

        return Status.SUCCESS, 'Qcmd device initialized.'

    def get_status(self):
        """
        Communicates with the device to determine its status.
        :return: status of the request, status dictionary from the device if successful
        """
        status, ret = self.communicate('/GetStatus', method='GET')
        if status == Status.SUCCESS:
            try:
                retdict = json.loads(ret)
            except json.JSONDecodeError:
                status = Status.ERROR
                retdict = None
        else:
            retdict = None

        return status, retdict

    def read(self, channel=None, subtask_id=None):
        """
        Establishes an HTTP connection to the QCMD Qt app and retrieves the current data. With the current thinking, it
        is the entire data set collected since start. Thereby, qcmd_read should be called only once after stopping the
        run.

        :return: QCMD data as a dictionary
        """
        if self.test:
            # single-tone dummy data for null returns
            ddict = {
                'time': [0., 10., 20., 30.],
                'frequency': [0., -1., -2., -3.],
                'dissipation': [100., 200., 300., 400.],
                'temperature': [300., 300., 300., 300.]
            }
            return Status.SUCCESS, ddict

        return super().read(channel, subtask_id)

    def standard_task(self, subtask, endpoint='/SubmitTask'):
        return super().standard_task(subtask, endpoint)


if __name__ == '__main__':
    open_QCMD1 = open_QCMD(name="Open QCMD", address="http://localhost:5011/QCMD/")
