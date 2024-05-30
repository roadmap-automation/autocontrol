from autocontrol.status import Status
from autocontrol.device import Device
import json
import time as ttime
import requests


class open_QCMD(Device):
    def read(self, channel=None):
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

        return super().read(channel)

    def transfer(self, subtask):
        if self.test:
            super().transfer(subtask)

        # The QCMD is a passive device concering transfer. There is no effect of a transfer on any status variable.
        return Status.SUCCESS, ''


if __name__ == '__main__':
    open_QCMD1 = open_QCMD(name="Open QCMD", address="http://localhost:5011/QCMD/")
