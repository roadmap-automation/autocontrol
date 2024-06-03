import autocontrol.status
from autocontrol.status import Status
from autocontrol.device import Device
import json
import time as ttime


class injection_device(Device):
    """
    This class implements a injection device interface for autocontrol.
    """
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

    def init(self, subtask):
        if self.test:
            return super().init(subtask)

        self.address = subtask.device_address
        self.channel_mode = subtask.channel_mode

        # injection devices have two hard-coded channels
        if subtask.number_of_channels is not None and subtask.number_of_channels != 2:
            return Status.INVALID, 'Number of channels must be 2 for an injection device.'
        self.number_of_channels = subtask.number_of_channels if subtask.number_of_channels is not None else 2

        return Status.SUCCESS, 'Injection device initialized.'

    def standard_task(self, subtask, endpoint='/SubmitTask'):
        return super().standard_task(subtask, endpoint)

