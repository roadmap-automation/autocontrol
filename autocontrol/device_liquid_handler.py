from device import Device
import json
import requests
import time as ttime

from status import Status


class lh_device(Device):
    """
    This class implements a liquid handler device interface for autocontrol.
    """

    def __init__(self, name="liquid handler", address=None):
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

        # TODO: Implement device initialization -> see documentation
        self.address = task['device_address']
        self.number_of_channels = task['channel']
        self.channel_mode = task['channel_mode']
        return Status.TODO

    def prepare(self, task):
        if self.test:
            ttime.sleep(5)
            return Status.SUCCESS

        # TODO: Implement prepare -> see documentation

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

        # TODO: Implement transfer -> see documentation
        # TODO: How does one best mark the target channel as busy as it resides in a different device?
        #   -> discuss with David

        # if liquid handler is busy, do not start new measurement
        status = self.get_device_status()
        if status != Status.UP:
            return Status.ERROR
        status = self.communicate("start")
        return Status.TODO