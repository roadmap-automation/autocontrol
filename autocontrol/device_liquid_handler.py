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
        :return status: (Status) Status.UP, Status.DOWN, Status.ERROR, Status.INVALID, Status.BUSY
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

    def no_channel(self, task):
        if self.test:
            ttime.sleep(5)
            return Status.SUCCESS

        # TODO: Implement a channel-less task -> see documentation
        #   Make sure to set the entire device to BUSY during task execution and back to UP when done.

        status = self.get_device_status()
        if status != Status.UP:
            return Status.ERROR

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
        #   Do not forget to mark the source or target channel as busy for a channel-based transfer
        #   Mark the entire device as busy for a non-channel based transfer

        # if liquid handler is busy, do not start new measurement
        status = self.get_device_status()
        if status != Status.UP:
            return Status.ERROR

        if task['device'] == self.name:
            # this device is the source device of the transfer (not exclusive)
            if task['task']['non_channel_source'] is not None:
                # we transfer from a non-channel source
                pass
        if task['target_device'] == self.name:
            # this device is the target device of the transfer (not exclusive)
            if task['task']['non_channel_target'] is not None:
                # we transfer to a non-channel target
                pass
        else:
            return Status.INVALID

        status = self.communicate("start")
        return Status.TODO
