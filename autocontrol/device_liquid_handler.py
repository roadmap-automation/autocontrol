from device import Device
import time as ttime

from status import Status


class lh_device(Device):
    """
    This class implements a liquid handler device interface for autocontrol.
    """

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
            return Status.IDLE

        # TODO: Implement for device
        return Status.TODO

    def init(self, subtask):
        # TODO: Implement device initialization -> see documentation
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

    def prepare(self, subtask):
        if self.test:
            ttime.sleep(5)
            return Status.SUCCESS, ''

        # TODO: Implement prepare -> see documentation

        # if liquid handler is busy, do not start new measurement
        status = self.get_device_status()
        if status != Status.UP:
            return Status.ERROR, ''
        # status = self.communicate("start")
        return Status.TODO, ''

    def transfer(self, subtask):
        if self.test:
            ttime.sleep(5)
            return Status.SUCCESS, ''

        # TODO: Implement transfer -> see documentation
        #   Do not forget to mark the source or target channel as busy for a channel-based transfer
        #   Mark the entire device as busy for a non-channel based transfer

        # if liquid handler is busy, do not start new measurement
        status = self.get_device_status()
        if status != Status.UP:
            return Status.ERROR, ''

        return Status.TODO, ''
