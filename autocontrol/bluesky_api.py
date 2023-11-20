import os
import sys
import threading
import time
from bluesky.plan_stubs import mv
from bluesky.plan_stubs import read
from bluesky.plans import count
import bluesky.plan_stubs as bps
import bluesky.preprocessors as bpp
from bluesky import RunEngine
from bluesky.callbacks.best_effort import BestEffortCallback
from event_model import RunRouter
from databroker import Broker
import math
from queue import PriorityQueue
from QCMD_device import open_QCMD
from liquid_handler_device import lh_device
import warnings


def bec_factory(doc):
    # Each run is subscribed to independent instance of BEC
    bec = BestEffortCallback()
    return [bec], []


class autocontrol:
    def __init__(self, num_meas_channels=1):

        # accepts only sample preparations
        self.prepare_only = False

        # currently executed measurements
        self.execution_list = []

        self.RE = RunEngine({})
        self.db = Broker.named('temp')
        self.RE.subscribe(self.db.insert)

        # dynamic call back subscription
        rr = RunRouter([bec_factory])
        self.RE.subscribe(rr)

        self.lh = lh_device(name="lh", labels={"motors"})

        self.qcmd = open_QCMD("QCMD", labels={"detectors"})
        self.qcmd.exposure_time = 1
        self.qcmd.address = "http://localhost:5011/QCMD/"

        self.queue = PriorityQueue()

    # creates call backs for every run

    def queue_put(self, sample=None, measurement_channel=None, md=None, sample_number=None, item_type='prepare',
                  device='QCMD'):
        """
        Puts an item into the priority queue, which is either the preparation or the measurement of a sample.
        :param sample: A description of the sample that can be passed to the LH or potentially stored as md.
        :param measurement_channel: Integer, which measurement channel to use.
        :param md: Any metadata to attach to the run.
        :param sample_number: Sample number for current Bluesky run. The lower, the higher the priority.
        :param item_type: Distinguishes between 'init' for instrument initialization
                                                'prepare' for sample preparation
                                                'measure' for measurement.
                                                'shut down' for instrument shut down
                                                'exit' for ending main loop
        :param device: Measurement device, one of the following: 'LH': liquid handler
                                                                 'QCMD': QCMD
                                                                 'NR': neutron reflectometer
        :return: no return value
        """

        # create a priority value with the following importance
        # 1. Sample number
        # 2. Time that step was submitted
        # convert time to a priority <1
        p1 = time.time()/math.pow(10, math.ceil(math.log10(time.time())))
        # convert sample number to priority, always overriding start time.
        priority = sample_number * (-1.)
        priority -= p1

        item = (
            priority,
            {'sample': sample,
             'channel': measurement_channel,
             'meta': md,
             'type': item_type,
             'device': device,
             'priority': priority}
        )
        self.queue.put_nowait(item)

    def queue_get(self, prepare_only=False):
        """
        Retrieves an item from the preparation queue.
        :param prepare_only: Flag that indicates whether only preparation methods ought to be retrieved.
        :return: Data field of the retrieved item or None if none retrieved
        """
        def get_highest_priority_prepare_item(queue):
            # create a new priority queue
            queue2 = PriorityQueue()

            all_items = [item for item in queue.queue]
            # create a list of all items in the queue where 'step' is 'prepare'
            prepare_items = [item for item in queue.queue if item[1]['type'] == 'prepare']

            # sort the prepare_items list in descending order of priority value
            prepare_items.sort(key=lambda item: item[1]['priority'], reverse=True)

            # return the data dict of the first item in the sorted list, if it exists and create a new list without it
            if prepare_items:
                for item in all_items:
                    # priority is a unique identifier
                    if item[1]['priority'] != prepare_items[0][1]['priority']:
                        queue2.put_nowait(item)
                return queue2, prepare_items[0][1]
            else:
                return queue, None

        if self.queue.empty():
            return None

        if prepare_only:
            self.queue, item = get_highest_priority_prepare_item(self.queue)
        else:
            item = self.queue.get_nowait()

        return item

    def check_for_finished_measurements(self, devicename, device):
        ret_value = []
        for item in self.execution_list:
            if item['device'] != devicename:
                continue
            # TODO: needs implementation
            if device.val.get_channel_status(item['channel']) == 'available':
                # measurement is done, remove item from list
                # TODO: double-check that data readout works
                self.execution_list.remove(item)
            else:
                # mark channel as in use
                ret_value.append(item['channel'])
        return ret_value

    def execute_one_item(self):

        job = self.queue_get(prepare_only=self.prepare_only)
        if job is None:
            return

        item = job[1]

        # Generate unique key for each run. The key generation algorithm
        # must only guarantee that execution of the runs that are assigned
        # the same key will never overlap in time.
        run_key = f"run_key_{str(item['priority']*(-1))}"
        if item['device'] == 'LH':
            device = self.lh
            plan = self.lh_plan
        elif item['device'] == 'QCMD':
            device = self.qcmd
            plan = self.qcmd_plan
        else:
            return

        if item['type'] == 'init':
            # TODO implement consistently
            device.init()
            return

        if item['type'] == 'shut down':
            # TODO implement consistently
            device.shutdown()
            return

        # test if device is free
        # TODO: Implement status for accepting new commands
        if device.val.status != '':
            # instrument busy, return job to queue
            self.queue_put(job)
            return

        busy_measurement_channels = self.check_for_finished_measurements(item['device'], device)
        if job['channel'] in busy_measurement_channels:
            # We are currently not attempting to find an available channel
            # TODO: define the behavior and implement it (autoselect channels?)
            self.queue_put(job)
            return

        sample = item['sample']
        channel = item['channel']
        # TODO handle meta data, here and in plan functions, data types
        meta = item['md']
        yield from bpp.set_run_key_wrapper(plan(sample=sample, measurement_channel=channel), run_key)

        # if the item is a measurement
        if item['type'] == 'measure':
            self.execution_list.append(item)

    @bpp.run_decorator(md={})
    def lh_plan(self, sample=None, measurement_channel=None):
        """
        Bluesky measurement plan for the liquid handler as implemented in liquid_handler_device.py

        :param sample:
        :param measurement_channel:
        :return:
        """

        md = {'sample': sample,
              'measurement_channel': measurement_channel,
              'start_time': time.gmtime(time.time())}
        yield from mv(self.lh, [sample, measurement_channel], md=md)

    @bpp.run_decorator(md={})
    def qcmd_plan(self, sample='', measurement_channel=0, md=None):
        """
        Bluesky measurement plan for a QCMD device as implemented in QCMD_device.py

        :param sample: (optional) sample description, will be stored with the metadata
        :param measurement_channel: (optional, default=0) the measurement channel to be used
        :param md: (optional) metadata to be stored with the measurement
        :return: no return value
        """

        def generate_new_key(base_key, dictionary):
            new_key = base_key
            counter = 1
            while new_key in dictionary:
                new_key = f"{base_key}_{counter}"
                counter += 1
            return new_key

        def adjust_keys(keylist, dictionary):
            for key in keylist:
                if key in dictionary:
                    new_key = generate_new_key(key, md)
                    md[new_key] = md.pop(key)

        # The QCMD device does not require the sample description for its measurement. It is only included in the
        # metadata
        md2 = {'sample': sample,
               'measurement_channel': measurement_channel,
               'start_time': time.gmtime(time.time())}
        # merge with additional metadata
        if md is not None:
            # make sure that neither of the
            adjust_keys(list(md2.keys()), md)
            md3 = {**md, **md2}
        else:
            md3 = md2

        yield from count([self.qcmd], md=md3)



