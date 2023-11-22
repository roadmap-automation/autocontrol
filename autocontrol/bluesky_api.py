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
from threading import Lock
import warnings


def bec_factory(doc):
    # Each run is subscribed to independent instance of BEC
    bec = BestEffortCallback()
    return [bec], []


def generate_new_dict_key(base_key, dictionary):
    """
    Helper function that iteratively modifies a key name of a dictionary until it finds one that is not used.
    :param base_key: The original key to be renamed.
    :param dictionary: The dictionary containing the key.
    :return: A name suggestions for a new key name.
    """
    new_key = base_key
    counter = 1
    while new_key in dictionary:
        new_key = f"{base_key}_{counter}"
        counter += 1
    return new_key


def merge_dict(dict1=None, dict2=None):
    """
    Helper function that performes a conflict free merging of two dictionaries. It renames keys in dictionary 2 without
    key name conflict.
    :param dict1: The dictionary whose keys will remain the same.
    :param dict2: The dictionary containing the keys that will be renamed.
    :return: The merged dictionary.
    """
    if dict1 is None and dict2 is None:
        return None
    if dict1 is None:
        return dict2
    if dict2 is None:
        return dict1

    for key in list(dict2.keys()):
        if key in dict1:
            new_key = generate_new_dict_key(key, dict1)
            dict2[new_key] = dict2.pop(key)

    return {**dict1, **dict2}


class autocontrol:
    def __init__(self, num_meas_channels=1):

        # accepts only sample preparations
        self.prepare_only = False

        # currently executed preparations and measurements
        self.tasks_in_process = []

        self.RE = RunEngine({})
        self.db = Broker.named('temp')
        self.RE.subscribe(self.db.insert)

        # dynamic call back subscription
        rr = RunRouter([bec_factory])
        self.RE.subscribe(rr)

        self.lh = lh_device(name="lh", labels={"motors"})

        self.qcmd = open_QCMD("QCMD", labels={"detectors"})
        # TODO: Move this to instrument initializatin routines
        self.qcmd.exposure_time = 1
        self.qcmd.address = "http://localhost:5011/QCMD/"

        # priority queue and locking
        self.queue = PriorityQueue()
        self.lock = Lock()

        # sample tracker
        self.sample_tracker = {}

    # creates call backs for every run

    def get_device_object(self, name):
        """
        Helper function that identifies the device object and Bluesky plan based on the device name.
        :param name: device name
        :return: (tuple), device object and plan
        """
        if name == 'LH':
            device = self.lh
            plan = self.lh_plan
        elif name == 'QCMD':
            device = self.qcmd
            plan = self.qcmd_plan
        else:
            device = None
            plan = None
        return device, plan

    def process_job(self, job):
        """
        Processes one job task and returns status.

        If autochannel settings are used, the algorithm will autoselect channels when transferring samples between
        devices. The initial preparation of a (sub)sample can take place in any channel, for example, any final vial
        in the liquid handler can constitute a channel, and it will be automatically selected based on availability. Any
        transfer in a new device is selected based on availability only the first time. After that any subsequent
        transfer with that same sample number will use this channel. Thereby, all material under one particular sample
        number will follow the same path between devices. This makes sure that subsequent measurements are made using
        the same channel or substrate.

        :param job: (list) job object, [priority, task]
        :return: (bool, str) success flag, response string
        """

        priority = job[0]
        task = job[1]

        # Generate unique key for each run. The key generation algorithm
        # must only guarantee that execution of the runs that are assigned
        # the same key will never overlap in time.
        run_key = f"run_key_{str(priority * (-1))}"

        # identify the device based on the device name
        device, plan = self.get_device_object(task['device'])
        # for transfer tasks only
        target_device, target_plan = self.get_device_object(task['target_device'])
        if device is None:
            return False, 'Unknown device.'

        if task['type'] == 'init':
            # TODO Implement consistently in device APIs, particularly the channel init. The device object should
            # TODO have an attribute device.number_of_channels
            resp0 = device.init(number_of_channels=task['channel'])
            resp1 = task['device'] + ' initialized with ' + str(task['channel']) + 'channels. \n'
            resp1 += ' [' + task['device'] + ']: ' + resp0
            return True, resp1

        if task['type'] == 'shut down':
            # TODO implement consistently in device APIs
            response = device.shutdown()
            response = task['device'] + ' shutdown. \n [' + task['device'] + ']: ' + response
            return True, response

        # test if task can be executed (preparation, transfer or measurement at this point)
        # *1 test if resource is generally available
        # TODO: Implement status for accepting new commands in device APIs not using val
        if device.val.status != '':
            return False, task['device'] + ' is busy.'

        # *2 test if task can be executed concerning other tasks in progress
        # collect information on free and busy preparation and measurement channels (fpc, bpc, fmc, bmc).
        # autochannels will use the lowest available
        pflag = True

        if task['task_type'] == 'preparation':
            fpc, bpc = self.update_tasks_in_process(task['device'], device)
            # preparations are free to use any channel, if autochannel is selected
            if task['channel'] is None:
                if fpc:
                    # chose first available
                    task['channel'] = fpc[0]
                else:
                    pflag = False
            elif task['channel'] in bpc:
                pflag = False

        if task['task_type'] == 'transfer':
            fpc, bpc = self.update_tasks_in_process(task['device'], device)
            if task['channel'] is None:
                found, channel, targetchannel = self.sample_tracker_find(task, device=task['device'])
                if found:
                    task['channel'] = channel
                    task['target_channel'] = targetchannel
                else:
                    pflag = False
            elif task['channel'] in bpc:
                pflag = False

            fpc, bpc = self.update_tasks_in_process(task['target_device'], device=target_device)
            if task['target_channel'] is None:
                if fpc:
                    task['target_channel'] = fpc[0]
                else:
                    pflag = False
            elif task['target_channel'] in bpc:
                pflag = False

        if task['task_type'] == 'measurement':
            fp, bpc, = self.update_tasks_in_process(task['device'], device)
            if task['channel'] is None:
                found, channel, targetchannel = self.sample_tracker_find(task, device=task['device'])
                found, channel, targetchannel = self.sample_tracker_find(task, targetdevice=task['device'])
                if fmc:
                    autochannel = fmc[0]
                else:
                    mflag = False
            elif task['channel'] in bmc:
                mflag = False

        # either channel or target_channel (for transfers) are busy
        if not pflag:
            return False, 'Channel or target channel are in use.'

        # put if not already there
        # TODO: Make sure that not multiple copies of the same task are stored
        self.sample_tracker_put(task)

        sample = task['sample']
        channel = task['channel']
        targetchannel = task['target_channel']
        md = task['md']

        if task['type'] == 'measure':
            yield from bpp.set_run_key_wrapper(plan(sample=sample, channel=channel, md=md), run_key)
            self.meas_list.append(task)
        elif task['type'] == 'prepare':
            yield from bpp.set_run_key_wrapper(plan(sample=sample, channel=channel, md=md), run_key)
            self.prep_list.append(task)
        elif task['type'] == 'transfer':
            # TODO: need to define how to pass the target_channel and target_device. Part of sample?
            # TODO: That depends on the definition of the sample field, to some extend.
            yield from bpp.set_run_key_wrapper(plan(sample=sample, channel=channel, md=md), run_key)
            self.prep_list.append(task)

        resp = ('Succesfully started ' + task['type'] + ' for sample ' + str(task['sample_number']) + ' on ' +
                task['device'])
        return True, resp

    def queue_execute_one_item(self):
        """
        Executes one task from the priority queue if not empty and the resource is available.

        Logic:
        Tasks in the queue are discriminated by their priority and task type. Priority is a combined quantity of sample
        number and task submission time, giving higher priorities to lower sample numbers and earlier submission times.
        Task types are prioritized from high to low as: 'init', ('prepare', 'transfer', 'measure'), 'shut down', and
        'exit'. After the highest priority task that can be executed given the availability of resources, the method
        terminates and returns a status string. 'Shut down' and 'exit' tasks are only executed if no tasks of higher
        priority are in the queue. 'Prepare', 'transfer', and 'measure' task are of the same priority, as they might be
        used in different order and multiple times on any given sample. The order of those tasks for the same sample
        is only determined by their submission time to the queue.

        :return: String that reports on what action was taken.
        """

        task_priority = [['init'], ['prepare', 'transfer', 'measure'], ['shut down'], ['exit']]
        response = ''
        blocked_samples = []
        unsuccesful_jobs = []
        success = False

        i = 0
        while i < len(task_priority):
            task_type = task_priority[i]
            # retrieve job from queue
            job = self.queue_get(task_type=task_type)
            if job is None:
                # no job of this priority found, move on to next priority group (task type)
                i += 1
            elif job[1]['sample_number'] not in blocked_samples:
                success, response = self.process_job(job)
                if success:
                    # a succesful job ends the execution of this method
                    break
                else:
                    # this sample number is now blocked as processing of the job was not successful
                    blocked_samples.append(job[1]['sample_number'])
                    unsuccesful_jobs.append(job)
            else:
                unsuccesful_jobs.append(job)

        # put unsuccessful jobs back in the queue
        for job in unsuccesful_jobs:
            self.queue.put_nowait(job)

        if success:
            return 'Success.\n ' + response
        else:
            return 'No task executed.'

    def queue_put(self, sample=None, channel=None, md=None, sample_number=None, task_type='prepare',
                  device=None, target_device=None, target_channel=None):
        """
        Puts an item into the priority queue, which is of a certain task type.

        Notes based on task type:

        init: If channel field is not None, it sets up the device with the number of channels given in this data field.
        measurement: If channel is None, then the channel is selected automatically.

        :param sample: (dict) A description of the sample that can be passed to the LH or potentially stored as md.
        :param channel: (int) which measurement channel to use.
        :param md: (dict) Any metadata to attach to the run.
        :param sample_number: (int) Sample number for current Bluesky run. The lower, the higher the priority.
        :param task_type: (str) Distinguishes between
                            'init' for instrument initialization
                            'prepare' for sample preparation
                            'transfer' for sample transfer
                            'measure' for measurement.
                            'shut down' for instrument shut down
                            'exit' for ending main loop
        :param device: Measurement device, one of the following: 'LH': liquid handler
                                                                 'QCMD': QCMD
                                                                 'NR': neutron reflectometer
        :param target_device: for task_type transfer, designates the target device
        :param target_channel: for task_type transfer, designates the target channel
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
             'sample_number': sample_number,
             'channel': channel,
             'meta': md,
             'task_type': task_type,
             'device': device,
             'priority': priority,
             'target_device': target_device,
             'target_channel': target_channel
             }
        )
        self.queue.put_nowait(item)

    def queue_get(self, task_type=None):
        """
        Retrieves an item from the preparation queue.
        :param task_type: if not None, (list) filter by task types contained in list
        :return: Data field of the retrieved item or None if none retrieved
        """
        def get_highest_priority_item_by_task(queue, task_type):
            # create a new priority queue
            queue2 = PriorityQueue()

            all_items = [item for item in queue.queue]
            # create a list of all items in the queue where 'step' is 'prepare'
            task_type_items = [item for item in queue.queue if item[1]['task_type'] in task_type]

            # return the data dict of the first item in the sorted list, if it exists and create a new list without it
            if task_type_items:
                # sort the prepare_items list in descending order of priority value
                task_type_items.sort(key=lambda item: item[1]['priority'], reverse=True)
                for item in all_items:
                    # priority is a unique identifier
                    if item[1]['priority'] != task_type_items[0][1]['priority']:
                        queue2.put_nowait(item)
                return queue2, task_type_items[0]
            else:
                return queue, None

        if self.queue.empty():
            return None

        # non-standard queue manipulation requires thread lock
        with self.lock:
            if task_type is not None:
                self.queue, item = get_highest_priority_item_by_task(self.queue, task_type)
            else:
                item = self.queue.get_nowait()

        return item

    @bpp.run_decorator(md={})
    def lh_plan(self, sample=None, channel=0, md=None):
        """
        Bluesky measurement plan for the liquid handler as implemented in liquid_handler_device.py
        :param sample: (optional) sample description, will be stored with the metadata
        :param channel: (optional, default=0) the measurement channel to be used
        :param md: (optional) metadata to be stored with the measurement
        :return: no return value
        """
        md2 = {'sample': sample,
               'channel': channel,
               'start_time': time.gmtime(time.time())}
        md3 = merge_dict(md2, md)

        yield from mv(self.lh, [sample, channel], md=md3)

    @bpp.run_decorator(md={})
    def qcmd_plan(self, sample=None, channel=0, md=None):
        """
        Bluesky measurement plan for a QCMD device as implemented in QCMD_device.py
        :param sample: (optional) sample description, will be stored with the metadata
        :param channel: (optional, default=0) the measurement channel to be used
        :param md: (optional) metadata to be stored with the measurement
        :return: no return value
        """

        # The QCMD device does not require the sample description for its measurement. It is only included in the
        # metadata
        md2 = {'sample': sample,
               'channel': channel,
               'start_time': time.gmtime(time.time())}
        # merge with additional metadata
        md3 = merge_dict(md2, md)

        # TODO: Change this to mv, as we will most likely decouple starting a measurement and retrieving the data
        yield from count([self.qcmd], md=md3)

    def update_tasks_in_process(self, devicename, device):
        """
        Helper function that checks a tasklist, such as those for in-procuess preparation and measurement tasks for channels
        that are in use for a particular device. It removes tasks that are finished from the list.
        :param devicename: device name
        :param device: device object
        :param tasklist: the task list
        :return: tuple, list of channels in use and the modified tasklist
        """
        in_use_channels = []
        for task in self.tasks_in_process:
            if task['device'] != devicename:
                continue
            # TODO: needs implementation
            if device.val.get_channel_status(task['channel']) == 'available':
                # task is done, remove item from list
                # TODO: initiate data readout
                self.tasks_in_process.remove(task)
            else:
                # mark channel as in use
                in_use_channels.append(int(task['channel']))

        free_channels = [i for i in range(device.number_of_channels) if i not in in_use_channels]

        return free_channels, in_use_channels



