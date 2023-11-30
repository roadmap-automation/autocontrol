import time
from bluesky.plan_stubs import mv
from bluesky.plans import count
import bluesky.preprocessors as bpp
from bluesky import RunEngine
from bluesky.callbacks.best_effort import BestEffortCallback
from event_model import RunRouter
from databroker import Broker
import math
from queue import PriorityQueue
from QCMD_device import open_QCMD
from task_container import TaskContainer
from liquid_handler_device import lh_device
from threading import Lock


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
    if dict1 is None:
        return dict2 or {}
    if dict2 is None:
        return dict1

    merged_dict = dict1.copy()  # Create a copy of dict1
    for key, value in dict2.items():
        if key in merged_dict:
            key = generate_new_dict_key(key, merged_dict)
        merged_dict[key] = value

    return merged_dict


class autocontrol:
    def __init__(self, num_meas_channels=1):

        # accepts only sample preparations
        self.prepare_only = False

        self.RE = RunEngine({})
        self.db = Broker.named('temp')
        self.RE.subscribe(self.db.insert)

        # dynamic call back subscription
        rr = RunRouter([bec_factory])
        self.RE.subscribe(rr)

        self.lh = lh_device(name="lh", labels={"motors"})

        self.qcmd = open_QCMD("QCMD", labels={"detectors"})
        # TODO: Move this to instrument initialization routines
        self.qcmd.exposure_time = 1
        self.qcmd.address = "http://localhost:5011/QCMD/"

        # priority queue and locking
        self.queue = PriorityQueue()
        self.lock = Lock()

        # sample tracker for the entire task history
        self.sample_history = TaskContainer()
        # currently executed preparations and measurements
        self.active_tasks = TaskContainer()

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

        def process_init(task):
            # currently no checks on init
            return True, task

        def process_shutdown(task):
            # TODO: Implement waiting for all active tasks to finish
            return True, task

        def process_prepare_transfer_measure(task):
            # Update list of all tasks in progress for the device and target device
            free_channels, busy_channels, free_target_channels, busy_target_channels = (
                self.update_active_tasks(task['device'], device))

            # If there's a target device for a transfer, update target channels
            if task['target_device'] and task['task_type'] == 'transfer':
                free_target_channels2, busy_target_channels2, _, _ = (
                    self.update_active_tasks(task['target_device'], target_device))
                free_target_channels = list(set(free_target_channels) | set(free_target_channels2))
                busy_target_channels = list(set(busy_target_channels) | set(busy_target_channels2))

            # Find previous channel and target channel for this sample and device, the same channesl will be reused
            if (task['channel'] is None and task['task_type'] != 'transfer') or (task['channel'] is None and
                                                                                 task['target_channel'] is None):
                channel, target_channel = self.sample_history.find_channels_for_sample_number_and_device(task)
            else:
                channel = target_channel = None

            # Initialize execute_task flag
            execute_task = True

            # Function to check and assign channel if available
            def assign_channel(channel_attr, free_channels, busy_channels, history_channel=None):
                if task[channel_attr] is None:
                    if history_channel and history_channel not in busy_channels:
                        task[channel_attr] = history_channel
                    elif not history_channel and free_channels:
                        task[channel_attr] = free_channels[0]
                    else:
                        return False  # No channel available or history channel busy
                elif task[channel_attr] in busy_channels:
                    return False  # Channel is busy
                return True  # Channel assigned or already set

            if task['task_type'] == 'preparation':
                execute_task = assign_channel('channel', free_channels, busy_channels)
            elif task['task_type'] == 'transfer':
                execute_task = assign_channel('channel', free_channels, busy_channels, channel)
                execute_task &= assign_channel('target_channel', free_target_channels, busy_target_channels,
                                               target_channel)
            elif task['task_type'] == 'measurement':
                execute_task = assign_channel('channel', free_channels, busy_channels, channel)

            return execute_task, task

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
            execute_task, task = process_init(task)
        elif task['type'] == 'shut down':
            execute_task, task = process_shutdown(task)
        elif task['type' == 'exit']:
            # TODO: Implement. Ending main loop. Other clean up tasks?
            execute_task = False
        else:
            execute_task, task = process_prepare_transfer_measure(task)

        if execute_task:
            # TODO: Implement status from submitting task. If task submission was not succesful, indicate in
            #  execute_task and resp variables.
            # TODO: Implement in plan and API: instrument init with setting the number of channels, instrument shutdown,
            #  and exit with waiting for all jobs in queue to finish
            # TODO: Do we want to reserve certain device channels for a particular sample until all tasks
            #  associated witha a sample have been processed?
            # Note: Every task execution including measurements only send a signal to the device and do not wait for
            # completion. Results are collected separately during self.update_active_tasks(). This allows using Bluesky
            # with parallel tasks.
            yield from bpp.set_run_key_wrapper(plan(task=task), run_key)

            # store every task that is executed in history and active tasks
            self.sample_history.put(task)
            self.active_tasks.put(task)

            resp = ('Succesfully started ' + task['type'] + ' for sample ' + str(task['sample_number']) + ' on ' +
                    task['device'])
        else:
            resp = 'Channel or target channel are in use.'

        return execute_task, resp

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

        task_priority = [['init'], ['prepare', 'transfer', 'measure'],  ['exit'], ['shut down']]
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

    def queue_put(self, task=None, channel=None, md=None, sample_number=None, task_type='prepare',
                  device=None, target_device=None, target_channel=None):
        """
        Puts an item into the priority queue, which is of a certain task type.

        Notes based on task type:

        init: If channel field is not None, it sets up the device with the number of channels given in this data field.
        measurement: If channel is None, then the channel is selected automatically.

        :param task: (dict) A description of the sample that can be passed to the LH or potentially stored as md.
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

        # TODO: Add time stamps at various points during processing
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
            {'task': task,
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

    def update_active_tasks(self, devicename, device):
        """
        Helper function that checks a tasklist, such as those for in-procuess preparation and measurement tasks for
        channels that are in use for a particular device. It removes tasks that are finished from the list.
        :param devicename: device name
        :param device: device object
        :return: tuple, list of free_channels, busy_channels, free_target_channels, busy_target_channels
        """

        # find in use channels based on stored active tasks
        channels, target_channels = self.active_tasks.find_channels_for_device(devicename)

        # check only those channels if they have become available to minimize device interactions
        busy_channels = []
        free_channels = []
        for channel in channels:
            # TODO: needs implementation
            if device.val.get_channel_status(channel) == 'free':
                # TODO: check for succesful task completion, initiate data readout
                free_channels.append(channel)
            else:
                busy_channels.append(channel)

        # remove freed channels from active task list, which also unblocks target channels if applicable
        self.active_tasks.remove_by_channel(free_channels)

        # update free channels
        busy_channels, busy_target_channels = self.active_tasks.find_channels_for_device(devicename)

        # determine free channels based on number of channels available
        free_channels = [i for i in range(device.number_of_channels) if i not in busy_channels]
        free_target_channels = [i for i in range(device.number_of_channels) if i not in busy_target_channels]

        return free_channels, busy_channels, free_target_channels,  busy_target_channels



