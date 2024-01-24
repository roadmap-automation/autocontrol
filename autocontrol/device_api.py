import time
import math
import os
from QCMD_device import open_QCMD
from task_container import TaskContainer
from liquid_handler_device import lh_device
from status import Status


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
    def __init__(self, num_meas_channels=1, storage_path=None):

        # accepts only sample preparations
        self.prepare_only = False

        # Devices
        self.lh = lh_device(name="lh", labels={"motors"})
        self.qcmd = open_QCMD("QCMD")

        # TODO: Move this to instrument initialization routines
        self.qcmd.exposure_time = 1
        self.qcmd.address = "http://localhost:5011/QCMD/"

        # Queues and containers
        if storage_path is None:
            storage_path = '../test/'

        db_path_queue = os.path.join(storage_path, 'priority_queue.sqlite3')
        db_path_history = os.path.join(storage_path, 'history_queue.sqlite3')
        db_path_active = os.path.join(storage_path, 'active_queue.sqlite3')

        # priority queue for future tasks
        self.queue = TaskContainer(db_path_queue)
        # sample tracker for the entire task history
        self.sample_history = TaskContainer(db_path_history)
        # currently executed preparations and measurements
        self.active_tasks = TaskContainer(db_path_active)

    # creates call backs for every run

    def get_device_object(self, name):
        """
        Helper function that identifies the device object and plan based on the device name.
        :param name: device name
        :return: device object
        """
        if name == 'LH' or name == 'lh':
            device = self.lh
        elif name == 'QCMD' or name == 'qcmd':
            device = self.qcmd
        else:
            device = None
        return device

    def get_channel_information_from_active_tasks(self, devicename):
        """
        Helper function that checks the active tasklist for channels that are in use for a particular device.
        :param devicename: device name for which the channel availability will be checked
        :return: tuple, list of free_channels, busy_channels
        """

        device = self.get_device_object(devicename)
        # find in-use channels based on stored active tasks
        busy_channels, _ = self.active_tasks.find_channels_for_device(devicename)
        free_channels = list(set(range(1, device.number_of_channels+1)) - set(busy_channels))

        return free_channels, busy_channels

    def process_job(self, task):
        """
        Processes one job task and returns status.

        If autochannel settings are used, the algorithm will autoselect channels when transferring samples between
        devices. The initial preparation of a (sub)sample can take place in any channel, for example, any final vial
        in the liquid handler can constitute a channel, and it will be automatically selected based on availability. Any
        transfer in a new device is selected based on availability only the first time. After that any subsequent
        transfer with that same sample number will use this channel. Thereby, all material under one particular sample
        number will follow the same path between devices. This makes sure that subsequent measurements are made using
        the same channel or substrate.

        :param task: (list) job object, [priority, task]
        :return: (bool, str) success flag, response string
        """

        def process_init(task):
            device = self.get_device_object(task['device'])
            device_status = device.get_device_status()
            if device_status != Status.UP:
                ret = False
            else:
                ret = True
            # currently no further checks on init
            return ret, task

        def process_shutdown(task):
            # TODO: Implement waiting for all active tasks to finish
            return True, task

        def process_prepare_transfer_measure(task):
            # Update list of all tasks in progress for the device and target device
            free_channels, busy_channels, = self.get_channel_information_from_active_tasks(task['device'])

            # If there's a target device for a transfer, update target channels
            if task['target_device'] and task['task_type'] == 'transfer':
                free_target_channels, busy_target_channels = (
                    self.get_channel_information_from_active_tasks(task['target_device']))

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
            elif task['task_type'] == 'measure':
                execute_task = assign_channel('channel', free_channels, busy_channels, channel)

            return execute_task, task

        # identify the device based on the device name
        device = self.get_device_object(task['device'])
        # for transfer tasks only
        target_device = self.get_device_object(task['target_device'])
        if device is None:
            return False, 'Unknown device.'

        if task['task_type'] == 'init':
            execute_task, task = process_init(task)
        elif task['task_type'] == 'shut down':
            execute_task, task = process_shutdown(task)
        elif task['task_type'] == 'exit':
            # TODO: Implement. Ending main loop. Other clean up tasks?
            execute_task = False
        else:
            execute_task, task = process_prepare_transfer_measure(task)

        if execute_task:
            # TODO: Implement in plan and API: instrument init with setting the number of channels, instrument shutdown,
            #  and exit with waiting for all jobs in queue to finish
            # TODO: Do we want to reserve certain device channels for a particular sample until all tasks
            #  associated witha a sample have been processed?
            # Note: Every task execution including measurements only send a signal to the device and do not wait for
            # completion. Results are collected separately during self.get_channel_information_from_active_tasks(). This allows for
            # parallel tasks.

            # add start time to metadata
            md2 = {'execution_start_time': time.gmtime(time.time())}
            # merge with additional metadata
            task['md'] = merge_dict(task['md'], md2)

            status = device.execute_task(task=task)
            if status == Status.SUCCESS:
                # store every task that is executed active tasks
                self.active_tasks.put(task)
                resp = ('Succesfully started ' + task['task_type'] + ' for sample ' + str(task['sample_number']) +
                        ' on ' + task['device'])
            else:
                execute_task = False
                resp = 'Task failed at instrument.'

        else:
            resp = 'Channel or target channel are in use.'

        return execute_task, resp

    def queue_inspect(self):
        """
        Returns the items of the queue in a list without removing them from the queue.
        :return: (list) the items of the queue
        """

        return self.queue.get_all()

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

        # TODO: Not sure that it is desired behavior that init tasks are executed before others irrespective of their
        #   relative priority
        i = 0
        while i < len(task_priority):
            task_type = task_priority[i]
            # retrieve job from queue
            job = self.queue.get_and_remove_by_priority(task_type=task_type)
            if job is None:
                # no job of this priority found, move on to next priority group (task type)
                i += 1
            elif job['sample_number'] not in blocked_samples:
                success, response = self.process_job(job)
                if success:
                    # a succesful job ends the execution of this method
                    break
                else:
                    # this sample number is now blocked as processing of the job was not successful
                    blocked_samples.append(job['sample_number'])
                    unsuccesful_jobs.append(job)
            else:
                unsuccesful_jobs.append(job)

        # put unsuccessful jobs back in the queue
        for job in unsuccesful_jobs:
            self.queue.put(job)

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

        item = {
            'task': task,
            'sample_number': sample_number,
            'channel': channel,
            'md': md,
            'task_type': task_type,
            'device': device,
            'priority': priority,
            'target_device': target_device,
            'target_channel': target_channel
        }

        self.queue.put(item)

    def update_active_tasks(self):
        """
        Goes through the entire list of active tasks and checks if they are completed. Follows up with clean-up steps.
        :return: no return value
        """

        # TODO: incorporate acquisition time

        task_list = self.active_tasks.get_all()

        for task in task_list:
            device = self.get_device_object(task['device'])
            if task['channel'] is None:
                # channel-less task such as init
                status = device.get_device_status()
                if status != Status.UP:
                    # device is not ready to accept new commands and therefore, the current one is not finished
                    continue
            else:
                # get channel-dependent status
                channel_status = device.get_channel_status(task['channel'])
                if channel_status == Status.BUSY:
                    # task not done
                    continue
                if task['target_channel'] is not None:
                    target_device = self.get_device_object(task['target_device'])
                    target_channel_status = target_device.get_channel_status(task['target_channel'])
                    if target_channel_status == Status.BUSY:
                        # task not done
                        continue

            # task is ready for collection

            # get measurment data
            if task['task_type'] == 'measure':
                while device.get_device_status() != Status.UP:
                    # wait for device to become available to retrieve data
                    # TODO: Better exception handling for this critical case
                    print('Device {} not up.', task['device'])
                    time.sleep(10)
                read_status, data = device.read(channel=task['channel'])
                # append data to task
                if 'md' not in task:
                    task['md'] = {}
                task['md']['measurement_data'] = data

            # move task to history
            self.active_tasks.remove(task)
            self.sample_history.put(task)
