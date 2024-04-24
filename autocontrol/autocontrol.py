import json
import time
import math
import os
from device_qcmd import open_QCMD
from task_container import TaskContainer
from device_liquid_handler import lh_device
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
        self.lh = lh_device(name="lh")
        self.qcmd = open_QCMD(name="QCMD", address="http://localhost:5011/QCMD/")

        # Queues and containers

        # Priority, active, and history queues
        if storage_path is None:
            self.storage_path = '../test/'

        db_path_queue = os.path.join(self.storage_path, 'priority_queue.sqlite3')
        db_path_history = os.path.join(self.storage_path, 'history_queue.sqlite3')
        db_path_active = os.path.join(self.storage_path, 'active_queue.sqlite3')

        # priority queue for future tasks
        self.queue = TaskContainer(db_path_queue)
        # sample tracker for the entire task history
        self.sample_history = TaskContainer(db_path_history)
        # currently executed preparations and measurements
        self.active_tasks = TaskContainer(db_path_active)

        # highest sample number in use
        self.highest_sample_number = None

        # channel physical occupation
        # for each device there will be a list in this dictionary with the device name as the key. Each list has as many
        # entries as channels. Each entry is either None for not occupied, or it contains the task object last executed
        # in this channel
        self.channel_po = {}
        self.store_channel_po()

    def get_channel_occupancy(self, devicename):
        """
        Obtains the channel occupancy from the active tasks (operational occupancy) and the channel physical occupancy
        status self.channel_po[devicename]. This yields surely free channels and potentially busy channels for methods
        trying to identify free channels.
        :param devicename: (str) name of the device for which the channels are analyzed
        :return: (list, list): list of channel numbers that are either free or busy
        """
        free_channels, busy_channels, = self.get_channel_information_from_active_tasks(devicename)
        # Combine this information with the channel physical occupation data
        if devicename in self.channel_po:
            cpo_list = self.channel_po[devicename]
            free_channels_po = [i for i in range(len(cpo_list)) if cpo_list[i] is None]
            busy_channels_po = [i for i in range(len(cpo_list)) if cpo_list[i] is not None]
            free_channels = list(set(free_channels + free_channels_po))
            busy_channels = list(set(busy_channels + busy_channels_po))
        return free_channels, busy_channels

    def get_device_object(self, name, device_address=None):
        """
        Helper function that identifies the device object and plan based on the device name.
        :param name: device name
        :param device_address: optional device address that will be stored in the respective device object
        :return: device object
        """
        if name == 'LH' or name == 'lh':
            device = self.lh
        elif name == 'QCMD' or name == 'qcmd':
            device = self.qcmd
        else:
            device = None

        if device is not None and device_address is not None:
            device.address = device_address

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
        free_channels = list(set(range(0, device.number_of_channels)) - set(busy_channels))

        return free_channels, busy_channels

    def process_init(self, task):
        if 'device_address' in task['task']:
            device_address = task['task']['device_address']
        else:
            device_address = None
        device = self.get_device_object(task['device'], device_address)
        device_status = device.get_device_status()
        if device_status != Status.UP:
            ret = False
            resp = 'Device status is not UP.'
        else:
            ret = True
            resp = 'Success.'
        # currently no further checks on init
        return ret, task, resp

    def process_shutdown(self, task):
        # TODO: Implement waiting for all active tasks to finish and shut down device
        return True, task, "Success."

    def process_measure(self, task):
        # A measurement task should have a channel which is already occupied by the sample
        if task['device'] not in self.channel_po:
            return False, task, 'Device not intialized.'

        if 'acquisition_time' not in task['task']:
            task['task']['acquisition_time'] = None

        cpol = self.channel_po[task['device']]
        sample_number = task['sample_number']

        if task['channel'] is None:
            # Source channel is not defined. Locate the sampel based on sample number. If there are multiple,
            # measure the one with the highest priority
            best_channel = None
            for i, channel_task in enumerate(cpol):
                if channel_task is not None and channel_task['sample_number'] == sample_number:
                    if best_channel is None or cpol[best_channel]['priority'] > cpol[i]['priority']:
                        best_channel = i
            if best_channel is None:
                return False, task, 'Did not find the sample to transfer.'
            task['channel'] = best_channel
        else:
            # check if manual channel selection is valid
            if not (0 <= task['channel'] < len(cpol)):
                return False, task, 'Invalid channel.'
            if cpol[task['channel']] is None:
                return False, task, 'No sample in measurement channel'
            if cpol[task['channel']]['sample_number'] != sample_number:
                return False, task, 'Wrong sample in measurement channel.'

        return True, task, "Success."

    def process_no_channel(self, task):
        # currently no conditions to test
        return True, task, "Success."

    def process_prepare(self, task):
        #
        if task['device'] not in self.channel_po:
            return False, task, 'Device not intialized.'

        cpol = self.channel_po[task['device']]
        sample_number = task['sample_number']
        device = self.get_device_object(task['device'])
        channel_mode = device.channel_mode
        free_channels, _ = self.get_channel_occupancy(task['device'])
        if not free_channels:
            return False, task, 'No free channels available.'

        if channel_mode is None:
            if not free_channels:
                return False, task, 'No free channels.'
            task['channel'] = free_channels[0]

        elif channel_mode == 'reuse' and task['channel'] is None:
            # Find previous channel and target channel for this sample and device, the same channels will be reused
            hist_channel, _ = self.sample_history.find_channels_per_device(task)
            act_channel, _ = self.active_tasks.find_channels_per_device(task)
            hist_channel = list(set(hist_channel + act_channel))
            if not hist_channel:
                task['channel'] = free_channels[0]
            else:
                success = False
                for channel in hist_channel:
                    if channel in free_channels:
                        task['channel'] = channel
                        success = True
                        break
                if not success:
                    return False, task, 'No free channels.'

        elif channel_mode == 'new' and task['channel'] is None:
            # Find previous channel and target channel for this device, the same channels will be reused
            hist_channel, _ = self.sample_history.find_channels_per_device(task, sample=False)
            act_channel, _ = self.active_tasks.find_channels_per_device(task, sample=False)
            hist_channel = list(set(hist_channel + act_channel))
            success = False
            for channel in free_channels:
                if channel not in hist_channel:
                    task['channel'] = channel
                    success = True
                    break
            if not success:
                return False, task, 'No free channels.'

        else:
            return False, task, 'Invalid channel mode.'

        return True, task, "Success."

    def process_transfer(self, task):
        # A transfer task should have a source channel which is already occupied by the sample that is being
        # transferred.

        if task['device'] not in self.channel_po:
            return False, task, 'Device not intialized.'
        if task['target_device'] not in self.channel_po:
            return False, task, 'Target device not initialized'

        # add force transfer flag if not present
        if 'force' not in task['task']:
            task['task']['force'] = False
        force = task['task']['force']
        # add non-channel source if not present
        if 'non_channel_source' not in task['task']:
            task['task']['non_channel_source'] = None
        # add non-channel target if not present
        if 'non_channel_target' not in task['task']:
            task['task']['non_channel_target'] = None

        # check for consistency between non-channel and channel transfers:
        if task['task']['non_channel_source'] is not None and task['task']['channel'] is not None:
            return False, task, 'Source channel and non-channel source simultaneously provided.'
        if task['task']['non_channel_target'] is not None and task['task']['target_channel'] is not None:
            return False, task, 'Target channel and non-channel target simultaneously provided.'

        cpol = self.channel_po[task['device']]
        target_cpol = self.channel_po[task['target_device']]
        sample_number = task['sample_number']

        if task['channel'] is not None:
            # check if manual channel selection is valid
            if not (0 <= task['channel'] < len(self.channel_po[task['device']])):
                return False, task, 'Invalid channel.'
            if cpol[task['channel']]['sample_number'] != sample_number:
                return False, task, 'Wrong sample in source channel.'
        elif task['task']['non_channel_source'] is None:
            # Source channel is not defined. Find a source channel. Locate the sample based on sample number. If there
            # are multiple, transfer the one with the highest priority.
            best_channel = None
            for i, channel_task in enumerate(cpol):
                if channel_task is not None and channel_task['sample_number'] == sample_number:
                    if best_channel is None or cpol[best_channel]['priority'] > cpol[i]['priority']:
                        best_channel = i
            if best_channel is None:
                return False, task, 'Did not find the sample to transfer.'
            task['channel'] = best_channel

        if task['task']['non_channel_target'] is not None:
            # no need to identify target channel
            return True, task, 'Success.'

        target_device = self.get_device_object(task['target_device'])
        channel_mode = target_device.channel_mode
        free_target_channels, _ = self.get_channel_occupancy(task['target_device'])
        if not free_target_channels and not force:
            return False, task, 'No idle target channels available.'

        if task['target_channel'] is None:
            if channel_mode is None:
                if not free_target_channels:
                    return False, task, 'No idle target channels.'
                task['target_channel'] = free_target_channels[0]

            elif channel_mode == 'reuse' and task['channel'] is None:
                # Find previous channel and target channel for this sample and device, the same channels will be reused
                _, hist_target_channel = self.sample_history.find_channels_per_device(task)
                _, act_target_channel = self.active_tasks.find_channels_per_device(task)
                hist_target_channel = list(set(hist_target_channel + act_target_channel))
                if not hist_target_channel:
                    task['target_channel'] = free_target_channels[0]
                else:
                    success = False
                    for channel in hist_target_channel:
                        if channel in free_target_channels:
                            task['target_channel'] = channel
                            success = True
                            break
                    if not success:
                        return False, task, 'No free target channels.'

            elif channel_mode == 'new' and task['channel'] is None:
                # Find previous channel and target channel for this device, the same channels will be reused
                _, hist_target_channel = self.sample_history.find_channels_per_device(task, sample=False)
                _, act_target_channel = self.active_tasks.find_channels_per_device(task, sample=False)
                hist_target_channel = list(set(hist_target_channel + act_target_channel))
                success = False
                for channel in free_target_channels:
                    if channel not in hist_target_channel:
                        task['target_channel'] = channel
                        success = True
                        break
                if not success:
                    return False, task, 'No free target channels.'

            else:
                return False, task, 'Invalid channel mode.'

        # check if either manual or freshly determined target channel selection is valid
        if not (0 <= task['target_channel'] < len(target_cpol)):
            return False, task, 'Invalid target channel.'
        if target_cpol[task['target_channel']] is not None and not force:
            return False, task, 'Target channel not empty.'

        return True, task, 'Success.'

    def process_task(self, task):
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

        # identify the device based on the device name
        device = self.get_device_object(task['device'])

        if device is None:
            return False, task, 'Unknown device.'
        elif task['task_type'] == 'init':
            execute_task, task, resp = self.process_init(task)
        elif device.get_device_status() is not Status.UP:
            # check if device is DOWN or unable to accept a task because it is BUSY or for other reasons
            # init is an exception as this task puts the device in operation
            return False, task, 'Device is down or busy.'
        elif task['task_type'] == 'shut down':
            execute_task, task, resp = self.process_shutdown(task)
        elif task['task_type'] == 'transfer':
            target_device = self.get_device_object(task['device'])
            if target_device is None:
                return False, task, 'Unknown target device.'
            if target_device.get_device_status() is not Status.UP:
                return False, task, 'Target device is down or busy.'
            execute_task, task, resp = self.process_transfer(task)
        elif task['task_type'] == 'prepare':
            execute_task, task, resp = self.process_prepare(task)
        elif task['task_type'] == 'measure':
            execute_task, task, resp = self.process_measure(task)
        elif task['task_type'] == 'no_channel':
            execute_task, task, resp = self.process_no_channel(task)
        else:
            return False, task, 'Unknown task type.'

        # Check if the device and channel of the task interferes with an ongoing task of the same sample number. This is
        # another layer of protection against cases in which are not caught by checks on the physical and operational
        # channel occupancies. Those checks can fail if the same sample is already present in a channel from a previous
        # step, although it (a new volume with the same sample number) is currently being transferred, or for starting
        # a measurement on a device and channel while there is a measurement still going on. It is also important for
        # transfer tasks to passive devices, which will not report on their channel activity or device status due to the
        # task.
        if execute_task and self.active_tasks.find_interference(task):
            execute_task = False
            resp = 'Waiting for ongoing task at (target) device and (target) channel to finish.'

        if execute_task:
            # Note: Every task execution including measurements only sends a signal to the device and do not wait for
            # completion. Results are collected separately during self.update_active_tasks(). This allows for parallel
            # tasks.

            # add start time to metadata
            if 'md' not in task:
                task['md'] = {}
            task['md']['execution_start_time'] = time.gmtime(time.time())

            status = device.execute_task(task=task)
            if task['task_type'] == 'transfer' and device != target_device:
                # a transfer task is active on the source and target device, reserving respective channels
                status &= target_device.execute_task(task=task)

            # TODO: There is a more elaborate exception handling required in case that one of the two devices ivolved
            #   in a transfer is returning a non-success status.

            if status == Status.SUCCESS:
                # store every task that is executed active tasks
                task['md']['response'] = resp
                self.active_tasks.put(task)
            else:
                execute_task = False
                resp = 'Task failed at instrument.'

        return execute_task, task, resp

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
        Task types are prioritized from high to low as: 'init', ('prepare', 'transfer', 'measure'),and 'shut down'.
        After the highest priority task that can be executed given the availability of resources, the method
        terminates and returns a status string. The 'shut down' task is only executed if no tasks of higher
        priority are in the queue. 'Prepare', 'transfer', and 'measure' task are of the same priority, as they might be
        used in different order and multiple times on any given sample. The order of those tasks for the same sample
        is only determined by their submission time to the queue.

        :return: String that reports on what action was taken.
        """

        # The parallel execution of tasks makes it difficult to re-initialize an instrument during a run. A not perfect
        # implementation is to give the 'init' task a higher priority than the rest.
        task_priority = [['init'], ['prepare', 'transfer', 'measure', 'no_channel'],  ['shut down']]
        response = ''
        blocked_samples = []
        unsuccesful_jobs = []
        success = False

        i = 0
        while i < len(task_priority):
            task_type = task_priority[i]
            # retrieve job from queue
            task = self.queue.get_and_remove_by_priority(task_type=task_type)
            if task is None:
                # no job of this priority found, move on to next priority group (task type)
                i += 1
            elif task['sample_number'] not in blocked_samples:
                success, task, response = self.process_task(task)
                if 'md' not in task:
                    task['md'] = {}
                task['md']['response'] = response
                if success:
                    # a succesful job ends the execution of this method
                    break
                else:
                    # this sample number is now blocked as processing of the job was not successful
                    blocked_samples.append(task['sample_number'])
                    unsuccesful_jobs.append(task)
            else:
                unsuccesful_jobs.append(task)

        # put unsuccessful jobs back in the queue
        for job in unsuccesful_jobs:
            self.queue.put(job)

        return response

    def queue_put(self, task):
        """
        Puts a task into the priority queue.

        :param task: (task.Task) The task.
        :return: no return value
        """

        if self.highest_sample_number is None:
            # TODO create data base query for recovery
            self.highest_sample_number = 0

        if task.sample_number is None:
            # create a sample number if none present
            task.sample_number = self.highest_sample_number + 1
        else:
            if task.sample_number > self.highest_sample_number:
                self.highest_sample_number = task.sample_number

        # create a priority value with the following importance
        # 1. Sample number
        # 2. Time that step was submitted
        # convert time to a priority <1
        p1 = time.time()/math.pow(10, math.ceil(math.log10(time.time())))
        # convert sample number to priority, always overriding start time.
        priority = task.sample_number * (-1.)
        priority -= p1

        self.queue.put(task)

    def store_channel_po(self):
        """
        Stores the channel physical occupancy list in the storage directory.
        :return: no return value
        """
        with open(os.path.join(self.storage_path, 'channel_po.json'), 'w') as f:
            json.dump(self.channel_po, f, indent=4)

    def update_active_tasks(self):
        """
        Goes through the entire list of active tasks and checks if they are completed. Follows up with clean-up steps.
        :return: no return value
        """

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

            # Test if target channel is still busy. Not all tasks will set a target channel to busy.
            if task['target_channel'] is not None:
                target_device = self.get_device_object(task['target_device'])
                target_channel_status = target_device.get_channel_status(task['target_channel'])
                if target_channel_status == Status.BUSY:
                    # task not done
                    continue
            elif task['task_type'] == 'transfer':
                # target channel-less transfer task
                status = device.get_device_status()
                if status != Status.UP:
                    # device is not ready to accept new commands and therefore, the current one is not finished
                    continue

            # task is ready for collection

            if task['task_type'] == 'init':
                # create an empty channel physical occupancy entry for the device (False == not occupied)
                noc = device.number_of_channels
                self.channel_po[task['device']] = [None] * noc

            elif task['task_type'] == 'measure':
                # get measurment data
                while device.get_device_status() != Status.UP:
                    # Wait for device to become available to retrieve data. This is for catching an instrument
                    # exception and not for waiting for the measurement to finish. This is done with ocupational channel
                    # states above.
                    # TODO: Better exception handling for this critical case
                    print('Device {} not up.', task['device'])
                    time.sleep(10)
                read_status, data = device.read(channel=task['channel'])
                # append data to task
                if 'md' not in task:
                    task['md'] = {}
                task['md']['measurement_data'] = data
                # Attach measurement task to the physical occupancy list
                self.channel_po[task['device']][task['channel']] = task

            elif task['task_type'] == 'prepare':
                # attach current task to the channel physical occupancy
                self.channel_po[task['device']][task['channel']] = task

            elif task['task_type'] == 'transfer':
                # remove existing task from the source channel physical occupancy
                self.channel_po[task['device']][task['channel']] = None
                # attach current task to the target channel physical occupancy
                self.channel_po[task['target_device']][task['target_channel']] = task

            # move task to history and save new channel physical occupancy
            self.active_tasks.remove(task)
            self.sample_history.put(task)
            self.store_channel_po()
