import json
import time
import math
import os
import uuid

from autocontrol.task_container import TaskContainer
from autocontrol.task_struct import TaskType
from autocontrol.task_struct import Task
from autocontrol.status import Status

# device imports
from autocontrol.device_injection import injection_device
from autocontrol.device_liquid_handler import lh_device
from autocontrol.device_qcmd import open_QCMD


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
    def __init__(self, storage_path):

        # accepts only sample preparations
        self.prepare_only = False
        self.storage_path = storage_path

        # Queues and containers
        # Priority, active, and history queues
        db_path_queue = os.path.join(self.storage_path, 'priority_queue.sqlite3')
        db_path_history = os.path.join(self.storage_path, 'history_queue.sqlite3')
        db_path_active = os.path.join(self.storage_path, 'active_queue.sqlite3')

        # priority queue for future tasks
        self.queue = TaskContainer(db_path_queue)
        # sample tracker for the entire task history
        self.sample_history = TaskContainer(db_path_history)
        # currently executed preparations and measurements
        self.active_tasks = TaskContainer(db_path_active)

        # directory of sample id (keys) and the associated sample numbers
        self.sample_id_to_number = {}
        # recreate this dict from saved tasks
        all_tasks = self.queue.get_all() + self.sample_history.get_all() + self.active_tasks.get_all()
        for task in all_tasks:
            self.sample_id_to_number[task.sample_id] = task.sample_number

        # device addresses
        # keys: device name
        # entries: dictionary with keys for device address, device object, device type
        self.devices = {}

        # channel physical occupation
        # for each device there will be a list in this dictionary with the device name as the key. Each list has as many
        # entries as channels. Each entry is either None for not occupied, or it contains the task object last executed
        # in this channel
        self.channel_po = {}
        self.store_channel_po()

        # run control
        self.paused = False

    def check_task(self, task):
        """
        Checks if a particular task has been completed and is ready for collection.
        :param task: The task.
        :return: True if ready, False if not.
        """
        for subtask in task.tasks:
            device = self.get_device_object(subtask.device)
            if subtask.channel is None:
                # channel-less task such as init
                status = device.get_device_status()
                if status != Status.IDLE and status != Status.UP:
                    # device is not ready to accept new commands and therefore, the current one is not finished
                    return False
            else:
                # get channel-dependent status
                channel_status = device.get_channel_status(subtask.channel)
                if channel_status != Status.IDLE and channel_status != Status.UP:
                    # task not done
                    return False
        # passed all tests, task has been finished
        return True

    def find_free_channels(self, subtask, sample_number):
        """
        Finds and allocates a free channel for a subtask, respecting the channel selection logic of the device
        :param subtask: the subtask (tasks.TaskData)
        :param sample_number: the sample number (int)
        :return: success flag, task, response (bool, task.TaskData, str)
        """

        device = self.get_device_object(subtask.device)
        channel_mode = device.channel_mode
        # get free chennels by inspecting active tasks and channel occupation data (the latter not for passive devices)
        free_channels, _ = self.get_channel_occupancy(subtask.device)

        if not free_channels:
            return False, subtask, 'No free channels available.'

        if channel_mode is None:
            subtask.channel = free_channels[0]
            return True, subtask, "Success."

        # Find previous channel and target channel for this sample and device for reuse
        hist_channel = self.sample_history.find_channels(sample_number, subtask.device)
        act_channel = self.active_tasks.find_channels(sample_number, subtask.device)
        hist_channel = list(set(hist_channel + act_channel))

        if channel_mode == 'reuse':
            if not hist_channel:
                subtask.channel = free_channels[0]
            else:
                success = False
                for channel in hist_channel:
                    if channel in free_channels:
                        subtask.channel = channel
                        success = True
                        break
                if not success:
                    return False, subtask, 'Previously used channel is not free.'
        elif channel_mode == 'new':
            success = False
            for channel in free_channels:
                if channel not in hist_channel:
                    subtask.channel = channel
                    success = True
                    break
            if not success:
                return False, subtask, 'No free unused channels.'
        else:
            return False, subtask, 'Invalid channel mode.'

        return True, subtask, "Success."

    def get_channel_occupancy(self, devicename):
        """
        Obtains the channel occupancy from the active tasks (operational occupancy) and the channel physical occupancy
        status self.channel_po[devicename]. This yields surely free channels and potentially busy channels for methods
        trying to identify free channels.
        :param devicename: (str) name of the device for which the channels are analyzed
        :return: (list, list): list of channel numbers that are either free or busy
        """
        device = self.get_device_object(devicename)
        free_channels, busy_channels, = self.get_channel_information_from_active_tasks(devicename)
        # Combine this information with the channel physical occupation data. Ignore for passive devices
        if (not device.passive) and (devicename in self.channel_po):
            cpo_list = self.channel_po[devicename]
            free_channels_po = [i for i in range(len(cpo_list)) if cpo_list[i] is None]
            busy_channels_po = [i for i in range(len(cpo_list)) if cpo_list[i] is not None]
            free_channels = list(set(free_channels).intersection(set(free_channels_po)))
            busy_channels = list(set(busy_channels + busy_channels_po))
        return free_channels, busy_channels

    def get_device_object(self, name):
        """
        Helper function that identifies the device object and plan based on the device name.
        :param name: device name
        :return: device object
        """

        if name in self.devices:
            return self.devices[name]['device_object']
        else:
            return None

    def get_channel_information_from_active_tasks(self, device_name):
        """
        Helper function that checks the active tasklist for channels that are in use for a particular device.
        :param device_name: device name for which the channel availability will be checked
        :return: tuple, list of free_channels, busy_channels
        """

        device = self.get_device_object(device_name)
        # find in-use channels based on stored active tasks
        busy_channels = self.active_tasks.find_channels(device_name=device_name)
        free_channels = list(set(range(0, device.number_of_channels)) - set(busy_channels))

        return free_channels, busy_channels

    def pre_process_init(self, task: Task):
        """
        Perform checks on init task and register device to device list.
        :param task:
        :return: success flag, the task, response (bool, task.Task, str)
        """

        device_name = task.tasks[0].device
        device_type = task.tasks[0].device_type
        device_address = task.tasks[0].device_address
        simulated = task.tasks[0].simulated

        if device_type == 'injection' or device_type == 'INJECTION':
            device_object = injection_device(name=device_name, address=device_address, simulated=simulated)
        elif device_type == 'lh' or device_type == 'LH':
            device_object = lh_device(name=device_name, address=device_address, simulated=simulated)
        elif device_type == 'qcmd' or device_type == 'QCMD':
            device_object = open_QCMD(name=device_name, address=device_address, simulated=simulated)
        else:
            return False, task, 'Unknown device.'

        self.devices[device_name] = {}
        self.devices[device_name]['device_object'] = device_object
        self.devices[device_name]['device_type'] = device_type
        self.devices[device_name]['device_address'] = device_address

        return True, task, 'Success.'

    def pre_process_measure(self, task: Task):
        """
        Perform checks on a measurement task given the current status of the autocontrol environment.
        :param task: the task (task.Task)
        :return: success flag, the task, response (bool, task.Task, str)
        """

        if len(task.tasks) > 1:
            # Multiple measurements per task are not supported because it is not clear how they would be assigned to
            # one sample id or sample number
            return False, task, "Multiple measurements per task not supported."
        subtask = task.tasks[0]

        # A measurement task should have a channel which is already occupied by the sample
        if subtask.device not in self.channel_po:
            return False, task, 'Device not intialized.'

        # check for consistency between non-channel and channel measurements
        if subtask.non_channel_storage is not None and subtask.channel is not None:
            return False, task, 'Channel and non-channel storage simultaneously provided.'

        cpol = self.channel_po[subtask.device]
        sample_number = task.sample_number

        if subtask.channel is not None:
            # check if manual channel selection is valid
            if not (0 <= subtask.channel < len(self.channel_po[subtask.device])):
                return False, task, 'Invalid channel number.'
            if cpol[subtask.channel] is None:
                # A measurement with a manual channel number can create a new sample
                cpol[subtask.channel] = task
                return True, task, 'Success. Created sample on measurement.'
            if cpol[subtask.channel].sample_number != sample_number:
                return False, task, 'Wrong sample in measurement channel.'
            return True, task, 'Success.'

        if subtask.non_channel_storage is not None:
            # no need to identify target channel
            return True, task, 'Success. Non-channel measurement has no checks.'

        # No channel or no-channel storage given. Locate the sample based on sample number. If there are multiple,
        # measure the one with the highest priority
        best_channel = None
        for i, channel_task in enumerate(cpol):
            if channel_task is not None and channel_task.sample_number == sample_number:
                if best_channel is None or cpol[best_channel].priority > cpol[i].priority:
                    best_channel = i
        if best_channel is None:
            return False, task, 'Did not find the sample to measure.'
        subtask.channel = best_channel

        return True, task, "Success."

    def pre_process_prepare(self, task: Task):
        """
        Perform checks on a preparation task given the current status of the autocontrol environment. Find free
        channels if none are given.
        :param task: the task (task.Task)
        :return: success flag, the task, response (bool, task.Task, str)
        """

        if len(task.tasks) > 1:
            # Multiple preparations per task are not supported because it is not clear how they would be assigned to
            # one sample id or sample number
            return False, task, "Multiple preparations per task not supported."
        subtask = task.tasks[0]

        if subtask.device not in self.channel_po:
            return False, task, 'Device not intialized.'

        if subtask.channel is not None:
            # no check if manual channel is already occupied and with what
            return True, task, 'Success.'

        # no channel given -> find channel
        ret, subtask, response = self.find_free_channels(subtask, task.sample_number)
        return ret, task, response

    def pre_process_transfer(self, task: Task):
        """
        Perform checks on a transfer task given the current status of the autocontrol environment. Find free
        channels if none are given.
        :param task: the task (task.Task)
        :return: success flag, the task, response (bool, task.Task, str)
        """

        def reterror(flag, subtask, i, task, resp):
            subtask.md['submission_response'] = 'Device not intialized.'
            resp += ' Subtask: {}.'.format(i+1)
            return flag, task, resp

        # A transfer task should have a source channel which is already occupied by the sample that is being
        # transferred.

        for i, subtask in enumerate(task.tasks):
            if subtask.device not in self.channel_po:
                return reterror(False, subtask, i, task, 'Device not intialized.')

            # check for consistency between non-channel and channel transfers:
            if subtask.non_channel_storage is not None and subtask.channel is not None:
                return reterror(False, subtask, i, task,
                                'Channel and non-channel storage simultaneously provided.')

            # check if source device is passive
            device_obj = self.get_device_object(subtask.device)
            if i == 0:
                if device_obj.passive:
                    return reterror(False, subtask, i, task, 'Passive device cannot initiate transfer.')

            # check on channel occupancies
            cpol = self.channel_po[subtask.device]
            sample_number = task.sample_number

            if subtask.channel is not None:
                # check if manual channel selection is valid
                if not (0 <= subtask.channel < len(self.channel_po[subtask.device])):
                    return reterror(False, subtask, i, task, 'Invalid channel number.')
                if i == 0:
                    if cpol[subtask.channel] is None:
                        # A transfer with a manual channel number can create a new sample
                        cpol[subtask.channel] = task
                        return True, task, 'Success. Created sample on transfer.'
                    elif cpol[subtask.channel].sample_number != sample_number:
                        return reterror(False, subtask, i, task, 'Wrong sample in source channel.')
                elif not device_obj.passive:
                    if cpol[subtask.channel].sample_number is not None:
                        return reterror(False, subtask, i, task, 'Device channel not empty.')
                return True, task, 'Success.'

            if subtask.non_channel_storage is not None:
                # no need to identify target channel
                return True, task, 'Success. Non-channel transfer has no checks.'

            # No channel or non-channel storage given. Find a channel.
            if i == 0:
                # Source device. Find the sample based on sample number. If there are multiple, transfer the one with
                # the highest priority.
                best_channel = None
                for j, channel_task in enumerate(cpol):
                    if channel_task is not None and channel_task.sample_number == sample_number:
                        if best_channel is None or cpol[best_channel].priority > cpol[i].priority:
                            best_channel = j
                if best_channel is None:
                    return False, task, 'Channel auto-select did not find the sample to transfer.'
                subtask.channel = best_channel
            else:
                # one of the target devices
                success, subtask, response = self.find_free_channels(subtask, task.sample_number)
                if not success:
                    return False, task, response

        return True, task, 'Success.'

    def process_task(self, task: Task):
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

        # identify the device based on the device name and check status, except for init tasks
        # there, the device might not be initialized yet
        if task.task_type != TaskType.INIT:
            device = self.get_device_object(task.tasks[0].device)
            if device is None:
                task.md['submission_response'] = 'Unknown device.'
                return False, task

            # Devices must be up or idle to submit any tasks
            for subtask in task.tasks:
                device = self.get_device_object(subtask.device)
                device_status = device.get_device_status()
                if not (device_status == Status.UP or device_status == Status.IDLE):
                    task.md['submission_response'] = 'Failure. Device status is .' + device_status.name
                    return False, task

        task_handlers = {
            TaskType.INIT: self.pre_process_init,
            TaskType.SHUTDOWN: None,
            TaskType.TRANSFER: self.pre_process_transfer,
            TaskType.PREPARE: self.pre_process_prepare,
            TaskType.MEASURE: self.pre_process_measure,
            TaskType.NOCHANNEL: None
        }
        if task.task_type not in task_handlers:
            task.md['submission_response'] = 'Unknown task type.'
            return False, task

        if task_handlers[task.task_type] is not None:
            # perform pre-processing and checks
            execute_task, task, resp = task_handlers[task.task_type](task)
        else:
            # currently no checks / pre-processing implemented
            execute_task = True
            resp = 'Success. No check performed.'

        # Check if the device and channel of the task interferes with an ongoing task of the same sample number. This is
        # another layer of protection against cases which are not caught by checks on the physical and operational
        # channel occupancies. Those checks can fail if the same sample is already present in a channel from a previous
        # step, although it (a new volume with the same sample number) is currently being transferred, or for starting
        # a measurement on a device and channel while there is a measurement still going on. It is also important for
        # transfer tasks to passive devices, which will not report on their channel activity or device status due to the
        # task.
        if execute_task and self.active_tasks.find_interference(task):
            execute_task = False
            task.md['submission_response'] = 'Waiting for ongoing task at device or channel to finish.'

        elif execute_task:
            # Note: Every task execution including measurements only sends a signal to the device and do not wait for
            # completion. Results are collected separately during self.update_active_tasks(). This allows for parallel
            # tasks.

            # add start time to metadata
            task.md['execution_start_time'] = time.gmtime(time.time())

            task_success = True
            for subtask in task.tasks:
                device = self.get_device_object(subtask.device)
                status, resp = device.execute_task(task=subtask, task_type=task.task_type)
                subtask.md['submission_response'] = resp
                if status != Status.SUCCESS:
                    task_success = False

            # TODO: There is a more elaborate exception handling required in case that one of the two devices ivolved
            #   in a transfer is returning a non-success status.
            #   For this, we need to implement abort methods and need to pull tasks already started from the instrument.
            #   Another option is implementing a hold-and-confirm logic. Or the subtasks need to self-abort after
            #   a while.

            if task_success:
                task.md['submission_response'] = 'Task successfully submitted.'
                self.active_tasks.put(task)
            else:
                execute_task = False
                task.md['submission_response'] = 'Task failed at instrument. See sub-task data.'
        else:
            task.md['submission_response'] = resp

        return execute_task, task

    def post_process_task(self, task):
        """
        Post-processes and cleans up a task that has been finished.
        :param task: The task (task.Task)
        :return: no return value
        """
        success = True
        device = self.get_device_object(task.tasks[0].device)

        if task.task_type == TaskType.INIT:
            # create an empty channel physical occupancy entry for the device (False == not occupied)
            noc = device.number_of_channels
            self.channel_po[task.tasks[0].device] = [None] * noc

        elif task.task_type == TaskType.MEASURE:
            # get measurment data

            status = device.get_device_status()
            if status != Status.IDLE and status != Status.UP:
                task.md['execution_response'] = 'Device busy or down. Cannot read out data.'
                self.active_tasks.replace(task, task.id)
                return False

            read_status, data = device.read(channel=task.tasks[0].channel)
            if read_status != Status.SUCCESS:
                task.md['execution_response'] = 'Failure reading measurement data.'
                self.active_tasks.replace(task, task.id)
                return False

            # append data to task
            task.tasks[0].md['measurement_data'] = data
            # append task id associated with measurement material to current measurement task
            task.task_history.append(self.channel_po[task.tasks[0].device][task.tasks[0].channel].id)
            # Attach measurement task to the physical occupancy list
            self.channel_po[task.tasks[0].device][task.tasks[0].channel] = task

        elif task.task_type == TaskType.PREPARE:
            # attach current task to the channel physical occupancy
            self.channel_po[task.tasks[0].device][task.tasks[0].channel] = task

        elif task.task_type == TaskType.TRANSFER:
            # transfers from channel source (as opposed to non-channel sources)
            if task.tasks[0].channel is not None:
                # append task id associated with transfer source to current transfer task
                if self.channel_po[task.tasks[0].device][task.tasks[0].channel] is not None:
                    task.task_history.append(self.channel_po[task.tasks[0].device][task.tasks[0].channel].id)
                    # remove existing task from the source channel physical occupancy
                    self.channel_po[task.tasks[0].device][task.tasks[0].channel] = None

            # transfers to channel targets
            if task.tasks[-1].channel is not None:
                # attach current task to the target channel physical occupancy
                self.channel_po[task.tasks[-1].device][task.tasks[-1].channel] = task

        # move task to history and save new channel physical occupancy
        task.md['execution_response'] = 'Success.'
        self.active_tasks.remove(task)
        self.sample_history.put(task)
        self.store_channel_po()

        return success

    def queue_inspect(self):
        """
        Returns the items of the queue in a list without removing them from the queue.
        :return: (list) the items of the queue
        """
        return self.queue.get_all()

    def store_channel_po(self):
        """
        Stores the channel physical occupancy list in the storage directory.
        :return: no return value
        """
        with open(os.path.join(self.storage_path, 'channel_po.json'), 'w') as f:
            serialized = self.channel_po.copy()
            for key in serialized:
                if serialized[key] is not None:
                    serialized[key] = [obj.json() for obj in serialized[key] if obj is not None]
            json.dump(serialized, f, indent=4)

    def queue_execute_one_item(self):
        """
        This is an external API method

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
        task_priority = [[TaskType.INIT], [TaskType.PREPARE, TaskType.TRANSFER, TaskType.MEASURE, TaskType.NOCHANNEL],
                         [TaskType.SHUTDOWN]]
        blocked_samples = []
        success = False

        i = 0
        while i < len(task_priority):
            task_type = task_priority[i]
            # retrieve job from queue
            task = self.queue.get_and_remove_by_priority(task_type=task_type, remove=False,
                                                         blocked_samples=blocked_samples)
            if task is None:
                # no job of this priority found, move on to next priority group (task type)
                i += 1
            elif task.sample_number not in blocked_samples:
                success, task = self.process_task(task)
                if success:
                    # remove task from queue
                    self.queue.remove(task_id=task.id)
                    # a succesful job ends the execution of this method
                    break
                else:
                    # this sample number is now blocked as processing of the job was not successful
                    blocked_samples.append(task.sample_number)
                    # modify the task in the queue because a submission response whas added
                    self.queue.replace(task, task_id=task.id)

        return success

    def queue_put(self, task):
        """
        This is an external API method.
        Puts a task into the priority queue.
        :param task: (task.Task) The task.
        :return: (Bool, str) success flag, descriptionn
        """

        # Check sample number and id.
        if task.sample_number is None and task.sample_id is None:
            # no number or id given, defaults to sample number 0 and default id
            task.sample_number = 1

        if task.sample_number is not None and task.sample_id is not None:
            if (task.sample_id not in self.sample_id_to_number and task.sample_number not in
                    self.sample_id_to_number.values()):
                # sample ID and number are both new
                pass
            elif task.sample_id in self.sample_id_to_number:
                # sample number and id are old
                if self.sample_id_to_number[task.sample_id] != task.sample_number:
                    return False, "Task not submitted. Sample number and ID do not match previous submission."
            else:
                return False, "Task not submitted. Sample number and ID do not match previous submission."

        elif task.sample_id is not None:
            # create a sample number if none present
            if not self.sample_id_to_number:
                task.sample_number = 1
            else:
                if task.sample_id in self.sample_id_to_number:
                    task.sample_number = self.sample_id_to_number[task.sample_id]
                else:
                    task.sample_number = max(self.sample_id_to_number.values()) + 1
        else:
            # create a sample id
            if task.sample_number in self.sample_id_to_number.values():
                sitn = self.sample_id_to_number
                task.sample_id = list(sitn.keys())[list(sitn.values()).index(task.sample_number)]
            else:
                task.sample_id = uuid.uuid4()

        self.sample_id_to_number[task.sample_id] = task.sample_number

        # create a priority value with the following importance
        # 1. Sample number
        # 2. Time that step was submitted
        # convert time to a priority <1
        p1 = time.time()/math.pow(10, math.ceil(math.log10(time.time())))
        # convert sample number to priority, always overriding start time.
        priority = task.sample_number * (-1.)
        priority -= p1
        task.priority = priority

        self.queue.put(task)
        return True, 'Task succesfully enqueued.'

    def reset(self):
        """
        This is an external API method. It wipes all tasks, channel po, and sample ID information
        :return: no return value
        """
        self.queue.clear()
        self.active_tasks.clear()
        # never delete the sample history
        # self.sample_history.clear()
        # clear channel occupancies
        for device in self.channel_po:
            for channel in range(len(self.channel_po[device])):
                self.channel_po[device][channel] = None
        self.store_channel_po()
        self.sample_id_to_number = {}

    def restart(self):
        """
        This is an external API method. It wipes all tasks, channel po, and sample ID information. It resets the
        device inits.
        :return:
        """
        self.reset()
        self.devices = {}

    def update_active_tasks(self):
        """
        This is an external API method.

        Goes through the entire list of active tasks and checks if they are completed. Follows up with clean-up and
        post-processing steps.
        :return: (Bool) flag whether a task was completed
        """
        collected = False
        task_list = self.active_tasks.get_all()
        for task in task_list:
            if self.check_task(task):
                # task is ready for collection
                if self.post_process_task(task):
                    collected = True

        return collected

