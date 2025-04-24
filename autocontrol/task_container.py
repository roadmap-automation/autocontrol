import sqlite3
import threading

import autocontrol.task_struct as task_struct


class TaskContainer:
    """
    A simple storage and retrieval class for tasks used in atc.py based on SQLite.
    """
    def __init__(self, db_path=':memory:'):
        """
        Init method.
        :param db_path: An optional path for the database, if not provided, the database will be in memory
        """

        self.db_path = db_path
        self.lock = threading.Lock()

        self._create_table()

    def empty(self):
        """
        Tests if the task container is empty.
        :return: (bool) True if the task container is empty, False otherwise.
        """
        self.lock.acquire()
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT count(*) FROM (select 1 from task_table limit 1);")
        result = cursor.fetchall()[0][0]

        cursor.close()
        conn.close()
        self.lock.release()

        if result == 0:
            return True
        else:
            return False

    def _create_table(self):

        self.lock.acquire()
        # note: creates a new db file if it does not exist
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        create_table_sql = """
            CREATE TABLE IF NOT EXISTS task_table (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id TEXT,
                sample_id TEXT,
                priority REAL,
                sample_number INTEGER,
                device TEXT,
                task_type TEXT,
                channel INTEGER,
                task TEXT,
                target_channel INTEGER,
                target_device TEXT
            )
        """
        cursor.execute(create_table_sql)
        conn.commit()

        cursor.close()
        conn.close()
        self.lock.release()

    def clear(self):
        """
        Clears the task container.
        :return: no return value
        """
        self.lock.acquire()
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("DELETE FROM task_table;")
        conn.commit()

        cursor.close()
        conn.close()
        self.lock.release()

    def find_channels(self, sample_number=None, device_name=None):
        """
        Find the used channels of all stored subtask given the device provided by the reference subtask. If enabled,
        the results will be further filtered by the sample_number provided.
        :param sample_number: (int) only consider the channels that were used by the same sample (number)
        :param device_name: (str) only consider the channels on the given device
        :return: (list) busy channels
        """

        self.lock.acquire()
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # seach for any task of this sample on this device and prioritize results of task type transfer
        # this way, if there is a transfer task with a defined target channel, it will be retrieved
        if sample_number is not None:
            query = """
            SELECT task FROM task_table
            WHERE (sample_number = ?)
            """
            cursor.execute(query, sample_number)
        else:
            query = """SELECT task FROM task_table"""
            cursor.execute(query)
        result = cursor.fetchall()

        # Use a set to avoid duplicate channel numbers
        channels = set()
        for element in result:
            tsk = task_struct.Task.parse_raw(element[0])
            for subtask in tsk.tasks:
                if device_name is None or subtask.device == device_name:
                    if subtask.channel is not None:
                        channels.add(subtask.channel)

        cursor.close()
        conn.close()
        self.lock.release()

        return list(channels)

    def find_interference(self, task):
        """
        Checks if a task is interfering with an existing task on the same (target) device and (target) channel.
        :param task: (task_struct.Task) task to check
        :return: (bool) True if task is interfering
        """

        for subtask in task.tasks:
            busy_channels = self.find_channels(device_name=subtask.device)
            if subtask.channel in busy_channels:
                return True
        return False

    def get_all(self):
        """
        Retrieves all items from the container.
        :return: list of items
        """

        self.lock.acquire()
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        query = "SELECT task FROM task_table"
        cursor.execute(query)
        result = cursor.fetchall()

        ret = []
        for entry in result:
            # deserialize tasks and append to results list
            ret.append(task_struct.Task.parse_raw(entry[0]))

        cursor.close()
        conn.close()
        self.lock.release()

        return ret

    def get_and_remove_by_priority(self, task_type=None, remove=True, blocked_samples=None):
        """
        Retrieves the highest priority item from the container. If the task type is provided it will return the highest
        priority item with the given task type. If there is no match or the container is empty, returns None.
        :param task_type: (str or list) task type or list of task types
        :param remove: (bool) flag whether to remove the highest priority item from the queue
        :param blocked_samples: (list) list of blocked sample numbers that are not to be retrieved
        :return: item or None
        """

        self.lock.acquire()
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        if blocked_samples is None:
            if task_type is None:
                query = "SELECT task FROM task_table ORDER BY priority DESC LIMIT 1"
            elif isinstance(task_type, str):
                query = "SELECT task FROM task_table WHERE task_type='" + task_type + "' ORDER BY priority DESC LIMIT 1"
            elif isinstance(task_type, list):
                task_type_str = "','".join(task_type)
                query = ("SELECT task FROM task_table WHERE task_type IN ('" + task_type_str +
                         "') ORDER BY priority DESC LIMIT 1")
            else:
                cursor.close()
                conn.close()
                self.lock.release()
                return None
        else:
            bss = [str(i) for i in blocked_samples]
            blocked_samples_str = "','".join(bss)
            if task_type is None:
                query = (f"SELECT task FROM task_table WHERE sample_number NOT IN ('{blocked_samples_str}') "
                         f"ORDER BY priority DESC LIMIT 1")
            elif isinstance(task_type, str):
                query = (f"SELECT task FROM task_table WHERE task_type='{task_type}' AND sample_number NOT IN "
                         f"('{blocked_samples_str}') ORDER BY priority DESC LIMIT 1")
            elif isinstance(task_type, list):
                task_type_str = "','".join(task_type)
                query = (f"SELECT task FROM task_table WHERE task_type IN ('{task_type_str}') AND sample_number NOT IN "
                         f"('{blocked_samples_str}') ORDER BY priority DESC LIMIT 1")
            else:
                cursor.close()
                conn.close()
                self.lock.release()
                return None

        cursor.execute(query)
        result = cursor.fetchone()

        # remove retrieved item
        ret = None
        if result is not None:
            # there is ever only one item in this tuple
            ret = task_struct.Task.parse_raw(result[0])

            # remove task if flag is set
            if remove:
                cursor.execute("DELETE FROM task_table WHERE task_id=:id", {'id': str(ret.id)})
                conn.commit()

        cursor.close()
        conn.close()
        self.lock.release()

        return ret

    def get_future_devices(self, sample_number, device_name, channel=None):
        """
        Retrieves a list of future devices for the given sample number. That is currently present in a certain device.
        :param sample_number: the sample number
        :param device_name: the device name
        :param channel: the channel
        :return: list of tuples of device names and channels, or an empty set if there are no future devices
        """

        device_set = set()
        current_device = device_name
        current_channel = channel

        self.lock.acquire()
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT task FROM task_table WHERE sample_number=:sample_number AND task_type='transfer' ",
                       {'sample_number': int(sample_number)})
        result = cursor.fetchall()

        ret = []
        for entry in result:
            # deserialize tasks and append to results list
            ret.append(task_struct.Task.parse_raw(entry[0]))

        cursor.close()
        conn.close()
        self.lock.release()

        if not ret:
            return []

        # find the first path of the sample through the network
        for task in ret:
            if task.tasks[0].device == current_device:
                if current_channel is None or current_channel == task.tasks[0].channel:
                    for subtask in task.tasks:
                        current_device = subtask.device
                        current_channel = subtask.channel
                        device_set.add((current_device, current_channel))

        return list(device_set)

    def get_lowest_sample_number(self):
        """
        Retrieves the lowest sample number from the container.
        :return: sample number
        """

        self.lock.acquire()
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT MIN(sample_number) FROM task_table")
        min_sample_number = cursor.fetchone()[0]

        cursor.close()
        conn.close()
        self.lock.release()

        return min_sample_number

    def get_task_by_id(self, task_id):
        """
        Retrieves a task by its ID without removing it from the container.
        :param task_id: (str or UUID4) the task id
        :return: the task or None
        """
        self.lock.acquire()
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT task FROM task_table WHERE task_id=:id", {'id': str(task_id)})
        result = cursor.fetchone()
        if result is not None:
            # there is ever only one item in this tuple
            result = task_struct.Task.parse_raw(result[0])

        cursor.close()
        conn.close()
        self.lock.release()

        return result

    def get_task_by_sample_number(self, sample_number, single=False):
        """
        Retrieves all tasks with the same sample number from the container.
        :param sample_number: sample number
        :param single: if True, only one task will be returned
        :return: list of tasks or None
        """

        self.lock.acquire()
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT task FROM task_table WHERE sample_number=:sample_number",
                       {'sample_number': int(sample_number)})

        if single:
            result = cursor.fetchone()
            if result is not None:
                ret = [task_struct.Task.parse_raw(result[0])]
            else:
                ret = None
        else:
            result = cursor.fetchall()
            if result:
                ret = []
                for entry in result:
                    # deserialize tasks and append to results list
                    ret.append(task_struct.Task.parse_raw(entry[0]))
            else:
                ret = None

        cursor.close()
        conn.close()
        self.lock.release()

        return ret

    def put(self, task):
        """
        Stores a task in the SQLite database
        :param task: task to store
        :return: no return value
        """

        self.lock.acquire()
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # serialize the entire object and save it extracting some parameters of immediate interest to autocontrol
        serialized_task = task.model_dump_json(indent=2)

        # The target channel and device are endpoints of a multistep transfer. Autocontrol is not currently not
        # concerned with assigning channels for intermediate devices.
        # TODO: Not sure that the device name needs to be presented at the top level anymore
        query = """
            INSERT INTO task_table (
                task, priority, task_id, sample_id, sample_number, channel, task_type, device, target_channel, 
                target_device
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        cursor.execute(query, (
            serialized_task, task.priority, str(task.id), str(task.sample_id), task.sample_number,
            task.tasks[0].channel, task.task_type, task.tasks[0].device, task.tasks[-1].channel, task.tasks[-1].device
        ))
        conn.commit()

        cursor.close()
        conn.close()
        self.lock.release()

    def remove(self, task=None, task_id=None):
        """
        Removes a task from the SQLite database using the unique 'priority' field.
        :param task: The task to remove
        :param task_id: (uuid) ID of the task to remove
        :return: no return value
        """
        # Do not do anything if missing or contradicting IDs
        if task is None and task_id is None:
            return
        if task is not None and task_id is not None and task.id != task_id:
            return

        if task is not None:
            task_id = task.id

        self.lock.acquire()
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("DELETE FROM task_table WHERE task_id=:id", {'id': str(task_id)})
        conn.commit()

        cursor.close()
        conn.close()
        self.lock.release()

    def replace(self, task, task_id=None):
        """
        Replaces a task in the SQLite database using the unique 'task_id' field of the task
        :param task: the replacement
        :param task_id: ID
        :return: no return value
        """
        if task_id is None:
            return
        self.remove(task_id=task_id)
        self.put(task=task)
        return
