import json
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
                id INTEGER PRIMARY KEY,
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
        result = cursor.fetchone()

        # Use a set to avoid duplicate channel numbers
        channels = set()
        if result is not None:
            for element in result:
                tsk = task_struct.Task.parse_raw(element)
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
        if result is not None:
            for entry in result:
                # deserialize tasks and append to results list
                ret.append(task_struct.Task.parse_raw(entry[0]))

        cursor.close()
        conn.close()
        self.lock.release()

        return ret

    def get_and_remove_by_priority(self, task_type=None, remove=True):
        """
        Retrieves the highest priority item from the container. If the task type is provided it will return the highest
        priority item with the given task type. If there is no match or the container is empty, returns None.
        :param task_type: (str or list) task type or list of task types
        :param remove: (bool) flag whether to remove the highest priority item from the queue
        :return: item or None
        """

        self.lock.acquire()
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

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
        serialized_task = task.json()

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
        Removes a task from the SQLite database using the unique 'priority' field of the task
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
