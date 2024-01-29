import json
import sqlite3
import threading


class TaskContainer:
    """
    A simple storage and retrieval class for tasks used in autocontrol.py based on SQLite.
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
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        create_table_sql = """
            CREATE TABLE IF NOT EXISTS task_table (
                id INTEGER PRIMARY KEY,
                priority REAL,
                sample_number INTEGER,
                device TEXT,
                task_type TEXT,
                channel INTEGER,
                task TEXT,
                target_channel INTEGER,
                target_device TEXT,
                md TEXT
            )
        """
        cursor.execute(create_table_sql)
        conn.commit()

        cursor.close()
        conn.close()
        self.lock.release()

    def find_channels_per_device(self, task, sample=True):
        """
        Find the channel of any stored task of this sample on this device and any target channels of
        transfers of this sample to other devices. Only one channel and target channel are returned (for applications
        that will reuse those channels).
        :param task: (task_container) the reference task
        :param sample: (bool) only consider the channels that were used by the same sample (number)
        :return: (tuple) Found channel and target channel.
        """

        self.lock.acquire()
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # seach for any task of this sample on this device and prioritize results of task type transfer
        # this way, if there is a transfer task with a defined target channel, it will be retrieved
        if sample:
            query = """
            SELECT channel, target_channel FROM task_table
            WHERE (device = ? AND sample_number = ?)
            ORDER BY CASE WHEN task_type = 'transfer' THEN 1 ELSE 2 END
            LIMIT 1
            """
            cursor.execute(query, (task['device'], task['sample_number']))
        else:
            query = """
            SELECT channel, target_channel FROM task_table
            WHERE (device = ?)
            ORDER BY CASE WHEN task_type = 'transfer' THEN 1 ELSE 2 END
            LIMIT 1
            """
            cursor.execute(query, (task['device']))

        result = cursor.fetchone()

        if result is not None:
            # make result into dictionary
            desc = cursor.description
            column_names = [col[0] for col in desc]
            data = dict(zip(column_names, result))
            result = data

            channel = result['channel']
            target_channel = result['target_channel']
        else:
            channel = None
            target_channel = None

        cursor.close()
        conn.close()
        self.lock.release()

        return channel, target_channel

    def find_channels_for_device(self, device_name):
        """
        Yields a list of used channels for this device based on the stored tasks
        :param device_name:
        :return: (list of int) channel numbers
        """

        self.lock.acquire()
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        query = """
        SELECT channel, target_channel FROM task_table
        WHERE device = ?
        """

        cursor.execute(query, (device_name, ))
        result = cursor.fetchall()

        channels = set()
        target_channels = set()
        if result is not None:
            # make result into dictionary
            desc = cursor.description
            column_names = [col[0] for col in desc]
            data = [dict(zip(column_names, row)) for row in result]
            result = data

            # Use a set to avoid duplicate channel numbers
            for element in result:
                channels.add(element['channel'])
                target_channels.add(element['target_channel'])

        cursor.close()
        conn.close()
        self.lock.release()

        return list(channels), list(target_channels)

    def get_all(self):
        """
        Retrieves all items from the container.
        :return: list of items
        """

        self.lock.acquire()
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        query = "SELECT * FROM task_table"
        cursor.execute(query)
        result = cursor.fetchall()

        if result is not None:
            # make result into dictionary
            desc = cursor.description
            column_names = [col[0] for col in desc]
            data = [dict(zip(column_names, row)) for row in result]
            result = data
            for entry in result:
                entry['task'] = json.loads(entry['task'])
                entry['md'] = json.loads(entry['md'])

        cursor.close()
        conn.close()
        self.lock.release()

        return result

    def get_and_remove_by_priority(self, task_type=None):
        """
        Retrieves the highest priority item from the container. If the task type is provided it will return the highest
        priority item with the given task type. If there is no match or the container is empty, returns None.
        :param task_type: (str or list) task type or list of task types
        :return: item or None
        """

        self.lock.acquire()
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        if task_type is None:
            query = "SELECT * FROM task_table ORDER BY priority DESC LIMIT 1"
        elif isinstance(task_type, str):
            query = "SELECT * FROM task_table WHERE task_type='" + task_type + "' ORDER BY priority DESC LIMIT 1"
        elif isinstance(task_type, list):
            task_type_str = "','".join(task_type)
            query = ("SELECT * FROM task_table WHERE task_type IN ('" + task_type_str +
                     "') ORDER BY priority DESC LIMIT 1")
        else:
            cursor.close()
            conn.close()
            self.lock.release()
            return None

        cursor.execute(query)
        result = cursor.fetchone()

        # remove retrieved item
        if result is not None:
            # make result into dictionary
            desc = cursor.description
            column_names = [col[0] for col in desc]
            data = dict(zip(column_names, result))
            result = data
            result['task'] = json.loads(result['task'])
            result['md'] = json.loads(result['md'])

            cursor.execute("DELETE FROM task_table WHERE id=:id", {'id': result['id']})
            conn.commit()

        cursor.close()
        conn.close()
        self.lock.release()

        return result

    def put(self, task):
        """
        Stores a task in the SQLite database
        :param task: task to store
        :return: no return value
        """

        self.lock.acquire()
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        serialized_task = json.dumps(task['task'])
        serialized_md = json.dumps(task['md'])

        query = """
            INSERT INTO task_table (
                task, priority, sample_number, channel, md, task_type, device,
                target_channel, target_device
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        cursor.execute(query, (
            serialized_task, task['priority'], task['sample_number'], task['channel'], serialized_md,
            task['task_type'], task['device'], task['target_channel'], task['target_device']
        ))
        conn.commit()

        cursor.close()
        conn.close()
        self.lock.release()

    def remove(self, task):
        """
        Removes a task from the SQLite database using the unique 'priority' field of the task
        :param task: Task to remove from the SQLite database
        :return: no return value
        """
        self.lock.acquire()
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("DELETE FROM task_table WHERE priority=:priority",
                       {'priority': task['priority']})
        conn.commit()

        cursor.close()
        conn.close()
        self.lock.release()

    def remove_by_channel(self, device_name, channel_list):
        """
        Removes all tasks of a certain device that use channels given in the channel list
        :param device_name: device name
        :param channel_list: list of channels
        :return: no return value
        """

        self.lock.acquire()
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Create a string with placeholders for each item in channel_list
        placeholders = ', '.join('?' * len(channel_list))

        query = f"""
        DELETE FROM task_table
        WHERE device = ? AND channel IN ({placeholders})
        """

        # Create a tuple of parameters including device_name and all channels
        parameters = (device_name,) + tuple(channel_list)

        cursor.execute(query, parameters)
        conn.commit()

        cursor.close()
        conn.close()
        self.lock.release()





