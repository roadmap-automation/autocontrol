import json
import sqlite3


class TaskContainer:
    """
    A simple storage and retrieval class for tasks used in bluesky_api.py based on SQLite.
    """
    def __init__(self, db_path=':memory:'):
        """
        Init method.
        :param db_path: An optional path for the database, if not provided, the database will be in memory
        """
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        self._create_table()

    def __del__(self):
        # close database connection
        self. conn.close()

    def _create_table(self):
        create_table_sql = """
            CREATE TABLE IF NOT EXISTS task_table (
                id INTEGER PRIMARY KEY,
                task TEXT,
                sample_number INTEGER,
                channel INTEGER,
                md TEXT,
                task_type TEXT,
                device TEXT,
                target_channel INTEGER,
                target_device TEXT
            )
        """
        self.cursor.execute(create_table_sql)
        self.conn.commit()

    def put(self, task):
        """
        Stores a task in the SQLite database
        :param task: task to store
        :return: no return value
        """
        serialized_task = json.dumps(task['task'])
        serialized_md = json.dumps(task['md'])
        query = """
            INSERT INTO task_table (
                task, sample_number, channel, md, task_type, device,
                target_channel, target_device
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        self.cursor.execute(query, (
            serialized_task, task['sample_number'], task['channel'], serialized_md,
            task['task_type'], task['device'], task['target_channel'], task['target_device']
        ))
        self.conn.commit()

    def find_channels_for_sample_number_and_device(self, task):
        """
        Find the channel of any stored task of this sample on this device and any target channels of
        transfers of this sample to other devices. Only one channel and target channel are returned (for applications
        that will reuse those channels).
        :param task:
        :return: (tuple) Found channel and target channel.
        """
        # seach for any task of this sample on this device and prioritize results of task type transfer
        # this way, if there is a transfer task with a defined target channel, it will be retrieved
        query = """
        SELECT channel, target_channel FROM task_table
        WHERE (device = ? AND sample_number = ?)
        ORDER BY CASE WHEN task_type = 'transfer' THEN 1 ELSE 2 END
        LIMIT 1
        """
        self.cursor.execute(query, (task['device'], task['sample_number']))
        result = self.cursor.fetchone()
        channel = result['channel'] if result else None
        target_channel = result['target_channel'] if result else None

        return channel, target_channel

    def find_channels_for_device(self, device_name):
        """
        Yields a list of used channels for this device based on the stored tasks
        :param device_name:
        :return: (list of int) channel numbers
        """
        query = """
        SELECT channel, target_channel FROM task_table
        WHERE device = ?
        """

        self.cursor.execute(query, (device_name, ))
        results = self.cursor.fetchall()

        # Use a set to avoid duplicate channel numbers
        channels = set()
        target_channels = set()
        for element in results:
            channels.add(element['channel'])
            target_channels.add(element['target_channel'])

        return list(channels), list(target_channels)

    def remove_by_channel(self, device_name, channel_list):
        """
        Removes all tasks of a certain device that use channels given in the channel list
        :param device_name: device name
        :param channel_list: list of channels
        :return: no return value
        """
        # Create a string with placeholders for each item in channel_list
        placeholders = ', '.join('?' * len(channel_list))

        query = f"""
        DELETE FROM task_table
        WHERE device = ? AND channel IN ({placeholders})
        """

        # Create a tuple of parameters including device_name and all channels
        parameters = (device_name,) + tuple(channel_list)

        self.cursor.execute(query, parameters)
        self.conn.commit()





