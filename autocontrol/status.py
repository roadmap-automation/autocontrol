from enum import Enum


class Status(int, Enum):
    SUCCESS = 0
    ERROR = 1
    WARNING = 2
    BUSY = 3
    INVALID = 4
    TODO = 5
    IDLE = 6
    UP = 7
    DOWN = 8


def get_status_member(status_str):
    """
    Returns a Status member from a string.
    :param status_str: string representation
    :return: Status member
    """
    return Status.__members__.get(status_str.upper(), None)
