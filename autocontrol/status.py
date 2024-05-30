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


def get_status_member(status_input):
    """
    Returns a Status member from a string or integer.
    :param status_input: string or integer representation
    :return: Status member
    """
    if isinstance(status_input, int):
        try:
            return Status(status_input)
        except ValueError:
            return None
    elif isinstance(status_input, str):
        return Status.__members__.get(status_input.upper(), None)
    else:
        return None

