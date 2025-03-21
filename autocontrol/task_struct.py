from pydantic import BaseModel, Field
from typing import Optional, List
from enum import Enum
import uuid


class TaskType(str, Enum):
    NONE = 'none'
    INIT = 'init'
    PREPARE = 'prepare'
    TRANSFER = 'transfer'
    MEASURE = 'measure'
    NOCHANNEL = 'nochannel'
    SHUTDOWN = 'shutdown'


class TaskData(BaseModel):
    """
    TaskData contains information for sub-tasks making up a task. For all tasks except
    transfers there is usually only one subtask. The task_type usually agrees with that
    of the task. Autocontrol will route the method_data without change to the device. Metadata
    is stored per sub-task and will be later aggregated.
    """
    # Note: I decided against an earlier implementation to subclass TaskData because I found it difficult to come
    # up with easy and legible implementations of such subclasses in autocontrol. That might be revisited in the future.

    # general fields
    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    device: str = ''
    channel: Optional[int] = None
    method_data: Optional[dict] = Field(default_factory=dict)
    md: dict = Field(default_factory=dict)

    # for init tasks
    device_type: Optional[str] = None
    device_address: Optional[str] = None
    channel_mode: Optional[int] = None
    number_of_channels: Optional[int] = None
    simulated: bool = False
    sample_mixing: bool = True

    # for measurement tasks
    acquisition_time: Optional[float] = None

    # for transfer tasks
    non_channel_storage: Optional[str] = None

    # for shutdown tasks
    wait_for_queue_to_empty: bool = True


class Task(BaseModel):
    """
    Base object for an autocontrol task. Priority and sample number are optional at the
    time of submission. Tasks contain typically one item, except for transfer tasks where for
    each involved a separate TaskData object is attached. The task history contains the uuid values
    of all previous tasks acting on this particular sub-sample
    """
    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    md: dict = Field(default_factory=dict)
    priority: Optional[float] = None
    sample_id: Optional[uuid.UUID] = None
    sample_number: Optional[int] = None
    tasks: List[TaskData] = Field(default_factory=list)
    task_history: List[uuid.UUID] = Field(default_factory=list)
    task_type: TaskType

    dependency_id: Optional[uuid.UUID] = None
    dependency_sample_number: Optional[int] = None
