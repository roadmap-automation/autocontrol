from pydantic import BaseModel, field_validator, Field, field_serializer
import pydantic
from typing import Type, Optional, List
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
    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    device: str = ''
    channel: Optional[int] = None
    method_data: Optional[dict] = Field(default_factory=dict)
    md: Optional[dict] = Field(default_factory=dict)
    device_address: Optional[str] = None
    channel_mode: Optional[int] = None
    number_of_channels: int = 1
    acquisition_time: Optional[float] = None
    force: bool = False
    # not sure whether those are needed, this information also could be part of the transfer method
    target_device: Optional[str] = None
    target_channel: Optional[int] = None
    source_device: Optional[str] = None
    source_channel: Optional[int] = None
    non_channel_source: Optional[str] = None
    non_channel_target: Optional[str] = None

    wait_for_queue_to_empty: bool = True


class Task(BaseModel):
    """
    Base object for an autocontrol task. Priority and sample number are optional at the
    time of submission. Tasks contain typically one item, except for transfer tasks where for
    each involved a separate TaskData object is attached. The task history contains the uuid values
    of all previous tasks acting on this particular sub-sample
    """
    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    sample_id: Optional[uuid.UUID] = Field(default_factory=uuid.uuid4)
    priority: Optional[float] = None
    sample_number: Optional[int] = None
    tasks: List[TaskData] = Field(default_factory=list)
    task_type: TaskType
    task_history: List[uuid.UUID] = Field(default_factory=list)
