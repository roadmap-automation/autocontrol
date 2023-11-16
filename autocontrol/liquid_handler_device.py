import numpy as np
from ophyd.device import Component as Cpt
from ophyd.device import Device
from ophyd.signal import Signal, DEFAULT_WRITE_TIMEOUT
from ophyd.status import DeviceStatus, MoveStatus, Status
from ophyd.utils import ReadOnlyError
import threading
import time as ttime
import warnings


class _ReadbackSignal(Signal):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._metadata.update(
            connected=True,
            write_access=False,
        )

    def get(self):
        # TODO implement readback from LH
        self._readback = 'not implemented'
        return self._readback

    def describe(self):
        res = super().describe()
        # There should be only one key here, but for the sake of
        # generality....
        for k in res:
            res[k]["precision"] = self.parent.precision
        return res

    def put(self, value, *, timestamp=None, force=False):
        raise ReadOnlyError("The signal {} is readonly.".format(self.name))

    def set(self, value, *, timestamp=None, force=False):
        raise ReadOnlyError("The signal {} is readonly.".format(self.name))


class _SetpointSignal(Signal):
    def put(self, value, *, timestamp=None, force=False, metadata=None, timeout=DEFAULT_WRITE_TIMEOUT, **kwargs):
        self._readback = value
        # TODO: implement LH put here

    def get(self):
        return self._readback

    def describe(self):
        res = super().describe()
        # There should be only one key here, but for the sake of generality....
        for k in res:
            res[k]["precision"] = self.parent.precision
        return res


class lh_device(Device):
    """
    A synthetic settable Device mimic any 1D Axis (position, temperature).

    Parameters
    ----------
    name : string, keyword only

    value : object, optional
        The initial value. Default is 0.

    precision : integer, optional
        Digits of precision. Default is 3.
    parent : Device, optional
        Used internally if this Signal is made part of a larger Device.
    kind : a member the Kind IntEnum (or equivalent integer), optional
        Default is Kind.normal. See Kind for options.
    """

    readback = Cpt(_ReadbackSignal, value=0, kind="hinted")
    setpoint = Cpt(_SetpointSignal, value=0, kind="normal")

    unused = Cpt(Signal, value=1, kind="omitted")

    SUB_READBACK = "readback"
    _default_sub = SUB_READBACK

    def __init__(
        self,
        *,
        name,
        value=0,
        delay=0,
        precision=3,
        parent=None,
        labels=None,
        kind=None,
        **kwargs,
    ):

        sentinel = object()
        loop = kwargs.pop("loop", sentinel)
        if loop is not sentinel:
            warnings.warn(
                f"{self.__class__} no longer takes a loop as input.  "
                "Your input will be ignored and may raise in the future",
                stacklevel=2,
            )

        self.delay = delay
        self.precision = precision

        super().__init__(name=name, parent=parent, labels=labels, kind=kind, **kwargs)
        self.readback.name = self.name

    def set(self, value) -> Status:
        # TODO: See if the set can be moved to the setpoint signal
        def sleep_and_finish(value):
            print('Write command to LH')
            self.setpoint.put(value)
            st.set_finished()

        # create status object (What is the associated timeout?)
        st = Status(timeout=60)
        threading.Thread(target=sleep_and_finish, args=[value], daemon=True).start()

        return st

    @property
    def position(self):
        return self.readback.get()


# motor1 = SynAxis(name="motor1", labels={"motors"})
