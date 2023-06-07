import json
import numpy as np
from ophyd.device import Component as Cpt
from ophyd.device import Device
from ophyd.signal import Signal
from ophyd.status import DeviceStatus, MoveStatus
from ophyd.utils import ReadOnlyError
import requests
import threading
import time as ttime
import warnings

class EnumSignal(Signal):
    def __init__(self, *args, value=0, enum_strings, **kwargs):
        super().__init__(*args, value=0, **kwargs)
        self._enum_strs = tuple(enum_strings)
        self._metadata["enum_strs"] = tuple(enum_strings)
        self.put(value)

    def put(self, value, **kwargs):
        if value in self._enum_strs:
            value = self._enum_strs.index(value)
        elif isinstance(value, str):
            err = f"{value} not in enum strs {self._enum_strs}"
            raise ValueError(err)
        return super().put(value, **kwargs)

    def get(self, *, as_string=True, **kwargs):
        """
        Implement getting as enum strings
        """
        value = super().get()

        if as_string:
            if self._enum_strs is not None and isinstance(value, int):
                return self._enum_strs[value]
            elif value is not None:
                return str(value)
        return value

    def describe(self):
        desc = super().describe()
        desc[self.name]["enum_strs"] = self._enum_strs
        return desc


class QCMDSignal(Signal):
    """
    A synthetic Signal that evaluates a Python function when triggered.

    Parameters
    ----------
    func : callable, optional
        This function sets the signal to a new value when it is triggered.
        Expected signature: ``f() -> value``.
        By default, triggering the signal does not change the value.
    name : string, keyword only
    exposure_time : number, optional
        Seconds of delay when triggered (simulated 'exposure time'). Default is
        0.
    precision : integer, optional
        Digits of precision. Default is 3.
    parent : Device, optional
        Used internally if this Signal is made part of a larger Device.
    kind : a member the Kind IntEnum (or equivalent integer), optional
        Default is Kind.normal. See Kind for options.

    """

    # This signature is arranged to mimic the signature of EpicsSignal, where
    # the Python function (func) takes the place of the PV.
    def __init__(self, func=None, *, name, address=None, exposure_time=0, precision=3, parent=None, labels=None,
                 kind=None, **kwargs):
        if func is None:
            # When triggered, just put the current value.
            func = self.get
            # Initialize readback with 0.
            self._readback = 0
        sentinel = object()
        loop = kwargs.pop("loop", sentinel)
        if loop is not sentinel:
            warnings.warn(
                f"{self.__class__} no longer takes a loop as input.  "
                "Your input will be ignored and may raise in the future",
                stacklevel=2,
            )
        self._func = self.qcmd_read
        self.exposure_time = exposure_time
        self.address = address
        self.precision = precision
        super().__init__(value=self._func(), timestamp=ttime.time(), name=name, parent=parent, labels=labels, kind=kind,
                         **kwargs)
        self._metadata.update(
            connected=True,
        )

    def describe(self):
        res = super().describe()
        # There should be only one key here, but for the sake of generality....
        for k in res:
            res[k]["precision"] = self.precision
        return res

    def trigger(self):
        st = DeviceStatus(device=self)

        # if QCMD is busy, do not start new measurement
        qcmd_status = self.qcmd_status()
        if qcmd_status != 'idle':
            exc = Exception("QCMD not idle.")
            st.set_exception(exc)
            return st

        delay_time = self.exposure_time
        if delay_time:
            def sleep_and_finish():
                self.log.debug("sleep_and_finish %s", self)
                self.qcmd_start()
                ttime.sleep(delay_time)
                self.put(self._func())
                self.qcmd_stop()
                st.set_finished()

            threading.Thread(target=sleep_and_finish, daemon=True).start()
        else:
            self.put(self._func())
            st.set_finished()
        return st

    def qcmd_read(self):
        """
        Establishes an HTTP connection to the QCMD Qt app and retrieves the current data. At the moment, current data
        is the entire data set collected since start. Thereby, qcmd_read should be called only once after stopping the
        run.

        :return: QCMD data as a list of scalars and lists
                 General format of the returned list:
                 0 - starttime
                 1 - relative time (list, one for each data point)
                 2 - frequency (list of lists, one for each channel and data point)
                 3 - dissipation (list of lists)
                 4 - temperature (list)
        """

        # single frequency dummy data for null returns since None does not seem to be an option for bluesky
        dvalue = [0., [0.], [[0., 0., 0., 0.]], [[0., 0., 0., 0.]], [0.]]

        if self.address is None:
            return dvalue
        cmdstr = '{"command": "get_data", "value": 0}'
        r = requests.post(self.address, cmdstr)
        rdict = json.loads(r.text)
        rrdict = rdict['result']

        if rrdict is None:
            rvalue = dvalue
        else:
            # TODO: Needs to be tested under live conditions. I am not sure whether rrdict is dict or json after
            # first unpacking, assume it is dict here:

            # TODO: Check type compatibility of starttime with bluesky, if incompatible change here or in QCMD app
            starttime = rrdict['starttime']
            relative_time = rrdict['relative_time']
            frequencies = rrdict['frequencies']
            dissipation = rrdict['dissipation']
            temperature = rrdict['temperature']

            rvalue = [starttime, relative_time, frequencies, dissipation, temperature]

        return rvalue

    def qcmd_start(self):
        """
        Starts the QCM-D data acquisition.

        :return: no return value
        """
        if self.address is None:
            return
        cmdstr = '{"command": "start", "value": 0}'
        _ = requests.post(self.address, cmdstr)

    def qcmd_status(self):
        """
        Retrieves the QCM-D status.

        :return: no return value
        """
        if self.address is None:
            return 'no connection'

        cmdstr = '{"command": "get_status", "value": 0}'
        r = requests.post(self.address, cmdstr)
        rdict = json.loads(r.text)
        rstring = rdict['result']

        return rstring

    def qcmd_stop(self):
        """
        Stops the QCM-D data acquisition.

        :return: no return value
        """
        if self.address is None:
            return
        cmdstr = '{"command": "stop", "value": 0}'
        _ = requests.post(self.address, cmdstr)


class _ReadbackSignal(Signal):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._metadata.update(
            connected=True,
            write_access=False,
        )

    def get(self):
        self._readback = self.parent.sim_state["readback"]
        return self._readback

    def describe(self):
        res = super().describe()
        # There should be only one key here, but for the sake of
        # generality....
        for k in res:
            res[k]["precision"] = self.parent.precision
        return res

    @property
    def timestamp(self):
        """Timestamp of the readback value"""
        return self.parent.sim_state["readback_ts"]

    def put(self, value, *, timestamp=None, force=False):
        raise ReadOnlyError("The signal {} is readonly.".format(self.name))

    def set(self, value, *, timestamp=None, force=False):
        raise ReadOnlyError("The signal {} is readonly.".format(self.name))


class _SetpointSignal(Signal):
    def put(self, value, *, timestamp=None, force=False):
        self._readback = float(value)
        self.parent.set(float(value))

    def get(self):
        self._readback = self.parent.sim_state["setpoint"]
        return self.parent.sim_state["setpoint"]

    def describe(self):
        res = super().describe()
        # There should be only one key here, but for the sake of generality....
        for k in res:
            res[k]["precision"] = self.parent.precision
        return res

    @property
    def timestamp(self):
        """Timestamp of the readback value"""
        return self.parent.sim_state["setpoint_ts"]


class SynAxis(Device):
    """
    A synthetic settable Device mimic any 1D Axis (position, temperature).

    Parameters
    ----------
    name : string, keyword only
    readback_func : callable, optional
        When the Device is set to ``x``, its readback will be updated to
        ``f(x)``. This can be used to introduce random noise or a systematic
        offset.
        Expected signature: ``f(x) -> value``.
    value : object, optional
        The initial value. Default is 0.
    delay : number, optional
        Simulates how long it takes the device to "move". Default is 0 seconds.
    precision : integer, optional
        Digits of precision. Default is 3.
    parent : Device, optional
        Used internally if this Signal is made part of a larger Device.
    kind : a member the Kind IntEnum (or equivalent integer), optional
        Default is Kind.normal. See Kind for options.
    events_per_move: number of events to push to a Status object for each move.
        Must be at least 1, more than one will give "moving" statuses that can be
        used for progress bars etc.
        Default is 1.
    """

    readback = Cpt(_ReadbackSignal, value=0, kind="hinted")
    setpoint = Cpt(_SetpointSignal, value=0, kind="normal")

    velocity = Cpt(Signal, value=1, kind="config")
    acceleration = Cpt(Signal, value=1, kind="config")

    unused = Cpt(Signal, value=1, kind="omitted")

    SUB_READBACK = "readback"
    _default_sub = SUB_READBACK

    def __init__(
        self,
        *,
        name,
        readback_func=None,
        value=0,
        delay=0,
        precision=3,
        parent=None,
        labels=None,
        kind=None,
        events_per_move: int = 1,
        egu: str = "mm",
        **kwargs,
    ):
        if readback_func is None:

            def readback_func(x):
                return x

        sentinel = object()
        loop = kwargs.pop("loop", sentinel)
        if loop is not sentinel:
            warnings.warn(
                f"{self.__class__} no longer takes a loop as input.  "
                "Your input will be ignored and may raise in the future",
                stacklevel=2,
            )
        self.sim_state = {}
        self._readback_func = readback_func
        self.delay = delay
        self.precision = precision

        # initialize values
        self.sim_state["setpoint"] = value
        self.sim_state["setpoint_ts"] = ttime.time()
        self.sim_state["readback"] = readback_func(value)
        self.sim_state["readback_ts"] = ttime.time()

        super().__init__(name=name, parent=parent, labels=labels, kind=kind, **kwargs)
        self.readback.name = self.name
        if events_per_move < 1:
            raise ValueError("At least 1 event per move is required")
        self._events_per_move = events_per_move
        self.egu = egu

    def _make_status(self, target: float):
        return MoveStatus(positioner=self, target=target)

    def set(self, value: float) -> MoveStatus:
        old_setpoint = self.sim_state["setpoint"]
        distance = value - old_setpoint
        self.sim_state["setpoint"] = value
        self.sim_state["setpoint_ts"] = ttime.time()
        self.setpoint._run_subs(
            sub_type=self.setpoint.SUB_VALUE,
            old_value=old_setpoint,
            value=self.sim_state["setpoint"],
            timestamp=self.sim_state["setpoint_ts"],
        )

        def update_state(position: float) -> None:
            old_readback = self.sim_state["readback"]
            self.sim_state["readback"] = self._readback_func(position)
            self.sim_state["readback_ts"] = ttime.time()
            self.readback._run_subs(
                sub_type=self.readback.SUB_VALUE,
                old_value=old_readback,
                value=self.sim_state["readback"],
                timestamp=self.sim_state["readback_ts"],
            )
            self._run_subs(
                sub_type=self.SUB_READBACK,
                old_value=old_readback,
                value=self.sim_state["readback"],
                timestamp=self.sim_state["readback_ts"],
            )

        st = self._make_status(target=value)

        def sleep_and_finish():
            event_delay = self.delay / self._events_per_move
            for i in range(self._events_per_move):
                if self.delay:
                    ttime.sleep(event_delay)
                position = old_setpoint + (distance * ((i + 1) / self._events_per_move))
                update_state(position)
            st.set_finished()

        threading.Thread(target=sleep_and_finish, daemon=True).start()

        return st

    @property
    def position(self):
        return self.readback.get()


class SynAxisEmptyHints(SynAxis):
    @property
    def hints(self):
        return {}


class SynAxisNoHints(SynAxis):
    readback = Cpt(_ReadbackSignal, value=0, kind="omitted")

    @property
    def hints(self):
        raise AttributeError


class open_QCMD(Device):
    """
    QCM-D Device implementation.

    Parameters
    ----------
    name : string

    """

    # From the documentation for Signal:
    # A Signal is much like a Device – they share almost the same interface – but a Signal has no subcomponents.
    # In ophyd’s hierarchical, tree-like representation of a complex piece of hardware, the signals are the leaves.
    # Each one represents a single PV or a read–write pairs of PVs.
    # https://blueskyproject.io/ophyd/user_v2/generated/ophyd.v2.core.Signal.html

    # From the documentation for Component:
    # A descriptor representing a device component (or signal).
    # val will contain the collected QCM-d data in some shape or form to be determined
    val = Cpt(QCMDSignal, kind="hinted", labels="primary")
    freqs = Cpt(Signal, value=[1, 2, 3], kind="config")

    def __init__(self, name, **kwargs):
        # [] can contain a number of PVs that are set later, see dummy_device.py
        set_later = {}
        for k in []:
            v = kwargs.pop(k, None)
            if v is not None:
                set_later[k] = v

        super().__init__(name=name, **kwargs)

        self.val.name = self.name
        # self.val.sim_set_func(self._compute)

        print(set_later)
        for k, v in set_later.items():
            setattr(self, k, v)

        self.trigger()

    # Devide functionality implementation
    # see: https://nsls-ii.github.io/bluesky/hardware.html

    def subscribe(self, *args, **kwargs):
        return self.val.subscribe(*args, **kwargs)

    def clear_sub(self, cb, event_type=None):
        return self.val.clear_sub(cb, event_type=event_type)

    def unsubscribe(self, cid):
        return self.val.unsubscribe(cid)

    def unsubscribe_all(self):
        return self.val.unsubscribe_all()

    # trigger() returns a status object that is marked done when the device is done triggering
    def trigger(self, *args, **kwargs):
        return self.val.trigger(*args, **kwargs)

    @property
    def exposure_time(self):
        return self.val.exposure_time

    @exposure_time.setter
    def exposure_time(self, v):
        self.val.exposure_time = v

    @property
    def address(self):
        return self.val.address

    @address.setter
    def address(self, v):
        self.val.address = v


# motor1 = SynAxis(name="motor1", labels={"motors"})
# open_QCMD1 = open_QCMD("det1", labels={"detectors"})
