import numpy as np
from ophyd.device import Component as Cpt
from ophyd.device import Device
from ophyd.signal import Signal
from ophyd.status import DeviceStatus, MoveStatus
from ophyd.utils import ReadOnlyError
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

class SynSignal(Signal):
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
    def __init__(
        self,
        func=None,
        *,
        name,  # required, keyword-only
        exposure_time=0,
        precision=3,
        parent=None,
        labels=None,
        kind=None,
        **kwargs,
    ):
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
        self._func = func
        self.exposure_time = exposure_time
        self.precision = precision
        super().__init__(
            value=self._func(),
            timestamp=ttime.time(),
            name=name,
            parent=parent,
            labels=labels,
            kind=kind,
            **kwargs,
        )
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
        delay_time = self.exposure_time
        if delay_time:

            def sleep_and_finish():
                self.log.debug("sleep_and_finish %s", self)
                ttime.sleep(delay_time)
                self.put(self._func())
                st.set_finished()

            threading.Thread(target=sleep_and_finish, daemon=True).start()
        else:
            self.put(self._func())
            st.set_finished()
        return st

    def sim_set_func(self, func):
        """
        Update the SynSignal function to set a new value on trigger.
        """
        self._func = func


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


class SynGauss(Device):
    """
    Evaluate a point on a Gaussian based on the value of a motor.

    Parameters
    ----------
    name : string
    motor : Device
    motor_field : string
    center : number
        center of peak
    Imax : number
        max intensity of peak
    sigma : number, optional
        Default is 1.
    noise : {'poisson', 'uniform', None}, optional
        Add noise to the gaussian peak.
    noise_multiplier : float, optional
        Only relevant for 'uniform' noise. Multiply the random amount of
        noise by 'noise_multiplier'
    random_state : numpy random state object, optional
        np.random.RandomState(0), to generate random number with given seed

    Example
    -------
    motor = SynAxis(name='motor')
    det = SynGauss('det', motor, 'motor', center=0, Imax=1, sigma=1)
    """

    def _compute(self):
        m = self._motor.read()[self._motor_field]["value"]
        # we need to do this one at a time because
        #   - self.read() may be screwed with by the user
        #   - self.get() would cause infinite recursion
        Imax = self.Imax.get()
        center = self.center.get()
        sigma = self.sigma.get()
        noise = self.noise.get()
        noise_multiplier = self.noise_multiplier.get()
        v = Imax * np.exp(-((m - center) ** 2) / (2 * sigma**2))
        if noise == "poisson":
            v = int(self.random_state.poisson(np.round(v), 1))
        elif noise == "uniform":
            v += self.random_state.uniform(-1, 1) * noise_multiplier
        return v

    val = Cpt(SynSignal, kind="hinted")
    Imax = Cpt(Signal, value=10, kind="config")
    center = Cpt(Signal, value=0, kind="config")
    sigma = Cpt(Signal, value=1, kind="config")
    noise = Cpt(
        EnumSignal,
        value="none",
        kind="config",
        enum_strings=("none", "poisson", "uniform"),
    )
    noise_multiplier = Cpt(Signal, value=1, kind="config")

    def __init__(
        self, name, motor, motor_field, center, Imax, *, random_state=None, **kwargs
    ):
        set_later = {}
        for k in ("sigma", "noise", "noise_multiplier"):
            v = kwargs.pop(k, None)
            if v is not None:
                set_later[k] = v
        super().__init__(name=name, **kwargs)
        self._motor = motor
        self._motor_field = motor_field
        self.center.put(center)
        self.Imax.put(Imax)

        self.random_state = random_state or np.random
        self.val.name = self.name
        self.val.sim_set_func(self._compute)
        for k, v in set_later.items():
            getattr(self, k).put(v)

        self.trigger()

    def subscribe(self, *args, **kwargs):
        return self.val.subscribe(*args, **kwargs)

    def clear_sub(self, cb, event_type=None):
        return self.val.clear_sub(cb, event_type=event_type)

    def unsubscribe(self, cid):
        return self.val.unsubscribe(cid)

    def unsubscribe_all(self):
        return self.val.unsubscribe_all()

    def trigger(self, *args, **kwargs):
        return self.val.trigger(*args, **kwargs)

    @property
    def precision(self):
        return self.val.precision

    @precision.setter
    def precision(self, v):
        self.val.precision = v

    @property
    def exposure_time(self):
        return self.val.exposure_time

    @exposure_time.setter
    def exposure_time(self, v):
        self.val.exposure_time = v


motor1 = SynAxis(name="motor1", labels={"motors"})
det1 = SynGauss("det1", motor1, "motor1", center=0, Imax=5, sigma=0.5, labels={"detectors"})