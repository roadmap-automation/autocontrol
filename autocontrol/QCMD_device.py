import json
import numpy as np
from ophyd.device import Component as Cpt
from ophyd.device import Device
from ophyd.signal import Signal
from ophyd.status import DeviceStatus
import requests
import threading
import time as ttime
import warnings


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

    def stop(self):
        """
        Stop function in case of Device failure

        :return: no return value
        """
        self.qcmd_stop()

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
                # start QCMD and check for success
                start_status = ''
                self.qcmd_start()
                for _ in range(10):
                    start_status = self.qcmd_status()
                    if start_status == 'measuring':
                        break
                    else:
                        self.log.debug("Waiting for QCM-D to start measurement, %s", self)
                        print("Waiting for QCM-D to start measurement ...")
                    ttime.sleep(5)
                if start_status == 'measuring':
                    self.log.debug("sleep_and_finish %s", self)
                    print("Started QCM-D measurement.")
                    ttime.sleep(delay_time)
                    self.put(self._func())
                    self.qcmd_stop()
                else:
                    self.log.debug("Failed to start QCM-D, %s", self)
                    print("Failed to start QCM-D.")
                st.set_finished()

            threading.Thread(target=sleep_and_finish, daemon=True).start()
        else:
            self.put(self._func())
            st.set_finished()
        return st

    def qcmd_communicate(self, command, value=0):
        """
        Communicate with QCM-D instrument and return response.

        :param command: HTTP POST request command field
        :param value: HTTP POST request value field
        :return: response from HTTP POST or None if failed
        """
        if self.address is None:
            return None

        cmdstr = '{"command": "' + str(command) + '", "value": ' + str(value) + '}'
        try:
            r = requests.post(self.address, cmdstr)
        except requests.exceptions.RequestException:
            return None

        if r.status_code != 200:
            return None

        rdict = json.loads(r.text)
        response = rdict['result']
        return response

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

        rrdict = self.qcmd_communicate("get_data")
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
        self.qcmd_communicate("start")

    def qcmd_status(self):
        """
        Retrieves the QCM-D status.

        :return: no return value
        """
        rstring = self.qcmd_communicate("get_status")
        if rstring is None:
            return 'no connection'

        return rstring

    def qcmd_stop(self):
        """
        Stops the QCM-D data acquisition.

        :return: no return value
        """
        self.qcmd_communicate("stop")


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

        # Not sure why one should trigger when initializing the object
        # self.trigger()

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


if __name__ == '__main__':
    open_QCMD1 = open_QCMD("det1", labels={"detectors"})
    open_QCMD1.exposure_time = 1
    open_QCMD1.address = "http://localhost:5011/QCMD/"
