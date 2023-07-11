import sys
from bluesky.plan_stubs import mv
from bluesky.plan_stubs import read
from bluesky import RunEngine
from bluesky.callbacks.best_effort import BestEffortCallback
from bluesky.utils import ProgressBarManager
from bluesky.plans import count
from bluesky.callbacks import LiveTable

from databroker import Broker

# from ophyd.sim import det1, det2  # two simulated detectors
from QCMD_device import open_QCMD
from liquid_handler_device import lh_device

if __name__ == "__main__":
    RE = RunEngine({})

    bec = BestEffortCallback()

    # Send all metadata/data captured to the BestEffortCallback.
    RE.subscribe(bec)

    # Make plots update live while scans run.
    # Running this in iPython it says that it is no longer necessary

    # install_kicker()

    db = Broker.named('temp')

    # Insert all metadata/data captured into db.
    RE.subscribe(db.insert)

    RE.waiting_hook = ProgressBarManager()

    '''
    dets = [det1, det2]  # a list of any number of detectors
    RE(count(dets))

    from ophyd.sim import det, motor
    from bluesky.plans import scan

    dets = [det]  # just one in this case, but it could be more than one

    RE(scan(dets, motor, -1, 1, 10))
    '''
    test = 'liquid_handler'
    # test = 'both'

    def read_lh(lh):
        # readings = yield from read([lh])
        readings = lh.read()
        return readings

    if test == 'liquid_handler' or test == 'both':
        lh = lh_device(name="lh", labels={"motors"})
        RE(mv(lh, 'command xxx'))
        RE(read(lh))

        print(read_lh(lh))

    if test == 'QCMD' or test == 'both':
        # QCM-D device
        dets = [open_QCMD("QCMD", labels={"detectors"})]

        # sets exposure time to 10 s
        dets[0].exposure_time = 1
        dets[0].address = "http://localhost:5011/QCMD/"

        # takes a count
        RE(count(dets))

        print(db[-1].start)
        print(db[-1].table())

    sys.exit()
