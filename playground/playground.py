import sys
from bluesky import RunEngine
from bluesky.callbacks.best_effort import BestEffortCallback
from bluesky.utils import install_kicker, ProgressBarManager
from bluesky.plans import count

from databroker import Broker

# from ophyd.sim import det1, det2  # two simulated detectors
from QCMD_device import open_QCMD

from bluesky_widgets.examples.utils.generate_msgpack_data import get_catalog
from bluesky_widgets.examples.qt_run_tree_view import RunTree


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
