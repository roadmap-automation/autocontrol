import autocontrol.task_struct as tsk
import autocontrol.support
import os
import time
import uuid

port = 5004


def submit_sample_block(qcmd_channel=None):
    sample_id = uuid.uuid4()
    task = tsk.Task(
        sample_id=sample_id,
        task_type=tsk.TaskType('prepare'),
        tasks=[tsk.TaskData(
            device='lh1',
            md={'description': '{} preparation'.format(str(sample_id))},
        )]
    )
    autocontrol.support.submit_task(task, port)

    task = tsk.Task(
        sample_id=sample_id,
        task_type=tsk.TaskType('transfer'),
        tasks=[
            tsk.TaskData(
                device='lh1',
                md={'description': '{} transfer'.format(str(sample_id))},
            ),
            tsk.TaskData(
                device='qcmd1',
                channel=qcmd_channel,
                md={'description': '{} transfer'.format(str(sample_id))},
            )
        ]
    )
    autocontrol.support.submit_task(task, port)

    task = tsk.Task(
        sample_id=sample_id,
        task_type=tsk.TaskType('measure'),
        tasks=[tsk.TaskData(
            device='qcmd1',
            channel=qcmd_channel,
            md={'description': 'QCMD measurement {}'.format(str(sample_id))},
        )]
    )
    autocontrol.support.submit_task(task, port)


def integration_test():
    print('Starting integration test')

    print('Preparing test directory')
    cfd = os.path.dirname(os.path.abspath(__file__))
    storage_path = os.path.join(cfd, '..', 'test_storage')

    # ----------- Starting Flask Server and Streamlit Viewer ---------------------------
    autocontrol.support.start(portnumber=port, storage_path=storage_path)

    # ------------------ Submitting Task ----------------------------------

    task = tsk.Task(
        task_type=tsk.TaskType('init'),
        tasks=[tsk.TaskData(
            device='qcmd1',
            device_type='qcmd',
            device_address='https:hereitcomes',
            number_of_channels=2,
            sample_mixing=False,
            simulated=True,
            md={'description': 'QCMD init'}
        )]
    )
    autocontrol.support.submit_task(task, port)
    time.sleep(0.1)

    task = tsk.Task(
        task_type=tsk.TaskType('init'),
        tasks=[tsk.TaskData(
            device='lh1',
            device_type='lh',
            device_address='https:hereitcomes',
            number_of_channels=10,
            simulated=True,
            md={'description': 'lh init'}
        )]
    )
    autocontrol.support.submit_task(task, port)
    time.sleep(0.1)

    submit_sample_block(qcmd_channel=0)
    submit_sample_block(qcmd_channel=0)
    submit_sample_block(qcmd_channel=1)

    # Wait for user input
    _ = input("Please enter to stop the autocontrol server.")

    # ------------------ Stopping Flask Server ----------------------------------
    autocontrol.support.stop(portnumber=port)
    time.sleep(5)

    _ = input("Please enter to stop Streamlit and all processes.")

    print('Integration test done.')
    print('Program exit.')


if __name__ == '__main__':
    integration_test()
    autocontrol.support.terminate_processes()
