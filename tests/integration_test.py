import autocontrol.task_struct as tsk
import autocontrol.support
import os
import time
import uuid

port = 5004


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
            number_of_channels=1,
            simulated=True,
            md={'description': 'QCMD init'}
        )]
    )
    autocontrol.support.submit_task(task, port)
    time.sleep(5)

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
    time.sleep(5)

    sample_id1 = uuid.uuid4()
    task = tsk.Task(
        sample_id=sample_id1,
        task_type=tsk.TaskType('prepare'),
        tasks=[tsk.TaskData(
            device='lh1',
            md={'description': 'Sample1 preparation'}
        )]
    )
    autocontrol.support.submit_task(task, port)
    time.sleep(5)

    sample_id2 = uuid.uuid4()
    task = tsk.Task(
        sample_id=sample_id2,
        task_type=tsk.TaskType('prepare'),
        tasks=[tsk.TaskData(
            device='lh1',
            md={'description': 'Sample2 preparation'}
        )]
    )
    autocontrol.support.submit_task(task, port)
    time.sleep(5)

    task = tsk.Task(
        sample_id=sample_id1,
        task_type=tsk.TaskType('transfer'),
        tasks=[
            tsk.TaskData(
                device='lh1',
                md={'description': 'Sample1 transfer'}
            ),
            tsk.TaskData(
                device='qcmd1',
                md={'description': 'Sample1 transfer'}
            )
        ]
    )
    autocontrol.support.submit_task(task, port)
    time.sleep(5)

    task = tsk.Task(
        sample_id=sample_id2,
        task_type=tsk.TaskType('transfer'),
        tasks=[
            tsk.TaskData(
                device='lh1',
                md={'description': 'Sample2 transfer'}
            ),
            tsk.TaskData(
                device='qcmd1',
                md={'description': 'Sample2 transfer'}
            )
        ]
    )
    autocontrol.support.submit_task(task, port)
    time.sleep(5)

    task = tsk.Task(
        sample_id=sample_id1,
        task_type=tsk.TaskType('measure'),
        tasks=[tsk.TaskData(
            device='qcmd1',
            md={'description': 'QCMD measurement sample1'}
        )]
    )
    autocontrol.support.submit_task(task, port)
    time.sleep(5)

    task = tsk.Task(
        sample_id=sample_id2,
        task_type=tsk.TaskType('measure'),
        tasks=[tsk.TaskData(
            device='qcmd1',
            md={'description': 'QCMD measurement sample2'}
        )]
    )
    autocontrol.support.submit_task(task, port)
    time.sleep(5)

    task = tsk.Task(
        sample_id=sample_id1,
        task_type=tsk.TaskType('nochannel'),
        tasks=[tsk.TaskData(
            device='lh1',
            md={'description': 'lh rinse'}
        )]
    )
    autocontrol.support.submit_task(task, port)
    time.sleep(5)

    # ------------------ Stopping Flask Server ----------------------------------
    autocontrol.support.stop(portnumber=port)
    time.sleep(5)

    print('Integration test done.')
    print('Program exit.')


if __name__ == '__main__':
    integration_test()
    # Wait for user input
    _ = input("Please enter some text and press Enter to stop all processes: ")
    autocontrol.support.terminate_processes()
