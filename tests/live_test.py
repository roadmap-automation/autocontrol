import autocontrol.task_struct as tsk
import autocontrol.support
import os
import time as ttime
import uuid

port = 5014


def live_test():
    print('Starting live test')

    print('Preparing test directory')
    cfd = os.path.dirname(os.path.abspath(__file__))
    storage_path = os.path.join(cfd, '..', 'test_storage')

    # ----------- Starting Flask Server and Streamlit Viewer ---------------------------
    autocontrol.support.start(portnumber=port, storage_path=storage_path)

    # ----------- Submitting tasks ---------------------------
    task = tsk.Task(
        task_type=tsk.TaskType('init'),
        tasks=[tsk.TaskData(
            device='injection1',
            device_type='injection',
            device_address='http://localhost:5003',
            number_of_channels=2,
            simulated=False,
            md={'description': 'injection device init'}
        )]
    )
    autocontrol.support.submit_task(task, port)

    sample_id1 = uuid.uuid4()
    task_id1 = uuid.uuid4()
    task = tsk.Task(
        sample_id=sample_id1,
        id=task_id1,
        task_type=tsk.TaskType('prepare'),
        tasks=[tsk.TaskData(
            device='injection1',
            method_data={'method_list': [{'method_name': 'RoadmapChannelSleep', 'method_data': {'sleep_time': 40}}]},
            md={'description': 'dummy prepare sleep'}
        )]
    )
    autocontrol.support.submit_task(task, port)

    sample_id2 = uuid.uuid4()
    task = tsk.Task(
        sample_id=sample_id2,
        task_type=tsk.TaskType('prepare'),
        tasks=[tsk.TaskData(
            device='injection1',
            method_data={'method_list': [{'method_name': 'RoadmapChannelSleep', 'method_data': {'sleep_time': 5}}]},
            md={'description': 'dummy prepare sleep'}
        )]
    )
    autocontrol.support.submit_task(task, port)

    task = tsk.Task(
        sample_id=sample_id1,
        task_type=tsk.TaskType('measure'),
        tasks=[tsk.TaskData(
            device='injection1',
            method_data={'method_list': [{'method_name': 'RoadmapChannelSleep', 'method_data': {'sleep_time': 10}}]},
            md={'description': 'dummy sleep'}
        )]
    )
    autocontrol.support.submit_task(task, port)

    task = tsk.Task(
        sample_id=sample_id2,
        task_type=tsk.TaskType('measure'),
        tasks=[tsk.TaskData(
            device='injection1',
            method_data={'method_list': [{'method_name': 'RoadmapChannelSleep', 'method_data': {'sleep_time': 10}}]},
            md={'description': 'dummy sleep'}
        )]
    )
    autocontrol.support.submit_task(task, port)

    # Wait for user input
    _ = input("Please enter some text and press Enter to stop all processes: ")

    # ------------------ Stopping Flask Server ----------------------------------
    autocontrol.support.stop(portnumber=port)
    print('Integration test done.')
    print('Program exit.')


if __name__ == '__main__':
    live_test()
    autocontrol.support.terminate_processes()
