import autocontrol.task_struct as tsk
import autocontrol.start
import os
import uuid

port = 5004


def live_test():
    print('Starting live test')

    print('Preparing test directory')
    cfd = os.path.dirname(os.path.abspath(__file__))
    storage_path = os.path.join(cfd, '..', 'test_storage')

    # ----------- Starting Flask Server and Streamlit Viewer ---------------------------
    autocontrol.start.start(portnumber=port, storage_path=storage_path)

    # ----------- Submitting tasks ---------------------------
    task = tsk.Task(
        task_type=tsk.TaskType('init'),
        tasks=[tsk.TaskData(
            device='injection1',
            device_type='injection',
            device_address='http://localhost:',
            number_of_channels=2,
            simulated=False,
            md={'description': 'injection device init'}
        )]
    )
    autocontrol.start.submit_task(task, port)

    sample_id1 = uuid.uuid4()
    task = tsk.Task(
        sample_id=sample_id1,
        task_type=tsk.TaskType('measure'),
        tasks=[tsk.TaskData(
            device='injection1',
            method_data={'method_name': 'RoadmapChannelSleep', 'method_dat': {'sleep_time': 10}},
            md={'description': 'dummy sleep'}
        )]
    )
    autocontrol.start.submit_task(task, port)


if __name__ == '__main__':
    live_test()
    # Wait for user input
    _ = input("Please enter some text and press Enter to stop all processes: ")
    autocontrol.start.terminate_processes()
