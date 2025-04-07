import autocontrol.support
from autocontrol import task_struct
from autocontrol import support
import argparse
import datetime
import graphviz
import json
import math
import os
import pandas as pd
import requests
import sqlite3
import streamlit as st
from streamlit_autorefresh import st_autorefresh
import time
import uuid

st.set_page_config(layout="wide")

if 'pause_button' not in st.session_state:
    st.session_state.pause_button = False
if 'reset_all' not in st.session_state:
    st.session_state.reset_all = False
if 'restart_all' not in st.session_state:
    st.session_state.restart_all = False
if 'priority_queue' not in st.session_state:
    st.session_state.priority_queue = None
if 'active_queue' not in st.session_state:
    st.session_state.active_queue = None
if 'history_queue' not in st.session_state:
    st.session_state.history_queue = None
if 'file_mod_time' not in st.session_state:
    st.session_state.file_mod_time = None
if 'poll_counter' not in st.session_state:
    st.session_state.poll_counter = None


def click_pause_button():
    # communicate with atc server and change state accordingly
    if not st.session_state.pause_button:
        url = st.session_state.atc_address # + '/pause'
        response = autocontrol.support.pause_queue(url=url)
    else:
        url = st.session_state.atc_address # + '/resume'
        response = autocontrol.support.resume_queue(url=url)

    if response.status_code == 200:
        st.session_state.pause_button = not st.session_state.pause_button


def click_reset_button():
    if not st.session_state.reset_all:
        st.session_state.reset_all = True
    else:
        url = st.session_state.atc_address + '/reset'
        headers = {'Content-Type': 'application/json'}
        response = requests.post(url, headers=headers)
        if response.status_code == 200:
            st.session_state.reset_all = False


def click_restart_button():
    if not st.session_state.restart_all:
        st.session_state.restart_all = True
    else:
        url = st.session_state.atc_address + '/restart'
        headers = {'Content-Type': 'application/json'}
        response = requests.post(url, headers=headers)
        if response.status_code == 200:
            st.session_state.restart_all = False


@st.cache_data
def analyze_df_for_device_pairs(df):
    filtered_df = df[df['task_type'] == 'transfer']
    pairs_df = filtered_df[['device', 'target_device']].dropna()
    unique_pairs = pairs_df.drop_duplicates()
    result = list(unique_pairs.to_records(index=False))
    return result


# @st.experimental_fragment
def ui_fragment():
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        if st.session_state.pause_button:
            st.button(':orange-background[Resume Queue]', on_click=click_pause_button)
        else:
            st.button(':green-background[Pause Queue]', on_click=click_pause_button)

    with col3:
        if st.session_state.reset_all:
            st.button(':red-background[Are you sure? Clear all tasks?]', on_click=click_reset_button)
        else:
            st.button('Clear All Tasks, keep device inits.', on_click=click_reset_button)

    with col4:
        if st.session_state.restart_all:
            st.button(':red-background[Are you sure? Restart?]', on_click=click_restart_button)
        else:
            st.button('Restart Autocontrol. Reset device inits.', on_click=click_restart_button)


def get_new_data(storage_path, identifier_list):
    priority_queue, active_queue, history_queue, channel_po, edges = load_all(storage_path=storage_path)
    channel_po_data = []
    for key in channel_po:
        for i, entry in enumerate(channel_po[key]):
            if entry is not None:
                # device information is usually part of the subtask and therefore must be supplied here
                entry['device'] = key
                channel_po_data.append(entry)
                # st.info(entry)

    if channel_po_data:
        channel_po_data = pd.DataFrame(channel_po_data)
    else:
        channel_po_data = pd.DataFrame(columns=priority_queue.columns)

    render_all_queues(priority_queue, active_queue, history_queue, channel_po_data, edges,
                      file_mod_time(storage_path), identifier_list=identifier_list, channel_po=channel_po,
                      storage_path=storage_path)

    # add a status column to each data frame for visualization
    priority_queue['status'] = ''
    active_queue['status'] = ''
    history_queue['status'] = ''

    if not priority_queue.empty:
        priority_queue['status'] = priority_queue.apply(lambda row: retrieve_md_key(row, key_strs=('submission_response', 'submission_device_response')), axis=1)
    if not active_queue.empty:
        active_queue['status'] = active_queue.apply(lambda row: retrieve_md_key(row, key_strs=('execution_response',)), axis=1)
    if not history_queue.empty:
        history_queue['status'] = history_queue.apply(lambda row: retrieve_md_key(row, key_strs=('execution_response',)), axis=1)

    # replace priority values by integers
    priority_queue = replace_priority_with_int(priority_queue)
    active_queue = replace_priority_with_int(active_queue)
    history_queue = replace_priority_with_int(history_queue)

    st.session_state.priority_queue = priority_queue
    st.session_state.active_queue = active_queue
    st.session_state.history_queue = history_queue


def file_mod_time(storage_path):
    def tconv(filename):
        time1 = os.path.getmtime(os.path.join(storage_path, filename))
        mod_time_datetime = datetime.datetime.fromtimestamp(time1)
        human_readable_mod_time = mod_time_datetime.strftime("%Y-%m-%d %H:%M:%S")
        return human_readable_mod_time

    time1 = tconv('priority_queue.sqlite3')
    time2 = tconv('active_queue.sqlite3')
    time3 = tconv('history_queue.sqlite3')
    return time1, time2, time3


def load_all(storage_path):
    priority_queue = load_sql('priority_queue', storage_path)
    active_queue = load_sql('active_queue', storage_path)
    history_queue = load_sql('history_queue', storage_path)
    channel_po = load_json_task_list('channel_po', storage_path)

    td_frames = []
    if 'target_device' in priority_queue.columns:
        td_frames.append(priority_queue[['task_type', 'device', 'target_device']])
    if 'target_device' in active_queue.columns:
        td_frames.append(active_queue[['task_type', 'device', 'target_device']])
    if 'target_device' in history_queue.columns:
        td_frames.append(history_queue[['task_type', 'device', 'target_device']])
    if td_frames:
        conc_df = pd.concat(td_frames)
        edges = analyze_df_for_device_pairs(conc_df)
    else:
        edges = None

    return priority_queue, active_queue, history_queue, channel_po, edges


def load_sql(filename, storage_path):
    # load status from SQLlite databases
    localhost = "sqlite:///"
    absolute_path = os.path.abspath(os.path.join(storage_path, filename+'.sqlite3'))
    url = localhost + absolute_path
    conn = st.connection(filename, type='sql', url=url)
    try:
        df = conn.query("SELECT * FROM task_table", ttl=5)
    except (sqlite3.OperationalError, sqlite3.DatabaseError) as e:
        st.info(f"Database error: {e}")
        df = pd.DataFrame()
    return df


def load_json_task_list(filename, storage_path):
    with open(os.path.join(storage_path, filename+'.json'), "r") as f:
        data = json.load(f)
    for key in data:
        for i, entry in enumerate(data[key]):
            # we do not convert back to a full Task object, just to a dictionary sufficient for visualization
            data[key][i] = json.loads(data[key][i])
            # take channel information from last task in 'tasks' subfield and elevate it
            data[key][i]['channel'] = data[key][i]['tasks'][-1]['channel']
    return data


def replace_priority_with_int(df):
    df = df.sort_values(by='priority', ascending=False).reset_index(drop=True)
    df['priority'] = range(1, len(df) + 1)
    return df


def retrieve_md_key(row, key_strs=('submission_response',)):
    status = ''
    taskmd = subtaskmd = False
    task = row['task']
    if task is not None:
        # there is ever only one item in this tuple
        task = task_struct.Task.parse_raw(task)
    if task.md is not None:
        for key_str in key_strs:
            if key_str in task.md:
                if not taskmd:
                    status += 'Task status:\n'
                    taskmd = True
                status += key_str + ': ' + task.md[key_str] + '\n'
    for i, subtask in enumerate(task.tasks):
        for key_str in key_strs:
            if key_str in subtask.md:
                if not subtaskmd:
                    status += 'Subtask {} status:\n'.format(i)
                    subtaskmd = True
                status += key_str + ': ' + subtask.md[key_str] + '\n'
    return status


def render_cluster(data, graph, identifier_list, name='0', color='grey', show_device=False):
    def create_uuid(id_first_node):
        # find unique node id
        while True:
            idstr = str(uuid.uuid4())
            if idstr not in identifier_list:
                identifier_list.append(idstr)
                break
        if id_first_node is None:
            id_first_node = idstr
        id_last_node = idstr
        return idstr, id_first_node, id_last_node

    id_first_node = None
    id_last_node = None
    with graph.subgraph(name='cluster_'+name) as c:
        c.attr(fillcolor=color, label=name, style='filled')
        c.attr('node', shape='box', style='filled', fillcolor='grey')

        if data is not None and not data.empty:
            for index, row in data[::-1].iterrows():
                # st.dataframe(row)
                idstr, id_first_node, id_last_node = create_uuid(id_first_node)
                if row['channel'] is not None and not math.isnan(row['channel']):
                    label = 'S' + str(row['sample_number']) + ' C' + str(int(row['channel'])) + '\n' + row['task_type']
                else:
                    label = 'S' + str(row['sample_number']) + '\n' + row['task_type']
                if show_device:
                    label += '\n' + row['device']
                c.node(idstr, label=label)
        else:
            idstr, id_first_node, id_last_node = create_uuid(id_first_node)
            c.node(idstr, label=' ', style='invisible')

    return id_first_node, id_last_node


def render_data(data, color, filename, identifier_list, channel_po, split_by_device=False, edges=None, storage_path=''):
    g = graphviz.Digraph('gvg')
    if split_by_device:
        edge_nodes = {}
        grouped = data.groupby('device')
        # render initialized devices that do not have active tasks
        for key in list(channel_po.keys()):
            if key not in grouped.groups:
                first, last = render_cluster(None, g, identifier_list, name=key, color='lightgreen')
                edge_nodes[key] = [first, last]
        # render each active device separately
        for device in grouped.groups:
            device_df = grouped.get_group(device)
            # st.dataframe(device_df)
            first, last = render_cluster(device_df, g, identifier_list, name=device, color='lightgreen')
            edge_nodes[device] = [first, last]
        for entry in edges:
            # draw edge from last to first node in the two clusters that should be connected given in the
            # edges dictionary
            if entry[0] in edge_nodes and entry[1] in edge_nodes:
                g.edge(edge_nodes[entry[0]][1], edge_nodes[entry[1]][0])
    else:
        render_cluster(data, g, identifier_list=identifier_list, name=filename, color=color, show_device=True)
    g.render(filename=os.path.join(storage_path, filename), format='png')


@st.cache_data
def render_all_queues(pdata, adata, hdata, cpodata, edges, filemodflag, identifier_list, channel_po,
                      storage_path=''):
    render_data(pdata, color='lightblue', filename='priority_queue', identifier_list=identifier_list,
                channel_po=channel_po, storage_path=storage_path)
    render_data(adata, color='lightgreen', filename='active_queue', identifier_list=identifier_list,
                channel_po=channel_po, split_by_device=True, edges=edges, storage_path=storage_path)
    render_data(hdata, color='orange', filename='history_queue', identifier_list=identifier_list,
                channel_po=channel_po, storage_path=storage_path)
    render_data(cpodata, color='lightgreen', filename='cpo_data', identifier_list=identifier_list,
                channel_po=channel_po, split_by_device=True, edges=edges, storage_path=storage_path)

# ---------------------------------------------------------------------------------------------------------------------
# --------------------------------------------- Streamlit Page Start --------------------------------------------------


def main(storage_path=None, atc_address=None):
    count = st_autorefresh(interval=5000, limit=None, key="pcounter")

    st.session_state.atc_address = atc_address
    identifier_list = []
    if storage_path is None:
        cfd = os.path.dirname(os.path.abspath(__file__))
        storage_path = os.path.join(cfd, '..', 'test')

    fmt = file_mod_time(storage_path)
    if st.session_state.file_mod_time is None or st.session_state.file_mod_time != fmt:
        st.session_state.file_mod_time = fmt
        get_new_data(storage_path=storage_path, identifier_list=identifier_list)

    priority_queue = st.session_state.priority_queue
    active_queue = st.session_state.active_queue
    history_queue = st.session_state.history_queue

    st.title('Autocontrol Viewer')

    ui_fragment()

    # create flow chart via graphviz
    # with st.expander('Task Diagram', expanded=True):
    st.write('Tasks')
    st.image(os.path.join(storage_path, 'priority_queue.png'))
    st.image(os.path.join(storage_path, 'active_queue.png'))
    st.image(os.path.join(storage_path, 'history_queue.png'))

    # with st.expander('Sample Occupancy Diagram'):
    st.write('Sample Occupancy')
    st.image(os.path.join(storage_path, 'cpo_data.png'))

    # visualize dataframes in tables
    co_list = ("priority", "sample_number", "task_type", "device", "channel", "status", "task")
    co_conf = {
        "priority": st.column_config.NumberColumn("priority", width='small'),
        "sample_number": st.column_config.NumberColumn("sample", width='small'),
        "task_type": st.column_config.TextColumn("task type", width='small'),
        "device": st.column_config.TextColumn("device", width='small'),
        "channel": st.column_config.NumberColumn("channel", width='small'),
        "task": st.column_config.Column("task", width='large'),
    }
    co_conf_priority = co_conf | {"status": st.column_config.TextColumn("submission status", width='small')}
    co_conf_activity = co_conf | {"status": st.column_config.TextColumn("execution status", width='small')}
    co_conf_history = co_conf | {"status": None}

    st.text('Queued Jobs:')
    st.dataframe(priority_queue, column_order=co_list, column_config=co_conf_priority, use_container_width=True,
                 hide_index=True)
    st.text('Active Jobs:')
    st.dataframe(active_queue, column_order=co_list, column_config=co_conf_activity, use_container_width=True,
                 hide_index=True)
    st.text('Finished Jobs:')
    st.dataframe(history_queue, column_order=co_list, column_config=co_conf_history, use_container_width=True,
                 hide_index=True)

    if st.session_state.poll_counter is None or st.session_state.poll_counter != count:
        st.session_state.poll_counter = count
        if st.session_state.restart_all or st.session_state.reset_all:
            st.session_state.restart_all = False
            st.session_state.reset_all = False


if __name__ == '__main__':
    # sys.argv = sys.argv[:1] + sys.argv[2:]  # Streamlit adds extra args; this line removes them
    parser = argparse.ArgumentParser()
    parser.add_argument('--storage_dir', type=str, default=os.getcwd(), help='Path to storage directory')
    parser.add_argument('--atc_address', type=str, default='http://localhost:5000',
                        help='Address of atc server')
    args = parser.parse_args()
    storage_dir = args.storage_dir
    atc_address = args.atc_address

    main(storage_path=storage_dir, atc_address=atc_address)
