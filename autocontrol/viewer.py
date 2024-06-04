from autocontrol import task_struct

import argparse
import datetime
import math

import graphviz
import json
import os
import requests
import time
import uuid
import pandas as pd
import streamlit as st
import sys

st.set_page_config(layout="wide")

if 'file_mod_date' not in st.session_state:
    st.session_state['file_mod_date'] = None

if 'pause_button' not in st.session_state:
    st.session_state.pause_button = False


def click_pause_button():
    # communicate with atc server and change state accordingly
    if not st.session_state.pause_button:
        url = st.session_state.atc_address + '/pause'
        headers = {'Content-Type': 'application/json'}
        response = requests.post(url, headers=headers)
    else:
        url = st.session_state.atc_address + '/resume'
        headers = {'Content-Type': 'application/json'}
        response = requests.post(url, headers=headers)

    if response.status_code == 200:
        st.session_state.pause_button = not st.session_state.pause_button


@st.cache_data
def analyze_df_for_device_pairs(df):
    filtered_df = df[df['task_type'] == 'transfer']
    pairs_df = filtered_df[['device', 'target_device']].dropna()
    unique_pairs = pairs_df.drop_duplicates()
    result = list(unique_pairs.to_records(index=False))
    return result


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


@st.cache_data
def load_all(modflag, storage_path):
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
        df = conn.query('select * from task_table', ttl=5)
    except Exception:
        df = pd.DataFrame()
    return df


def load_json_task_list(filename, storage_path):
    with open(os.path.join(storage_path, filename+'.json'), "r") as f:
        data = json.load(f)
    ret = {}
    for key in data:
        for channel, entry in enumerate(data[key]):
            # we do not convert back to a full Task object, just to a dictionary sufficient for visualization
            data[key][channel] = json.loads(data[key][channel])
    return data


def replace_priority_with_int(df):
    df = df.sort_values(by='priority', ascending=False).reset_index(drop=True)
    df['priority'] = range(1, len(df) + 1)
    return df


def retrieve_md_key(row, key_str='submission_respone'):
    status = ''
    task = row['task']
    if task is not None:
        # there is ever only one item in this tuple
        task = task_struct.Task.parse_raw(task)
    if task.md is not None:
        if key_str in task.md:
            status = task.md[key_str]
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
    st.session_state.atc_address = atc_address
    identifier_list = []
    if storage_path is None:
        cfd = os.path.dirname(os.path.abspath(__file__))
        storage_path = os.path.join(cfd, '..', 'test')

    file_mod_date = file_mod_time(storage_path)
    if st.session_state['file_mod_date'] is None or st.session_state['file_mod_date'] != file_mod_date:
        priority_queue, active_queue, history_queue, channel_po, edges = load_all(file_mod_time(storage_path),
                                                                                  storage_path=storage_path)
        channel_po_data = []
        for key in channel_po:
            for channel, entry in enumerate(channel_po[key]):
                if entry is not None:
                    channel_po_data.append(entry)
                    # st.info(entry)
                    entry['channel'] = channel
                    entry['device'] = key
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
            priority_queue['status'] = priority_queue.apply(lambda row: retrieve_md_key(row,key_str='submission_response'), axis=1)
        if not active_queue.empty:
            active_queue['status'] = active_queue.apply(lambda row: retrieve_md_key(row, key_str='submission_response'), axis=1)

        # replace priority values by integers
        priority_queue = replace_priority_with_int(priority_queue)
        active_queue = replace_priority_with_int(active_queue)
        history_queue = replace_priority_with_int(history_queue)

    st.title('Autocontrol Viewer')

    if st.session_state.pause_button:
        st.button(':orange-background[Resume Queue]', on_click=click_pause_button)
    else:
        st.button(':green-background[Pause Queue]', on_click=click_pause_button)

    # create flow chart via graphviz
    with st.expander('Task Diagram', expanded=True):
        st.image(os.path.join(storage_path, 'priority_queue.png'))
        st.image(os.path.join(storage_path, 'active_queue.png'))
        st.image(os.path.join(storage_path, 'history_queue.png'))

    with st.expander('Sample Occupancy Diagram'):
        st.image(os.path.join(storage_path, 'cpo_data.png'))

    # visualize dataframes in tables
    co_list = (
        # "id",
        "priority",
        "sample_number",
        "task_type",
        "device",
        "channel",
        "status",
        # "target_device",
        # "target_channel",
        "task",
        # "md"
    )
    co_conf = {"sample_number": "sample",
                                "task_type": "task type",
                                # "md": "meta data",
                                # "target_device": "target device",
                                # "target_channel": "target channel"
               }


    st.text('Queued Jobs:')
    st.dataframe(priority_queue, column_order=co_list, column_config=co_conf, use_container_width=True,
                 hide_index=True)
    st.text('Active Jobs:')
    st.dataframe(active_queue, column_order=co_list, column_config=co_conf, use_container_width=True,
                 hide_index=True)
    st.text('Finished Jobs:')
    st.dataframe(history_queue, column_order=co_list, column_config=co_conf, use_container_width=True,
                 hide_index=True)

    # if st.button('Reload', type="primary"):
    time.sleep(10)
    # st.cache_data.clear()
    st.rerun()


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
