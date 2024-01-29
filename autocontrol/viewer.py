import datetime
import graphviz
import json
import os
import time
import uuid

import streamlit as st
import pandas as pd

st.set_page_config(layout="wide")

if 'file_mod_date' not in st.session_state:
    st.session_state['file_mod_date'] = None

@st.cache_data
def analyze_df_for_device_pairs(df):
    filtered_df = df[df['task_type'] == 'transfer']
    pairs_df = filtered_df[['device', 'target_device']].dropna()
    unique_pairs = pairs_df.drop_duplicates()
    result = list(unique_pairs.to_records(index=False))
    return result


def file_mod_time():
    def tconv(filename):
        time1 = os.path.getmtime(os.path.join(storage_path) + filename)
        mod_time_datetime = datetime.datetime.fromtimestamp(time1)
        human_readable_mod_time = mod_time_datetime.strftime("%Y-%m-%d %H:%M:%S")
        return human_readable_mod_time

    time1 = tconv('priority_queue.sqlite3')
    time2 = tconv('active_queue.sqlite3')
    time3 = tconv('history_queue.sqlite3')
    return time1, time2, time3


@st.cache_data
def load_all(modflag):
    priority_queue = load_sql('priority_queue')
    active_queue = load_sql('active_queue')
    history_queue = load_sql('history_queue')
    channel_po = load_json('channel_po')

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


def load_sql(filename):
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


def load_json(filename):
    with open(os.path.join(storage_path, filename+'.json'), "r") as f:
        data = json.load(f)
    return data


def render_cluster(data, graph, name='0', color='grey'):
    id_first_node = None
    id_last_node = None
    with graph.subgraph(name='cluster_'+name) as c:
        c.attr(fillcolor=color, label=name, style='filled')
        c.attr('node', shape='box', style='filled', fillcolor='grey')
        identifier = name[0]
        if identifier in identifier_list:
            i = 2
            while True:
                modified_identifier = identifier + str(i)
                if modified_identifier not in identifier_list:
                    identifier = modified_identifier
                    break
                i += 1
        identifier_list.append(identifier)

        if data is not None:
            for index, row in data[::-1].iterrows():
                if id_first_node is None:
                    id_first_node = name+str(row['id'])
                id_last_node = name+str(row['id'])
                c.node(
                    name+str(row['id']),
                    label=identifier + str(row['id']) + ', Sample ' + str(row['sample_number']) + ',\n' +
                          row['task_type'] + ' ' + row['device'] + '(' + str(row['channel']) + ')'
                )
        else:
            idstr = str(uuid.uuid4())
            if id_first_node is None:
                id_first_node = idstr
            id_last_node = idstr
            c.node(idstr, label=' ', style='invisible')

    return id_first_node, id_last_node


def render_data(data, color, filename, split_by_device=False, edges=None):
    g = graphviz.Digraph('gvg')
    if split_by_device:
        edge_nodes = {}
        grouped = data.groupby('device')
        # render initialized devices that do not have active tasks
        for key in list(channel_po.keys()):
            if key not in grouped.groups:
                first, last = render_cluster(None, g, name=key, color='lightgreen')
                edge_nodes[key] = [first, last]
        # render each active device separately
        for device in grouped.groups:
            device_df = grouped.get_group(device)
            # st.dataframe(device_df)
            first, last = render_cluster(device_df, g, name=device_df.at[0, 'device'], color='lightgreen')
            edge_nodes[device_df.at[0, 'device']] = [first, last]
        for entry in edges:
            # draw edge from last to first node in the two clusters that should be connected given in the
            # edges dictionary
            if entry[0] in edge_nodes and entry[1] in edge_nodes:
                g.edge(edge_nodes[entry[0]][1], edge_nodes[entry[1]][0])
    else:
        render_cluster(data, g, name=filename, color=color)
    g.render(filename=os.path.join(storage_path, filename), format='png')


@st.cache_data
def render_all_queues(pdata, adata, hdata, edges, filemodflag):
    render_data(pdata, color='lightblue', filename='priority_queue')
    render_data(adata, color='lightgreen', filename='active_queue', split_by_device=True, edges=edges)
    render_data(hdata, color='orange', filename='history_queue')

# ---------------------------------------------------------------------------------------------------------------------
# --------------------------------------------- Streamlit Page Start --------------------------------------------------


identifier_list = []
storage_path = '../test/'

file_mod_date = file_mod_time()
if st.session_state['file_mod_date'] is None or st.session_state['file_mod_date'] != file_mod_date:
    priority_queue, active_queue, history_queue, channel_po, edges = load_all(file_mod_time())
    render_all_queues(priority_queue, active_queue, history_queue, edges, file_mod_time())

st.title('Autocontrol Viewer')

# create flow chart via graphviz
st.text('Flow Chart')
st.image(os.path.join(storage_path, 'priority_queue.png'))
st.image(os.path.join(storage_path, 'active_queue.png'))
st.image(os.path.join(storage_path, 'history_queue.png'))

# visualize dataframes in tables
co_list = ("id", "priority", "sample_number", "task_type", "device", "channel", "target_device", "target_channel", "md")
co_conf = {"sample_number": "sample",
                            "task_type": "task",
                            "md": "meta data",
                            "target_device": "target device",
                            "target_channel": "target channel"
           }
st.text('Queued Jobs:')
st.dataframe(priority_queue, column_order=co_list, column_config=co_conf, use_container_width=True)
st.text('Active Jobs:')
st.dataframe(active_queue, column_order=co_list, column_config=co_conf, use_container_width=True)
st.text('Finished Jobs:')
st.dataframe(history_queue, column_order=co_list, column_config=co_conf, use_container_width=True)

# if st.button('Reload', type="primary"):
time.sleep(10)
# st.cache_data.clear()
st.rerun()
