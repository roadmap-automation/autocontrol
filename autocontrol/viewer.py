import graphviz
import os
import time

import streamlit as st
import pandas as pd
import numpy as np

identifier_list = []

@st.cache_data
def load_sql(filename):
    storage_path = '../test/'
    # load status from SQLlite databases
    localhost = "sqlite:///"
    absolute_path = os.path.abspath(os.path.join(storage_path, filename+'.sqlite3'))
    url = localhost + absolute_path
    conn = st.connection(filename, type='sql', url=url)
    try:
        df = conn.query('select * from task_table')
    except Exception:
        df = pd.DataFrame()
    return df


st.set_page_config(layout="wide")
st.title('Autocontrol Viewer')


priority_queue = load_sql('priority_queue')
active_queue = load_sql('active_queue')
history_queue = load_sql('history_queue')


# create flow chart via graphviz
def render_cluster(data, graph, name='0', color='grey'):
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

        for index, row in data[::-1].iterrows():
            c.node(
                identifier + ':' + str(row['id']) + ', Sample ' + str(row['sample_number']) + ',\n' + row['task_type']
                + ' ' + row['device'] + '(' + str(row['channel']) + ')')


g = graphviz.Graph('gvg')
render_cluster(priority_queue, g, name='Priority Queue', color='lightblue')

# render each active device separately
grouped = active_queue.groupby('device')
for device in grouped.groups:
    device_df = grouped.get_group(device)
    # st.dataframe(device_df)
    render_cluster(device_df, g, name=device_df.at[0, 'device'], color='lightgreen')

storage_path = '../test/'
render_cluster(history_queue, g, name='History Queue', color='orange')
g.render(filename=os.path.join(storage_path, 'gvg'), format='png')

# visualize flow chart
st.text('Flow Chart')
st.image(os.path.join(storage_path, 'gvg.png'))

# visualize dataframes in tables
st.text('Queued Jobs:')
st.dataframe(priority_queue,
             column_order=("id", "priority", "sample_number", "task_type", "device", "channel", "target_device",
                           "target_channel", "md"),
             column_config={"sample_number": "sample",
                            "task_type": "task",
                            "md": "meta data",
                            "target_device": "target device",
                            "target_channel": "target channel"
                            }
             )

st.text('Active Jobs:')
st.dataframe(active_queue,
             column_order=("id", "priority", "sample_number", "task_type", "device", "channel", "target_device",
                           "target_channel", "md"),
             column_config={"sample_number": "sample",
                            "task_type": "task",
                            "md": "meta data",
                            "target_device": "target device",
                            "target_channel": "target channel"
                            }
             )

st.text('Finished Jobs:')
st.dataframe(history_queue,
             column_order=("id", "priority", "sample_number", "task_type", "device", "channel", "target_device",
                           "target_channel", "md"),
             column_config={"sample_number": "sample",
                            "task_type": "task",
                            "md": "meta data",
                            "target_device": "target device",
                            "target_channel": "target channel"
                            }
             )


# if st.button('Reload', type="primary"):

time.sleep(10)
st.cache_data.clear()
st.rerun()
