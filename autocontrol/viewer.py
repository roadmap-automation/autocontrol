import graphviz
import os
import time

import streamlit as st
import pandas as pd
import numpy as np

from urllib.parse import urljoin

st.set_page_config(layout="wide")
st.title('Autocontrol Viewer')

# load status from SQLlite databases
storage_path = '../test/'

localhost = "sqlite:///"
absolute_path = os.path.abspath(os.path.join(storage_path, 'priority_queue.sqlite3'))
url = localhost + absolute_path
conn = st.connection('priority_queue', type='sql', url=url)
priority_queue = conn.query('select * from task_table')

absolute_path2 = os.path.abspath(os.path.join(storage_path, 'active_queue.sqlite3'))
url2 = localhost + absolute_path2
conn2 = st.connection('active_queue', type='sql', url=url2)
active_queue = conn2.query('select * from task_table')

absolute_path3 = os.path.abspath(os.path.join(storage_path, 'history_queue.sqlite3'))
url3 = localhost + absolute_path3
conn3 = st.connection('history_queue', type='sql', url=url3)
history_queue = conn3.query('select * from task_table')

# remove active jobs out of history queue:
history_queue = history_queue[~history_queue['id'].isin(active_queue['id'])]


# create flow chart via graphviz
def render_cluster(data, graph, name='0', color='grey'):
    with graph.subgraph(name='cluster_'+name) as c:
        c.attr(fillcolor=color, label=name, style='filled')
        c.attr('node', shape='box', style='filled', fillcolor='grey')
        for index, row in data[::-1].iterrows():
            c.node(
                'ID:' + str(row['id']) + ', Sample ' + str(row['sample_number']) + ',\n' + row['task_type'] + ' ' +
                row['device'] + '(' + str(row['channel']) + ')')

g = graphviz.Graph('gvg')
render_cluster(priority_queue, g, name='Priority Queue', color='lightblue')

# render each active device separately
grouped = active_queue.groupby('device')
for device in grouped.groups:
    device_df = grouped.get_group(device)
    # st.dataframe(device_df)
    render_cluster(device_df, g, name=device_df.at[0, 'device'], color='lightgreen')

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

st.button('Reload', type="primary")
# time.sleep(5)
#st.rerun()
