from support import app_functions

import argparse
import os
import streamlit as st
import tempfile

st.set_page_config(layout="wide")

def main(storage_path=None, atc_address=None):
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


    st.session_state.atc_address = atc_address

    identifier_list = []
    if storage_path is None:
        storage_path = tempfile.mkdtemp()

    app_functions.setup_app_dirs(create_dirs=False, init_datalad=False)


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
