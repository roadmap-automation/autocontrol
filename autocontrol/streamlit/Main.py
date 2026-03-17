from support import app_functions

from roadmap_datamanager import datamanager
from autocontrol.support import configuration

import argparse
import os
import streamlit as st

st.set_page_config(layout="wide")

def main(storage_path=None, atc_address=None):

    st.session_state.cfg = configuration.load_persistent_cfg()

    st.session_state.cfg.atc_address = atc_address or st.session_state.cfg.atc_address or '5004'
    storage_path = storage_path or st.session_state.cfg.autocontrol_dir or None

    if storage_path is None:
        # use datamanager functionality to select paths and use Datalad / Remote storage integration
        st.session_state.storage_path_overwrite = False
        st.session_state.cfg.autocontrol_dir = None
        app_functions.setup_app_dirs(create_dirs=False, init_datalad=False)
        print("No storage path provided. Select experiment in the File System tab of the Streamlit App and "
              "authorize the autocontrol server startup manually.")
        # storage path is contained in st.session_state.autocontrol_dir after setup_app_dirs()
        # storage_path = st.session_state.autocontrol_dir
    else:
        # check if provided storage path is within a Datalad repository
        node_type, _ = datamanager.get_dataset_nodetype(storage_path)
        if node_type == 'outside datalad' or node_type == 'not a datamanager repository':
            # manual overwrite for autocontrol storage path
            print("Storage path provided to autocontrol is not within a Datamanager Repository.")
            print("File System Tab deactivated.")
            st.session_state.storage_path_overwrite = True
            st.session_state.cfg.autocontrol_dir = storage_path
            st.session_state.data_folders_ready = True
            # authorize autocontrol server startup
            st.session_state.cfg.autocontrol_startup = False
            configuration.save_persistent_cfg(st.session_state.cfg)
        elif node_type in ['root', 'project', 'campaign', 'experiment']:
            print("WARNING: Storage path provided at startup is at a dataset level of a Datamanager Repository.")
            print("The storage path should be at a below-experiment level. Datalad and Remote storage capabilities"
                  "not available via the Streamlit App.")
            st.session_state.storage_path_overwrite = True
            st.session_state.cfg.autocontrol_dir = storage_path
            st.session_state.data_folders_ready = True
            # authorize autocontrol server startup
            st.session_state.cfg.autocontrol_startup = False
            configuration.save_persistent_cfg(st.session_state.cfg)
        else:
            print("Storage path provided to autocontrol is within existing Datamanager Repository.")
            print("Datalad and Remote storage capabilities available via the Streamlit App.")
            # initialize a datamanager instance just for bootstrapping, thereby updating the config
            _ = datamanager.DataManager(bootstrap_path = storage_path)
            st.session_state.storage_path_overwrite = False
            st.session_state.cfg.autocontrol_dir = storage_path
            app_functions.setup_app_dirs(create_dirs=False, init_datalad=False)
            # authorize autocontrol server startup
            st.session_state.cfg.autocontrol_startup = False
            configuration.save_persistent_cfg(st.session_state.cfg)

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

    configuration.save_persistent_cfg(st.session_state.cfg)

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
