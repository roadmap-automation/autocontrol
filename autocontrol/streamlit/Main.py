from roadmap_datamanager import datalad_gin_api as dgapi
from roadmap_datamanager import configuration as dmc
from autocontrol.support import configuration

import argparse
from pathlib import Path
import streamlit as st

st.set_page_config(layout="wide")

def main(storage_path=None, atc_address=None):

    st.session_state.cfg = configuration.load_persistent_cfg()
    st.session_state.cfg.atc_address = atc_address or st.session_state.cfg.atc_address or '5004'
    st.session_state["user_selection_enabled"] = True

    if storage_path is None:
        # use datamanager functionality to select paths and use Datalad / Remote storage integration
        st.session_state.storage_path_overwrite = False
        st.session_state.cfg.data_folders_ready = False
        print("No storage path provided. Select experiment in the File System tab of the Streamlit App and "
              "authorize the autocontrol server startup manually.")
    else:
        # check if provided storage path is within a Datalad repository
        node_type, _ = dgapi.get_dataset_nodetype(storage_path)
        if node_type == 'outside datalad' or node_type == 'not a datamanager repository':
            # manual overwrite for autocontrol storage path
            print("Storage path provided to autocontrol is not within a Datamanager Repository.")
            print("File System Tab deactivated.")
            st.session_state.storage_path_overwrite = True
            st.session_state.cfg.autocontrol_dir = str(storage_path)
            st.session_state.data_folders_ready = True
            # authorize autocontrol server startup
            st.session_state.cfg.autocontrol_startup = False
            configuration.save_persistent_cfg(st.session_state.cfg)
        elif node_type in ['root', 'project', 'campaign', 'experiment']:
            st.error("Storage path provided at startup is at a dataset level of a Datamanager Repository. The"
                     "storage path should be at a below-experiment level. This location interferes with "
                     "Autocontrol functionality.")
            st.session_state.data_folders_ready = False
            st.stop()
        else:
            print("Storage path provided to autocontrol is within existing Datamanager Repository.")
            print("Datalad and Remote storage capabilities available via the Streamlit App. User selection disabled.")
            # initialize a datamanager instance just for bootstrapping, thereby updating the config
            st.session_state.cfg = dmc.bootstrap_config(
                path=storage_path,
                cfg=st.session_state.cfg
            )
            st.session_state.storage_path_overwrite = False
            st.session_state.cfg.autocontrol_dir = str(storage_path)
            # user provided an autocontrol directory that resides in an existing datalad tree, but we have no way of
            # knowing whether there are multiple users -> chose name of dm_root as user and make it fixed.
            st.session_state.user_root_dir = Path(st.session_state.cfg.dm_root).expanduser().resolve().parent
            st.session_state["data_folders_ready"] = True
            st.session_state["user_selection_enabled"] = False
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

    st.write("""
    # AutoControl Viewer
    """)

    if st.session_state.cfg.autocontrol_startup:
        st.warning("Autocontrol needs storage path setup and server startup authorization. "
                   "Please see the File System tab.")

    st.write("""
    Welcome to Autocontrol.
    
    ## Startup
    
    A startup of Autocontrol can be performed as shown below. If no autocontrol storage path is provided, a path can be 
    selected in the File System tab.
    
    from autocontrol.support import support
    
    support.start(portnumber=5004, storage_path=None)
    
    ## Storage
    
    While almost any autocontrol storage path can be provided manually, DataLad and remote storage integration prefers
    the storage path directly under the 'Experiment' level of a User / Project / Campaign / Experiment
    Datalad repository. The storage path should be named 'autocontrol' for it to be recognized as such. The File System
    tab will facilitate this convention.
    
    Autocontrol uses SQLite databases for task data storage. Those are not readily archivable in a Git Annex repository,
    because they are frequently modified in place and can be quite large. Therefore, an archive 
    functionality has been implemented.
    """)

if __name__ == '__main__':
    # sys.argv = sys.argv[:1] + sys.argv[2:]  # Streamlit adds extra args; this line removes them
    parser = argparse.ArgumentParser()
    parser.add_argument('--storage_dir', type=str, default=None, help='Path to storage directory')
    parser.add_argument('--atc_address', type=str, default='http://localhost:5000',
                        help='Address of atc server')
    args = parser.parse_args()
    storage_dir = args.storage_dir
    atc_address = args.atc_address

    main(storage_path=storage_dir, atc_address=atc_address)
