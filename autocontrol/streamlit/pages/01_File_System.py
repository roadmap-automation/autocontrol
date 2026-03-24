from __future__ import annotations

import shutil
from pathlib import Path
import streamlit as st

from roadmap_datamanager.gui import streamlit_components as stc
from autocontrol.support import configuration

st.set_page_config(layout="wide")

def start_server():
    st.session_state.cfg.autocontrol_startup = False
    configuration.save_persistent_cfg(st.session_state.cfg)


# --------------------------------- Streamlit UI Start --------------------------
if st.session_state.storage_path_overwrite:
    st.info(f"Autocontrol storage path has been overwritten at startup to '{st.session_state.cfg.autocontrol_dir}'. "
            f"Datamanager's Datalad and remote storage capabilities are not available. Start autocontrol with "
            f"storage_path=None to use the datamanager or provide a storage path that lies within a Datalad "
            f"repository.")
    st.stop()

cfg = st.session_state.cfg

st.write("""
# File System
""")
if not cfg.autocontrol_startup:
    st.info("Autocontrol server startup has been authorized. No change of storage directory possible.")
    dm_root = Path(cfg.dm_root).expanduser().resolve()
else:
    # ----------------------- User dialog --------------------------------------
    if st.session_state.user_selection_enabled:
        if 'user_root_dir' not in st.session_state:
            st.session_state.user_root_dir = Path.home() / "app_data"

    cfg= stc.UI_fragment_user(
        cfg,
        user_root_dir=st.session_state.user_root_dir,
        enable_user_selection=st.session_state.user_selection_enabled
    )
    st.session_state.cfg = cfg
    configuration.save_persistent_cfg(st.session_state.cfg)

    dm_root = st.session_state.cfg.dm_root
    if dm_root is None or not dm_root.is_dir():
        st.stop()

    # ------------------ Project/Campaign/Experiment Diaolog -------------------
    cfg, st.session_state.data_folders_ready, rerun = stc.UI_fragment_PCE(cfg)
    st.session_state.cfg = cfg
    configuration.save_persistent_cfg(st.session_state.cfg)
    if rerun:
        st.rerun()
    if not st.session_state.data_folders_ready:
        st.stop()

# -------------------- Storage Directory --------------------------------------
cfg, rerun = stc.UI_fragment_app_storage(
    cfg=cfg,
    storage_folders=['autocontrol'],
    gitignore_folders=['autocontrol'],
    special_action=start_server,
    special_action_arguments=None,
    special_action_label='Authorize Autocontrol Server Startup',
    special_action_enabled=st.session_state.cfg.autocontrol_startup
)
st.session_state.cfg = cfg
configuration.save_persistent_cfg(st.session_state.cfg)
st.session_state.cfg.autocontrol_dir = (Path(cfg.dm_root).expanduser().resolve() / cfg.project / cfg.campaign /
                                        cfg.experiment / 'autocontrol')
if rerun:
    st.rerun()

# --------------------- Datalad UI fragment --------------------------
if st.session_state.storage_path_overwrite:
    st.write("""
    ## DataLad
    """)
    st.info("Storage path outside Datalad repository or not below experiment level. Datalad and remote storage"
            "disabled.")
    st.stop()

cfg, dm = stc.UI_fragment_datalad(
    cfg=st.session_state.cfg
)
st.session_state.cfg = cfg
st.session_state.datamanager = dm
configuration.save_persistent_cfg(st.session_state.cfg)
if not st.session_state.cfg.use_datalad or dm is None:
    st.stop()

# ---------------------- GIN remote storage ----------------------------
cfg, rerun = stc.UI_fragment_GIN_actions(st.session_state.cfg, st.session_state.datamanager)
st.session_state.cfg = cfg
configuration.save_persistent_cfg(st.session_state.cfg)
if rerun:
    st.rerun()