from __future__ import annotations

from pathlib import Path
import streamlit as st

from autocontrol.support import configuration


def setup_app_dirs(
        user_root_dir: str | Path = None,
        create_dirs=False):
    """
    Sets up directories for app use. Initializes the datamanager.
    :param user_root_dir: (bool) root dir path under which user (root) datamanager datasets will be situated
    :param create_dirs: (bool) whether to create directories if they do not exist
    :return:
    """
    # check if canonical app working directories exist
    if user_root_dir is None:
        user_root_dir = Path.home() / "app_data"
    else:
        user_root_dir = Path(user_root_dir).expanduser().resolve()
    user_root_dir.mkdir(parents=True, exist_ok=True)
    st.session_state['user_root_dir'] = user_root_dir

    # load config file from disc
    cfg = configuration.load_persistent_cfg()
    st.session_state["cfg"] = cfg

    # default data root based on username
    dataroot_dir = user_root_dir / cfg.user_name
    st.session_state['dataroot_dir'] = dataroot_dir

    if cfg.project is None or cfg.campaign is None or cfg.experiment is None:
        st.session_state["data_folders_ready"] = False
        return

    exp_root = dataroot_dir / cfg.project / cfg.campaign / cfg.experiment
    if not (exp_root.is_dir() or create_dirs):
        st.session_state["data_folders_ready"] = False
        return

    st.session_state["data_folders_ready"] = True
    dataroot_dir.mkdir(parents=True, exist_ok=True)
    exp_root.mkdir(parents=True, exist_ok=True)
