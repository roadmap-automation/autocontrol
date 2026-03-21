from __future__ import annotations

import json
import shutil
import subprocess
import platform
from pathlib import Path

import streamlit as st

from roadmap_datamanager import datamanager
from roadmap_datamanager import datalad_gin_api as dgapi
from roadmap_datamanager.gui import streamlit_components as stc

from autocontrol.support import configuration
from autocontrol.support import app_functions

st.set_page_config(layout="wide")

def open_in_file_browser(path: Path):
    if not path.exists():
        return

    system = platform.system()

    if system == "Darwin":        # macOS
        subprocess.run(["open", path])
    elif system == "Windows":
        subprocess.run(["explorer", path])
    elif system == "Linux":
        subprocess.run(["xdg-open", path])

def file_browser_button(path: Path, label="↗️"):
    if path.exists():
        if st.button(label, help=f"Open {path}"):
            open_in_file_browser(path)

def start_server():
    st.session_state.cfg.autocontrol_startup = False
    Path(st.session_state.cfg.autocontrol_dir).mkdir(parents=True, exist_ok=True)
    configuration.save_persistent_cfg(st.session_state.cfg)

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
    cfg= stc.UI_fragment_user(
        cfg,
        user_root_dir=st.session_state.user_root_dir
    )
    st.session_state.cfg = cfg
    configuration.save_persistent_cfg(st.session_state.cfg)
    dm_root = st.session_state.cfg.dm_root
    if dm_root is None or not dm_root.is_dir():
        st.stop()

    st.write("""
        ## Project / Campaign / Experiment
        """)

    project_list = []
    default_project = None
    root = dm_root
    if root.is_dir():
        project_list = [p.name for p in root.iterdir() if p.is_dir() and not p.name.startswith(".")]
        project_list.sort()
    if st.session_state.cfg.project is not None:
        if st.session_state.cfg.project not in project_list:
            project_list.append(st.session_state.cfg.project)
            project_list.sort()
        default_project = project_list.index(st.session_state.cfg.project)
    project = st.selectbox(
        "Project Name",
        options=project_list,
        index=default_project,
        placeholder='Create or select a project.',
        accept_new_options=True)
    if project and project != st.session_state.cfg.project:
        st.session_state.cfg.project = project
        configuration.save_persistent_cfg(st.session_state.cfg)
        app_functions.setup_app_dirs(create_dirs=False, init_datalad=False)
    if st.session_state.cfg.project is None:
        st.stop()

    campaign_list = []
    default_campaign = None
    root = st.session_state.dataroot_dir / st.session_state.cfg.project
    if root.is_dir():
        campaign_list = [p.name for p in root.iterdir() if p.is_dir() and not p.name.startswith(".")]
        campaign_list.sort()
    if st.session_state.cfg.campaign is not None:
        if st.session_state.cfg.campaign not in campaign_list:
            campaign_list.append(st.session_state.cfg.campaign)
            campaign_list.sort()
        default_campaign = campaign_list.index(st.session_state.cfg.campaign)
    campaign = st.selectbox(
        "Campaign Name",
        options=campaign_list,
        index=default_campaign,
        placeholder='Create or select a campaign.',
        accept_new_options=True)
    if campaign and campaign != st.session_state.cfg.campaign:
        st.session_state.cfg.campaign = campaign
        configuration.save_persistent_cfg(st.session_state.cfg)
        app_functions.setup_app_dirs(create_dirs=False, init_datalad=False)
    if st.session_state.cfg.campaign is None:
        st.stop()

    experiment_list = []
    default_experiment = None
    root = st.session_state.dataroot_dir / st.session_state.cfg.project / st.session_state.cfg.campaign
    if root.is_dir():
        experiment_list = [p.name for p in root.iterdir() if p.is_dir() and not p.name.startswith(".")]
        experiment_list.sort()
    if st.session_state.cfg.experiment is not None:
        if st.session_state.cfg.experiment not in experiment_list:
            experiment_list.append(st.session_state.cfg.experiment)
            experiment_list.sort()
        default_experiment = experiment_list.index(st.session_state.cfg.experiment)
    experiment = st.selectbox(
        "Experiment Name",
        options=experiment_list,
        index=default_experiment,
        placeholder='Create or select an experiment.',
        accept_new_options=True)
    if experiment and experiment != st.session_state.cfg.experiment:
        st.session_state.cfg.experiment = experiment
        configuration.save_persistent_cfg(st.session_state.cfg)
        app_functions.setup_app_dirs(create_dirs=False, init_datalad=False)
    if st.session_state.cfg.experiment is None:
        st.stop()

    col4, col5, col6 = st.columns([6, 1, 3])
    exp_dir = root / st.session_state.cfg.experiment
    info_text = "Experiment directory " + str(exp_dir)
    if exp_dir.is_dir():
        info_text += " exists."
        with col4:
            st.text(info_text)
        with col5:
            file_browser_button(exp_dir)

    else:
        info_text += " has not been created, yet."
        with col4:
            st.text(info_text)
        with col6:
            if st.button("Create Experimental Directory", type='primary'):
                # exp_dir.mkdir(parents=True, exist_ok=True)
                app_functions.setup_app_dirs(create_dirs=True)
                st.rerun()
        st.stop()

st.write("""
## Autocontrol Storage Directory
""")
exp_dir = root = st.session_state.dataroot_dir / st.session_state.cfg.project / st.session_state.cfg.campaign
exp_dir = exp_dir / st.session_state.cfg.experiment
col7, col8 = st.columns([7, 3])
with col7:
    st.session_state.cfg.autocontrol_dir = str(exp_dir / 'autocontrol')
    st.success(f"Autocontrol storage directory: {st.session_state.cfg.autocontrol_dir}")
with col8:
    if cfg.autocontrol_startup:
        st.button("Authorize Autocontrol Server Startup", type='primary', on_click=start_server)
    else:
        file_browser_button(Path(st.session_state.cfg.autocontrol_dir))

if (exp_dir / 'autocontrol').is_dir():
    st.text("The autocontrol storage folder is not archived due to frequent in-place modification. ")
    if st.button("Make an archived copy of the storage directory"):
        archive_dir = exp_dir / "autocontrol_archive"
        if archive_dir.exists():
            shutil.rmtree(archive_dir)

        shutil.copytree((exp_dir / 'autocontrol'), (exp_dir / 'autocontrol_archive'))

if st.session_state.storage_path_overwrite:
    st.write("""
    ## DataLad
    """)
    st.info("Storage path outside Datalad repository or not below experiment level. Datalad and remote storage"
            "disabled.")
    st.stop()

# --------------------- Datalad UI fragment --------------------------
cfg, dm = stc.UI_fragment_datalad(
    cfg=st.session_state.cfg
)
st.session_state.cfg = cfg
st.session_state.datamanager = dm
configuration.save_persistent_cfg(st.session_state.cfg)
if not st.session_state.cfg.use_datalad or dm is None:
    st.stop()


st.write("""
## GIN Remote Storage
""")

use_GIN = st.toggle(label='Use GIN', value=st.session_state.cfg.use_GIN)
if use_GIN != st.session_state.cfg.use_GIN:
    st.session_state.cfg.use_GIN = use_GIN
    configuration.save_persistent_cfg(st.session_state.cfg)

if not use_GIN:
    st.stop()

with st.expander(label='Connection Setup', expanded=False):
    st.session_state.cfg = stc.UI_fragment_SSH_connection(st.session_state.cfg)
    configuration.save_persistent_cfg(st.session_state.cfg)

with st.expander(label='Repository Actions', expanded=True):
    status = dgapi.get_git_sync_status(dataset=exp_dir)
    ok = status['ok']
    state = status['state']
    message = status['message']

    with st.expander(label='Detailed Status', expanded=False):
        st.text(json.dumps(status, indent=2, sort_keys=True, default=str))

    if state == "not_dataset":
        st.info(message)
        st.error("This should never happen at this point in the script.")
        st.stop()

    if state == "no_remote":
        st.info(message)
        st.text("Experiment does not yet have a remote repository. When creating a remote repository for the current "
                "experiment, repositories for all other projects / campaigns / experiments will be created or updated.")
        if st.button("Create Remote Repository", type='primary'):
            dm.publish_gin_sibling(
                sibling_name='gin',
                repo_name=st.session_state.cfg.user_name,
                dataset=dm_root,
                recursive=True,
                push_annex_data=True,
                existing='reconfigure'
            )
            st.rerun()
        st.stop()

    if state in ['branch_failed', 'detached_head', 'no_upstream', 'compare_failed', 'parse_failed']:
        st.error(state + ': ' + message)
        st.text('A solution to this problem is outside the abilities of this script.')
        st.stop()

    if state == 'fetch_failed':
        status_parent = dgapi.get_git_sync_status(dataset=exp_dir, from_parent=True)

        st.error('Status from Experiment Dataset: ' + state + ': ' + message)
        st.info('Status from parent Category Dataset: ' + status_parent['state'] + ': ' + status_parent['message'])

        if status_parent['ok'] and status_parent['state'] != 'no_remote':
            st.text('The parent dataset repository appears to be o.k. If the remote repository for the experiment '
                    'dataset has been deleted, you can try to remove and republish the experiment dataset only.')
            if st.button('Remove and republish stale remote siblings for entire Datalad tree', type='primary'):
                dgapi.remove_siblings(dataset=exp_dir, recursive=False)
                dm.publish_gin_sibling(
                    sibling_name='gin',
                    repo_name=st.session_state.cfg.user_name,
                    dataset=exp_dir,
                    recursive=False,
                    push_annex_data=True
                )
                st.rerun()
        else:
            st.text("The remote parent dataset repository appears to be not o.k., as well. If the entire remote "
                    "repository tree has been deleted, you can try to remove all all stale siblings and republish "
                    "the entire tree again.")
            if st.button('Remove and republish stale remote siblings for the current Experiment only.',
                         type='primary'):
                dgapi.remove_siblings(dataset=dm_root, recursive=True)
                st.rerun()

    if state == 'up_to_date':
        st.success("Local and remote branches are up-to-date.")
    elif state == 'ahead':
        st.warning("Local branch is ahead.")
        if st.button('Push local branch to remote.', type='primary'):
            dgapi.push_to_remotes(dataset=exp_dir, recursive=True, push_annex_data=True)
            st.rerun()
    elif state == 'behind':
        st.warning("Local branch is behind.")
        if st.button('Update local branch from remote.', type='primary'):
            dgapi.pull_from_remotes(dataset=exp_dir, recursive=True)
            dgapi.get_content(dataset=exp_dir, recursive=True)
            st.rerun()
    elif state == 'diverged':
        st.warning("Local branch and remote are diverged. Feel free to sync manually.")
        col14, col15 = st.columns([5, 5])
        with col14:
            if st.button('Update local branch from remote.', type='primary'):
                dgapi.pull_from_remotes(dataset=exp_dir, recursive=True)
                dgapi.get_content(dataset=exp_dir, recursive=True)
                st.rerun()
        with col15:
            if st.button('Push local branch to remote.', type='primary'):
                dgapi.push_to_remotes(dataset=exp_dir, recursive=True, push_annex_data=True)
                st.rerun()