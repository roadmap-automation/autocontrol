from __future__ import annotations

import json
import subprocess
import platform
from pathlib import Path
import shlex

import streamlit as st
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

# TODO: Block UI access when autocontrol server is running

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
else:
    st.write("""
        ## User
                 """)
    user_list = []
    default_user = None
    root = st.session_state.user_root_dir
    if root.is_dir():
        user_list = [p.name for p in root.iterdir() if p.is_dir() and not p.name.startswith(".")]
        user_list.sort()
    if st.session_state.cfg.user_name is not None:
        if st.session_state.cfg.user_name not in user_list:
            user_list.append(st.session_state.cfg.user_name)
            user_list.sort()
        default_user = user_list.index(st.session_state.cfg.user_name)
    user = st.selectbox(
        "User Name",
        options=user_list,
        index=default_user,
        placeholder='Create or select a user.',
        accept_new_options=True)
    if user and user != st.session_state.cfg.user_name:
        st.session_state.cfg.user_name = user
        st.session_state.cfg.project = None
        st.session_state.cfg.campaign = None
        st.session_state.cfg.experiment = None
        configuration.save_persistent_cfg(st.session_state.cfg)
    if st.session_state.cfg.user_name is None:
        st.stop()

    st.session_state.dataroot_dir = st.session_state.user_root_dir / cfg.user_name

    col1, col2, col3 = st.columns([6, 1, 3])
    info_text = "Data root directory " + str(st.session_state.dataroot_dir)
    if st.session_state.dataroot_dir.is_dir():
        info_text += " exists."
        with col1:
            st.text(info_text)
        with col2:
            file_browser_button(st.session_state.dataroot_dir)
    else:
        info_text += " has not been created, yet."
        with col1:
            st.text(info_text)
        with col3:
            if st.button("Create Data Root Directory", type='primary'):
                st.session_state.dataroot_dir.mkdir(parents=True, exist_ok=True)
                st.rerun()

    st.write("""
        ## Project / Campaign / Experiment
        """)

    project_list = []
    default_project = None
    root = st.session_state.dataroot_dir
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
## Storage Directory
""")
col7, col8 = st.columns([7, 3])
with col7:
    st.info(f"Autocontrol storage directory: {st.session_state.cfg.autocontrol_dir}")
with col8:
    if cfg.autocontrol_startup:
        if st.button("Reset to Default"):
            st.session_state.cfg.autocontrol_dir = exp_dir / 'autocontrol'
            configuration.save_persistent_cfg(st.session_state.cfg)
    else:
        file_browser_button(st.session_state.cfg.autocontrol_dir)


st.write("""
## DataLad
""")
if st.session_state.storage_path_overwrite:
    st.info("Storage path outside Datalad repository or not below experiment level. Datalad and remote storage"
            "disabled.")
    st.stop()

use_datalad = st.toggle(label='Use DataLad', value=st.session_state.cfg.use_datalad)
if use_datalad != st.session_state.cfg.use_datalad:
    st.session_state.cfg.use_datalad = use_datalad
    configuration.save_persistent_cfg(st.session_state.cfg)

if not st.session_state.cfg.use_datalad:
    st.stop()

app_functions.setup_app_dirs(create_dirs=False, init_datalad=True)
dm = st.session_state.datamanager

root_dir = st.session_state.dataroot_dir
project_dir = root_dir / st.session_state.cfg.project
campaign_dir = project_dir / st.session_state.cfg.campaign
exp_dir = campaign_dir / st.session_state.cfg.experiment

_, r_installed, r_status = dm.get_status(dataset=root_dir, recursive=False)
_, p_installed, p_status = dm.get_status(dataset=project_dir, recursive=False)
_, c_installed, c_status = dm.get_status(dataset=campaign_dir, recursive=False)
_, e_installed, e_status = dm.get_status(dataset=exp_dir, recursive=False)
ds_installed = r_installed and p_installed and c_installed and e_installed

#all dirs exists at this point in the script as checked above
col9, col10 = st.columns([7, 3])
if not ds_installed:
    with col9:
        st.info('DataLad branch (project / campaign / experiment) is not (fully) initialized.')
    with col10:
        if st.button("Initialize DataLad Tree.", type='primary'):
            # ensure that data structure is a datalad tree
            dm.init_tree(project=cfg.project, campaign=cfg.campaign, experiment=cfg.experiment, force=True)
            st.rerun()
    st.stop()
else:
    # st.info(e_status)
    status = r_status + p_status + c_status + e_status
    clean = True
    for element in status:
        if element['state'] != 'clean':
            clean = False
    if clean:
        with col9:
            st.success('DataLad branch (project / campaign / experiment) is saved (clean).')
    else:
        with col9:
            st.warning('DataLad branch (project / campaign / experiment) has unsaved changes.')
        with col10:
            if st.button("Save DataLad Branch.", type='primary'):
                dm.save(path=exp_dir, recursive=True)
                dm.save(path=campaign_dir, recursive=False)
                dm.save(path=project_dir, recursive=False)
                dm.save(path=root_dir, recursive=False)
                st.rerun()
        st.stop()

with st.expander(label='Detailed Status', expanded=False):
    only_non_clean = st.toggle(label='Show only non-clean entries.', value=True)
    if only_non_clean:
        status = [element for element in status if element['state']!='clean']
    # Pretty-print the combined DataLad status (list of dicts) as JSON.
    st.text(json.dumps(status, indent=2, sort_keys=True, default=str))


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

    gin_user = st.text_input('GIN User', value=st.session_state.cfg.GIN_user)
    if gin_user is not None and  gin_user != st.session_state.cfg.GIN_user:
        st.session_state.cfg.GIN_user = gin_user
        configuration.save_persistent_cfg(st.session_state.cfg)

    ssh_hostname = st.text_input('GIN URL / SSH Host Name', value=st.session_state.cfg.GIN_url)
    if ssh_hostname != st.session_state.cfg.GIN_url:
        st.session_state.cfg.GIN_url = ssh_hostname
        configuration.save_persistent_cfg(st.session_state.cfg)

    ssh_host_alias = st.text_input('GIN / SSH Host Alias for .ssh/config', value=st.session_state.cfg.SSH_host_alias)
    if ssh_host_alias != st.session_state.cfg.SSH_host_alias:
        st.session_state.cfg.SSH_host_alias = ssh_host_alias
        configuration.save_persistent_cfg(st.session_state.cfg)

    st.text("We use SSH key authentication for communicating with GIN. This section will ensure the proper key setup.")

    # GIN SSH UI section (example snippet)
    ssh_host_alias_default = ssh_hostname
    ssh_host_user = 'git' if ssh_hostname == 'gin.g-node.org' else gin_user

    config_file = app_functions.ssh_config_path()
    found, message = app_functions.ssh_config_has_entry(ssh_host_alias, ssh_hostname, ssh_host_user)

    suggested_private_key = app_functions.ssh_default_key_path(ssh_hostname, ssh_host_user)
    private_key_path = Path(suggested_private_key).expanduser()
    public_key_path = private_key_path.with_suffix('.pub')

    if found:
        st.success(message)
        if public_key_path.exists():
            st.text(f"Here is the folder with you public key '{str(public_key_path.name)}' that should be provided to "
                    f"your gin.g-node.org account.")
        else:
            st.text(f"Although an entry for the host and user was found in the SSH config file, no key was found under "
                    f"the canonical name: '{str(public_key_path.name)}'. It might be missing or under a different name."
                    f" Either regenerate a new key pair or provide the differently named key to gin.g-node.org. Inspect"
                    f" or clean up .ssh/config for a coherent setup.")
    else:
        st.info(message)
        if st.button("Create new SSH key pair."):


            st.text("This creates an SSH ed25519 key pair locally. You can then copy the public key into your GIN "
                    "account manually.")

            comment = f"{ssh_host_user}@{ssh_hostname}"
            success, message = app_functions.ssh_generate_keypair(private_key_path=private_key_path, comment=comment)
            if success:
                st.success(message)
            else:
                st.error(message)
        st.stop()

    col11, col12, col13 = st.columns([3, 4, 3])
    with col11:
        file_browser_button(public_key_path.parent, label="Show SSH Directory ↗️")
    with col12:
        if st.button("Test SSH Connection", type='primary'):
            ok, summary, details = app_functions.ssh_test_connection(ssh_host_alias)
            if ok:
                st.success(summary)
            else:
                st.error(summary)
            if details:
                st.code(details)
            st.caption(f"Command: {shlex.join(['ssh', '-T', '-o', 'BatchMode=yes', ssh_host_alias])}")

            if not ok:
                st.stop()

with st.expander(label='Repository Actions', expanded=True):
    status = dm.get_git_sync_status(dataset=exp_dir)
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
                dataset=root_dir,
                recursive=True,
                push_annex_data=True
            )
            st.rerun()
        st.stop()

    if state in ['branch_failed', 'detached_head', 'no_upstream', 'compare_failed', 'parse_failed']:
        st.error(state + ': ' + message)
        st.text('A solution to this problem is outside the abilities of this script.')
        st.stop()

    if state == 'fetch_failed':
        status_parent = dm.get_git_sync_status(dataset=exp_dir, from_parent=True)

        st.error('Status from Experiment Dataset: ' + state + ': ' + message)
        st.info('Status from parent Category Dataset: ' + status_parent['state'] + ': ' + status_parent['message'])

        if status_parent['ok'] and status_parent['state'] != 'no_remote':
            st.text('The parent dataset repository appears to be o.k. If the remote repository for the experiment '
                    'dataset has been deleted, you can try to remove and republish the experiment dataset only.')
            if st.button('Remove and republish stale remote siblings for entire Datalad tree', type='primary'):
                dm.remove_siblings(dataset=exp_dir, recursive=False)
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
                dm.remove_siblings(dataset=root_dir, recursive=True)
                st.rerun()

    if state == 'up_to_date':
        st.success("Local and remote branches are up-to-date.")
    elif state == 'ahead':
        st.warning("Local branch is ahead.")
        if st.button('Push local branch to remote.', type='primary'):
            dm.push_to_remotes(dataset=exp_dir, recursive=True, push_annex_data=True)
            st.rerun()
    elif state == 'behind':
        st.warning("Local branch is behind.")
        if st.button('Update local branch from remote.', type='primary'):
            dm.pull_from_remotes(dataset=exp_dir, recursive=True)
            dm.get_content(dataset=exp_dir, recursive=True)
            st.rerun()
    elif state == 'diverged':
        st.warning("Local branch and remote are diverged. Feel free to sync manually.")
        col14, col15 = st.columns([5, 5])
        with col14:
            if st.button('Update local branch from remote.', type='primary'):
                dm.pull_from_remotes(dataset=exp_dir, recursive=True)
                dm.get_content(dataset=exp_dir, recursive=True)
                st.rerun()
        with col15:
            if st.button('Push local branch to remote.', type='primary'):
                dm.push_to_remotes(dataset=exp_dir, recursive=True, push_annex_data=True)
                st.rerun()