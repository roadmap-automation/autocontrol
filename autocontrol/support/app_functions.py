from __future__ import annotations

import os
from pathlib import Path
import streamlit as st
import subprocess

from roadmap_datamanager import datamanager
from autocontrol.support import configuration


def setup_app_dirs(
        user_root_dir: str | Path = None,
        create_dirs=False,
        init_datalad=False):
    """
    Sets up directories for app use. Initializes the datamanager.
    :param user_root_dir: (bool) root dir path under which user (root) datamanager datasets will be situated
    :param create_dirs: (bool) whether to create directories if they do not exist
    :param init_datalad: (bool) whether to initialize the DataLad repo in the app dir tree
    :return:
    """
    # check if canonical app working directories exist
    if user_root_dir is None:
        user_root_dir = Path.home() / "app_data"
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

    if st.session_state.cfg.autocontrol_dir is None:
        autocontrol_dir = exp_root / 'autocontrol'
        autocontrol_dir.mkdir(parents=True, exist_ok=True)
        # save paths to persistent session state
        st.session_state.cfg.autocontrol_dir = autocontrol_dir

    if init_datalad:
        dm = datamanager.DataManager(
            root= dataroot_dir,
            user_name = cfg.user_name,
            user_email = cfg.user_email,
            default_project = cfg.project,
            default_campaign = cfg.campaign,
            GIN_url = cfg.GIN_url,
            GIN_repo = cfg.GIN_repo,
            GIN_user = cfg.GIN_user,
            verbose=True
        )
        st.session_state['datamanager'] = dm
    else:
        st.session_state['datamanager'] = None


def ssh_config_block(host_alias: str, hostname: str, username: str) -> str:
    return (
        f"Host {host_alias}\n"
        f"    HostName {hostname}\n"
        f"    User {username}\n"
    )


def ssh_config_path() -> Path:
    # Standard location across macOS, Linux, and most Windows OpenSSH installs
    return Path.home() / ".ssh" / "config"


def ssh_config_has_entry(host_alias: str, hostname: str | None = None, username: str | None = None) -> tuple[bool, str]:
    """
    Checks whether an entry for the host already exists in ~/.ssh/config.
    Returns (found, message).
    """

    config_file = ssh_config_path()

    if not config_file.exists():
        return False, f"SSH config file does not exist yet: {config_file}"

    try:
        text = config_file.read_text(encoding="utf-8")
    except Exception as exc:
        return False, f"Could not read SSH config: {exc}"

    lines = text.splitlines()

    current_host = None
    host_blocks = {}

    for line in lines:
        stripped = line.strip()
        if stripped.lower().startswith("host "):
            current_host = stripped.split(maxsplit=1)[1]
            host_blocks[current_host] = []
        elif current_host:
            host_blocks[current_host].append(stripped)

    if host_alias in host_blocks:
        block = "\n".join(host_blocks[host_alias])
        if hostname and f"hostname {hostname}".lower() not in block.lower():
            return True, f"Host '{host_alias}' exists but different from hostname ({hostname})."
        if username and f"user {username}".lower() not in block.lower():
            return True, f"Host '{host_alias}' exists but user differs."
        return True, f"Host '{host_alias}' exists in {config_file}."

    return False, f"No SSH config entry for host '{host_alias}'."

def ssh_default_key_path(hostname: str, username: str) -> Path:
    safe_host = hostname.replace(".", "_").replace("/", "_")
    safe_user = username.replace(".", "_").replace("/", "_")
    return Path.home() / ".ssh" / f"id_ed25519_{safe_user}_{safe_host}"

def ssh_ensure_ssh_dir() -> Path:
    ssh_dir = Path.home() / ".ssh"
    ssh_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    try:
        os.chmod(ssh_dir, 0o700)
    except OSError:
        pass
    return ssh_dir

def ssh_generate_keypair(private_key_path: Path, comment: str = "") -> tuple[bool, str]:
    ssh_ensure_ssh_dir()

    if private_key_path.exists() or private_key_path.with_suffix(".pub").exists():
        return False, f"Key file already exists: {private_key_path}"

    cmd = [
        "ssh-keygen",
        "-t", "ed25519",
        "-f", str(private_key_path),
        "-N", "",
    ]
    if comment:
        cmd.extend(["-C", comment])

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    except FileNotFoundError:
        return False, "ssh-keygen was not found on this system."
    except subprocess.CalledProcessError as exc:
        stderr = (exc.stderr or "").strip()
        stdout = (exc.stdout or "").strip()
        message = stderr if stderr else stdout if stdout else str(exc)
        return False, f"ssh-keygen failed: {message}"

    try:
        os.chmod(private_key_path, 0o600)
    except OSError:
        pass

    output = (result.stdout or "").strip()
    return True, output if output else f"Created SSH key pair at {private_key_path}"

def ssh_test_connection(host_alias: str) -> tuple[bool, str, str]:
    """
    Test SSH connectivity using the configured host alias.
    Returns (success, summary_message, detailed_output).
    """
    cmd = [
        "ssh",
        "-T",
        "-o", "BatchMode=yes",
        host_alias,
    ]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
    except FileNotFoundError:
        return False, "ssh was not found on this system.", ""
    except subprocess.TimeoutExpired:
        return False, f"SSH connection test timed out for host '{host_alias}'.", ""
    except Exception as exc:
        return False, f"SSH connection test failed: {exc}", ""

    stdout = (result.stdout or "").strip()
    stderr = (result.stderr or "").strip()
    combined = "\n".join(part for part in [stdout, stderr] if part).strip()

    success_markers = [
        "successfully authenticated",
        "welcome to gin",
        "you've successfully authenticated",
    ]
    permission_markers = [
        "shell access is not supported",
        "pty allocation request failed",
    ]

    lowered = combined.lower()
    if any(marker in lowered for marker in success_markers) or any(marker in lowered for marker in permission_markers):
        return True, f"SSH connection to '{host_alias}' appears to work.", combined

    if result.returncode == 0:
        return True, f"SSH connection to '{host_alias}' succeeded.", combined

    return False, f"SSH connection to '{host_alias}' failed.", combined
