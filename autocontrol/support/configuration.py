from __future__ import annotations

from datetime import datetime
import json
import os

from dataclasses import asdict, dataclass, fields, is_dataclass
from pathlib import Path
from typing import Optional

try:
    from platformdirs import user_config_dir
except ImportError:
    user_config_dir = None


@dataclass
class DataConfig:
    # Identity
    user_name: str = 'default'
    user_email: str = ''

    # Optional identity/context
    user_id: Optional[str] = None
    organization: Optional[str] = None
    lab_group: Optional[str] = None

    # Defaults
    project: Optional[str] = None
    campaign: Optional[str] = None
    experiment: Optional[str] = None

    # DataLad behavior
    use_datalad: bool = False
    datalad_profile: Optional[str] = None

    # GIN repository
    use_GIN: bool = False
    GIN_url: str = 'gin.g-node.org'
    GIN_user: str = 'fhein'
    SSH_host_alias: str = 'gin.g-node.org'

    # Datamanager root directory
    dm_root: Optional[str] = None

    # autocontrol-specific fields
    autocontrol_dir: Optional[str] = None
    atc_address: Optional[str] = None
    autocontrol_startup: bool = False

def default_config_path() -> Path:
    # env override
    override = os.getenv("AUTOCONTROL_APP_CONFIG")
    if override:
        return Path(override).expanduser()
    if user_config_dir is not None:
        return Path(user_config_dir("autocontrol_app", "streamlit")) / "config.json"
    # fallback
    return Path.home() / ".autocontrol_app_config" / "config.json"


def load_persistent_cfg() -> DataConfig:
    """Load config from disk and return a DataConfig instance.

    If no config file exists (or it cannot be parsed), returns a default DataConfig.
    Unknown keys in the JSON are ignored to allow schema evolution.
    """
    cfg_path = default_config_path()
    if not cfg_path.exists():
        return DataConfig()

    try:
        raw = json.loads(cfg_path.read_text())
    except (json.JSONDecodeError, OSError, NotADirectoryError):
        return DataConfig()

    if not isinstance(raw, dict):
        return DataConfig()

    valid_keys = {f.name for f in fields(DataConfig)}
    filtered = {k: v for k, v in raw.items() if k in valid_keys}
    return DataConfig(**filtered)


def save_persistent_cfg(data: dict | DataConfig) -> None:

    def _make_json_safe(obj):
        # Ensure all values are JSON-safe
        if isinstance(obj, Path):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, dict):
            return {k: _make_json_safe(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_make_json_safe(v) for v in obj]
        return obj

    cfg_path = default_config_path()
    cfg_path.parent.mkdir(parents=True, exist_ok=True)

    # Normalize input
    if is_dataclass(data):
        data = asdict(data)

    safe_data = _make_json_safe(data)

    cfg_path.write_text(json.dumps(safe_data, indent=2))
