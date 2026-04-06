from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, ClassVar, Any

from roadmap_datamanager.configuration import BaseConfig, load_config, save_config

@dataclass
class DataManagerConfig(BaseConfig):
    CONFIG_ENV_VAR: ClassVar[str] = "AUTOCONTROL_APP_CONFIG"
    CONFIG_APP_NAME: ClassVar[str] = "autocontrol_app"
    CONFIG_APP_AUTHOR: ClassVar[str] = "streamlit"
    CONFIG_FILENAME: ClassVar[str] = "config.json"

    # autocontrol-specific fields
    autocontrol_dir: Optional[str] = None
    atc_address: Optional[str] = None
    autocontrol_startup: bool = False

def load_persistent_cfg() -> DataManagerConfig:
    config_cls = DataManagerConfig
    return load_config(
        config_cls,
        env_var=getattr(config_cls, "CONFIG_ENV_VAR", None),
        app_name=config_cls.CONFIG_APP_NAME,
        app_author=config_cls.CONFIG_APP_AUTHOR,
        filename=getattr(config_cls, "CONFIG_FILENAME", "config.json"),
    )

def save_persistent_cfg(data: Any) -> Path:
    cls = type(data)
    return save_config(
        data,
        env_var=getattr(cls, "CONFIG_ENV_VAR", None),
        app_name=cls.CONFIG_APP_NAME,
        app_author=cls.CONFIG_APP_AUTHOR,
        filename=getattr(cls, "CONFIG_FILENAME", "config.json"),
    )

