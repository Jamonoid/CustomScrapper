"""Configuración de la aplicación y carga de ajustes."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import yaml


@dataclass
class Settings:
    """Contiene los ajustes de ejecución cargados desde entorno y YAML."""

    database_dsn: str
    channels_config: Dict[str, Any]
    default_channel_config: Dict[str, Any]
    channel_specific_config: Dict[str, Any]


def load_yaml_config(path: Path) -> Dict[str, Any]:
    """Carga la configuración YAML para los canales."""

    with path.open("r", encoding="utf-8") as file:
        data: Dict[str, Any] = yaml.safe_load(file) or {}
    return data


def get_settings(config_path: Optional[Path] = None) -> Settings:
    """Lee variables de entorno y configuración YAML."""

    base_path = config_path or Path(__file__).resolve().parents[1] / "config" / "channels.yaml"
    config_data = load_yaml_config(base_path)

    default_channel_config: Dict[str, Any] = config_data.get("default", {})
    channels: Dict[str, Any] = config_data.get("channels", {})

    database_dsn = os.environ.get("DATABASE_DSN", "postgresql+psycopg2://user:pass@localhost:5432/pricemonitor")

    return Settings(
        database_dsn=database_dsn,
        channels_config=config_data,
        default_channel_config=default_channel_config,
        channel_specific_config=channels,
    )
