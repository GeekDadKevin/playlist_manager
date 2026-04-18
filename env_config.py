from __future__ import annotations

import json
import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any

CONFIG_JSON_ENABLE_ENV = "PLAYLIST_MANAGER_USE_CONFIG_JSON"

_TRUE_VALUES = {"1", "true", "yes", "on"}
_FALSE_VALUES = {"0", "false", "no", "off"}


def should_apply_json_config(
    values: Mapping[str, object] | None = None,
    *,
    dockerenv_path: str | Path = "/.dockerenv",
) -> bool:
    source = os.environ if values is None else values
    raw_value = str(source.get(CONFIG_JSON_ENABLE_ENV, "")).strip().lower()
    if raw_value in _TRUE_VALUES:
        return True
    if raw_value in _FALSE_VALUES:
        return False
    return Path(dockerenv_path).exists()


def read_json_config(config_path: str | Path) -> dict[str, Any] | None:
    path = Path(config_path)
    if not path.exists():
        return None

    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise TypeError("Config JSON must be an object of key/value pairs.")
    return raw


def coerce_json_config_values(raw: Mapping[str, object]) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for key, value in raw.items():
        if value is None:
            continue
        if isinstance(value, bool):
            normalized[str(key)] = "1" if value else "0"
        elif isinstance(value, (int, float, str)):
            normalized[str(key)] = str(value)
    return normalized
