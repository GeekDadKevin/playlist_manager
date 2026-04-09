from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

ALLOWED_THEMES = {"dark", "light", "system"}
DAY_NAMES = (
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
    "sunday",
)
_DAY_TO_CRON = {
    "sunday": 0,
    "monday": 1,
    "tuesday": 2,
    "wednesday": 3,
    "thursday": 4,
    "friday": 5,
    "saturday": 6,
}


def default_settings() -> dict[str, Any]:
    return {
        "theme": "dark",
        "automation_enabled": False,
        "schedule_day": "monday",
        "schedule_time": "06:00",
        "playlist_targets": ["weekly exploration", "weekly jams"],
        "sync_with_downloads": False,
        "last_run_at": "",
        "last_run_status": "never",
        "last_run_message": "Not run yet.",
        "last_run_key": "",
        "last_run_results": [],
    }


def load_settings(path: str | Path) -> dict[str, Any]:
    settings_path = Path(path)
    defaults = default_settings()

    try:
        raw = json.loads(settings_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return defaults

    if not isinstance(raw, dict):
        return defaults

    return normalize_settings({**defaults, **raw})


def save_settings(path: str | Path, settings: dict[str, Any]) -> dict[str, Any]:
    settings_path = Path(path)
    settings_path.parent.mkdir(parents=True, exist_ok=True)

    normalized = normalize_settings(settings)
    settings_path.write_text(json.dumps(normalized, indent=2), encoding="utf-8")
    return normalized


def normalize_settings(raw: dict[str, Any]) -> dict[str, Any]:
    settings = default_settings()

    theme = str(raw.get("theme", settings["theme"])).strip().lower()
    settings["theme"] = theme if theme in ALLOWED_THEMES else settings["theme"]

    schedule_day = str(raw.get("schedule_day", settings["schedule_day"])).strip().lower()
    settings["schedule_day"] = (
        schedule_day if schedule_day in DAY_NAMES else settings["schedule_day"]
    )

    settings["schedule_time"] = _normalize_time(
        str(raw.get("schedule_time", settings["schedule_time"])).strip()
    )
    settings["automation_enabled"] = _bool_value(
        raw.get("automation_enabled", settings["automation_enabled"])
    )
    settings["sync_with_downloads"] = _bool_value(
        raw.get("sync_with_downloads", settings["sync_with_downloads"])
    )
    settings["playlist_targets"] = _normalize_targets(raw.get("playlist_targets"))

    settings["last_run_at"] = str(raw.get("last_run_at", "")).strip()
    settings["last_run_status"] = str(raw.get("last_run_status", "never")).strip() or "never"
    settings["last_run_message"] = (
        str(raw.get("last_run_message", "Not run yet.")).strip() or "Not run yet."
    )
    settings["last_run_key"] = str(raw.get("last_run_key", "")).strip()

    raw_results = raw.get("last_run_results", [])
    settings["last_run_results"] = raw_results if isinstance(raw_results, list) else []

    return settings


def cron_expression(settings: dict[str, Any]) -> str:
    hour, minute = _parse_time(settings.get("schedule_time", "06:00"))
    cron_day = _DAY_TO_CRON[str(settings.get("schedule_day", "monday"))]
    return f"{minute} {hour} * * {cron_day}"


def matches_playlist_target(title: str, playlist_targets: list[str] | tuple[str, ...]) -> bool:
    normalized_title = str(title).strip().lower()
    return any(term and term in normalized_title for term in playlist_targets)


def current_schedule_key(settings: dict[str, Any], now: datetime | None = None) -> str:
    current_time = now or datetime.now()
    iso_year, iso_week, _ = current_time.isocalendar()
    schedule_day = str(settings.get("schedule_day", "monday"))
    schedule_time = str(settings.get("schedule_time", "06:00"))
    return f"{iso_year}-W{iso_week}:{schedule_day}:{schedule_time}"


def should_run_now(settings: dict[str, Any], now: datetime | None = None) -> bool:
    if not bool(settings.get("automation_enabled")):
        return False

    current_time = now or datetime.now()
    schedule_day = str(settings.get("schedule_day", "monday"))
    if DAY_NAMES.index(schedule_day) != current_time.weekday():
        return False

    target_hour, target_minute = _parse_time(str(settings.get("schedule_time", "06:00")))
    scheduled_minutes = (target_hour * 60) + target_minute
    current_minutes = (current_time.hour * 60) + current_time.minute
    if current_minutes < scheduled_minutes:
        return False

    return str(settings.get("last_run_key", "")).strip() != current_schedule_key(
        settings, current_time
    )


def record_run_result(
    path: str | Path,
    *,
    status: str,
    message: str,
    run_key: str,
    results: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    settings = load_settings(path)
    settings["last_run_at"] = datetime.now().isoformat(timespec="seconds")
    settings["last_run_status"] = status
    settings["last_run_message"] = message
    settings["last_run_key"] = run_key
    settings["last_run_results"] = list(results or [])
    return save_settings(path, settings)


def _normalize_targets(value: Any) -> list[str]:
    if isinstance(value, str):
        raw_items = value.replace("\n", ",").split(",")
    elif isinstance(value, list):
        raw_items = [str(item) for item in value]
    else:
        raw_items = []

    targets = [item.strip().lower() for item in raw_items if str(item).strip()]
    return targets or default_settings()["playlist_targets"]


def _normalize_time(value: str) -> str:
    hour, minute = _parse_time(value)
    return f"{hour:02d}:{minute:02d}"


def _parse_time(value: str) -> tuple[int, int]:
    try:
        hour_text, minute_text = value.split(":", 1)
        hour = max(0, min(23, int(hour_text)))
        minute = max(0, min(59, int(minute_text)))
    except (ValueError, AttributeError):
        return 6, 0

    return hour, minute


def _bool_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}
