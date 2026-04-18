from __future__ import annotations

import os
from pathlib import Path

from validate_env import _apply_config_json, validate_environment


def test_validate_environment_reports_specific_bad_variables(monkeypatch) -> None:
    monkeypatch.setenv("DEEZER_ARL", "demo-cookie")
    monkeypatch.setenv("NAVIDROME_MUSIC_ROOT", "")
    monkeypatch.setenv("DEEZER_QUALITY", "ULTRA-HD")
    monkeypatch.setenv("DEEZER_MATCH_THRESHOLD", "101")
    monkeypatch.setenv("SYNC_MAX_TRACKS", "0")

    errors = validate_environment()

    assert any("NAVIDROME_MUSIC_ROOT" in error for error in errors)
    assert any("DEEZER_QUALITY" in error for error in errors)
    assert any("DEEZER_MATCH_THRESHOLD" in error for error in errors)
    assert any("SYNC_MAX_TRACKS" in error for error in errors)


def test_validate_environment_accepts_valid_core_settings(monkeypatch) -> None:
    monkeypatch.setenv("APP_PORT", "3000")
    monkeypatch.setenv("SYNC_MAX_TRACKS", "100")
    monkeypatch.setenv("DEEZER_ARL", "demo-cookie")
    monkeypatch.setenv("NAVIDROME_MUSIC_ROOT", "/tmp/downloads")
    monkeypatch.setenv("DEEZER_QUALITY", "FLAC")
    monkeypatch.setenv("DEEZER_MATCH_THRESHOLD", "72")
    monkeypatch.setenv("LISTENBRAINZ_API_BASE_URL", "https://api.listenbrainz.org")

    assert validate_environment() == []


def test_repo_env_file_passes_validation() -> None:
    env_path = Path(__file__).resolve().parent.parent / ".env"
    values: dict[str, str] = {}

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()

    assert validate_environment(values) == []


def test_apply_config_json_skips_defaults_for_local_runs(tmp_path, monkeypatch) -> None:
    config_path = tmp_path / "config.json"
    config_path.write_text('{"DATA_DIR": "/app/data", "APP_PORT": 3000}', encoding="utf-8")
    monkeypatch.delenv("PLAYLIST_MANAGER_USE_CONFIG_JSON", raising=False)

    merged = _apply_config_json({}, config_path=config_path, dockerenv_path=tmp_path / ".dockerenv")

    assert "DATA_DIR" not in merged
    assert "APP_PORT" not in merged


def test_apply_config_json_respects_explicit_opt_in(tmp_path, monkeypatch) -> None:
    config_path = tmp_path / "config.json"
    config_path.write_text('{"DATA_DIR": "/app/data", "APP_PORT": 3000}', encoding="utf-8")
    monkeypatch.setenv("PLAYLIST_MANAGER_USE_CONFIG_JSON", "1")

    merged = _apply_config_json(
        os.environ,
        config_path=config_path,
        dockerenv_path=tmp_path / ".dockerenv",
    )

    assert merged["DATA_DIR"] == "/app/data"
    assert merged["APP_PORT"] == "3000"
