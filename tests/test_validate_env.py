from __future__ import annotations

from pathlib import Path

from validate_env import validate_environment


def test_validate_environment_reports_specific_bad_variables(monkeypatch) -> None:
    monkeypatch.setenv("DEEZER_ARL", "demo-cookie")
    monkeypatch.setenv("DEEZER_DOWNLOAD_DIR", "")
    monkeypatch.setenv("DEEZER_QUALITY", "ULTRA-HD")
    monkeypatch.setenv("DEEZER_MATCH_THRESHOLD", "101")
    monkeypatch.setenv("SYNC_MAX_TRACKS", "0")

    errors = validate_environment()

    assert any("DEEZER_DOWNLOAD_DIR" in error for error in errors)
    assert any("DEEZER_QUALITY" in error for error in errors)
    assert any("DEEZER_MATCH_THRESHOLD" in error for error in errors)
    assert any("SYNC_MAX_TRACKS" in error for error in errors)


def test_validate_environment_accepts_valid_core_settings(monkeypatch) -> None:
    monkeypatch.setenv("APP_PORT", "3000")
    monkeypatch.setenv("SYNC_MAX_TRACKS", "100")
    monkeypatch.setenv("DEEZER_ARL", "demo-cookie")
    monkeypatch.setenv("DEEZER_DOWNLOAD_DIR", "/tmp/downloads")
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
