from __future__ import annotations

from pathlib import Path

from validate_env import validate_environment


def test_validate_environment_reports_specific_bad_variables(monkeypatch) -> None:
    monkeypatch.setenv("OCTO_FIESTA_BASE_URL", "not-a-url")
    monkeypatch.setenv("OCTO_FIESTA_HANDOFF_MODE", "explode")
    monkeypatch.setenv("OCTO_FIESTA_USERNAME", "demo")
    monkeypatch.delenv("OCTO_FIESTA_PASSWORD", raising=False)
    monkeypatch.setenv("OCTO_FIESTA_TOKEN", "token-only")
    monkeypatch.delenv("OCTO_FIESTA_SALT", raising=False)
    monkeypatch.setenv("SYNC_MAX_TRACKS", "0")

    errors = validate_environment()

    assert any("OCTO_FIESTA_BASE_URL" in error for error in errors)
    assert any("OCTO_FIESTA_HANDOFF_MODE" in error for error in errors)
    assert any("OCTO_FIESTA_SALT" in error for error in errors)
    assert any("SYNC_MAX_TRACKS" in error for error in errors)


def test_validate_environment_accepts_valid_core_settings(monkeypatch) -> None:
    monkeypatch.setenv("APP_PORT", "3000")
    monkeypatch.setenv("SYNC_MAX_TRACKS", "100")
    monkeypatch.setenv("OCTO_FIESTA_BASE_URL", "http://example.com:5274")
    monkeypatch.setenv("OCTO_FIESTA_HANDOFF_MODE", "download")
    monkeypatch.setenv("OCTO_FIESTA_USERNAME", "demo")
    monkeypatch.setenv("OCTO_FIESTA_PASSWORD", "secret")
    monkeypatch.delenv("OCTO_FIESTA_TOKEN", raising=False)
    monkeypatch.delenv("OCTO_FIESTA_SALT", raising=False)
    monkeypatch.setenv("LISTENBRAINZ_API_BASE_URL", "https://api.listenbrainz.org")
    monkeypatch.setenv("LISTENBRAINZ_PLAYLIST_TYPE", "createdfor")

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
