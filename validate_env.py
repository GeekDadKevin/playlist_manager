from __future__ import annotations

import os
import sys
from collections.abc import Mapping
from urllib.parse import urlparse

_ALLOWED_FLASK_ENVS = {"development", "production", "testing"}
_ALLOWED_DEEZER_QUALITIES = {"FLAC", "MP3_320", "MP3_128"}


def validate_environment(env: Mapping[str, object] | None = None) -> list[str]:
    values = os.environ if env is None else env
    errors: list[str] = []

    flask_env = _get_value(values, "FLASK_ENV", "development").lower()
    if flask_env not in _ALLOWED_FLASK_ENVS:
        errors.append("FLASK_ENV must be one of: development, production, testing.")

    secret_key = _get_value(values, "SECRET_KEY", "dev-only-change-me")
    if flask_env == "production" and secret_key in {"change-me", "dev-only-change-me", ""}:
        errors.append("SECRET_KEY must be set to a non-default value when FLASK_ENV=production.")

    _validate_int(values, "APP_PORT", errors, default="3000", minimum=1, maximum=65535)
    _validate_int(values, "SYNC_MAX_TRACKS", errors, default="100", minimum=1)

    _validate_float(
        values,
        "DEEZER_MATCH_THRESHOLD",
        errors,
        default="72",
        minimum=0.0,
        maximum=100.0,
    )

    for name, default in (
        ("DATA_DIR", "/app/data"),
        ("NAVIDROME_PLAYLISTS_DIR", "/app/data/navidrome_playlists"),
        ("DEEZER_DOWNLOAD_DIR", "/app/downloads"),
    ):
        if not _get_value(values, name, default):
            errors.append(f"{name} cannot be empty.")

    for name in (
        "LISTENBRAINZ_API_BASE_URL",
        "NAVIDROME_BASE_URL",
    ):
        value = _get_value(values, name)
        if value and not _is_http_url(value):
            errors.append(f"{name} must be a valid http:// or https:// URL.")

    deezer_quality = _get_value(values, "DEEZER_QUALITY", "FLAC").upper()
    if deezer_quality not in _ALLOWED_DEEZER_QUALITIES:
        allowed = ", ".join(sorted(_ALLOWED_DEEZER_QUALITIES))
        errors.append(f"DEEZER_QUALITY must be one of: {allowed}.")

    deezer_arl = _get_value(values, "DEEZER_ARL")
    deezer_download_dir = _get_value(values, "DEEZER_DOWNLOAD_DIR", "/app/downloads")
    if deezer_arl and not deezer_download_dir:
        errors.append("DEEZER_DOWNLOAD_DIR is required when DEEZER_ARL is set.")

    return errors


def main() -> int:
    errors = validate_environment()
    if errors:
        print("Environment validation failed:", file=sys.stderr)
        for error in errors:
            print(f" - {error}", file=sys.stderr)
        return 1

    print("Environment validation passed.")
    return 0


def _get_value(values: Mapping[str, object], name: str, default: str = "") -> str:
    return str(values.get(name, default)).strip()


def _is_http_url(value: str) -> bool:
    normalized = value.strip()
    if normalized.startswith("listenbrainz.org/"):
        normalized = f"https://{normalized}"
    parsed = urlparse(normalized)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _validate_int(
    values: Mapping[str, object],
    name: str,
    errors: list[str],
    default: str = "",
    minimum: int | None = None,
    maximum: int | None = None,
) -> None:
    raw_value = _get_value(values, name, default) or default
    try:
        parsed = int(raw_value)
    except ValueError:
        errors.append(f"{name} must be an integer, got {raw_value!r}.")
        return

    if minimum is not None and parsed < minimum:
        errors.append(f"{name} must be >= {minimum}.")
    if maximum is not None and parsed > maximum:
        errors.append(f"{name} must be <= {maximum}.")


def _validate_float(
    values: Mapping[str, object],
    name: str,
    errors: list[str],
    default: str = "",
    minimum: float | None = None,
    maximum: float | None = None,
) -> None:
    raw_value = _get_value(values, name, default) or default
    try:
        parsed = float(raw_value)
    except ValueError:
        errors.append(f"{name} must be a number, got {raw_value!r}.")
        return

    if minimum is not None and parsed < minimum:
        errors.append(f"{name} must be >= {minimum}.")
    if maximum is not None and parsed > maximum:
        errors.append(f"{name} must be <= {maximum}.")


if __name__ == "__main__":
    raise SystemExit(main())
