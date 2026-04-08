from __future__ import annotations

import os
import re
import sys
from collections.abc import Mapping
from urllib.parse import urlparse

_ALLOWED_FLASK_ENVS = {"development", "production", "testing"}
_ALLOWED_HANDOFF_MODES = {"preview", "download"}
_ALLOWED_PLAYLIST_TYPES = {
    "createdfor",
    "created-for-you",
    "created_for_you",
    "user",
    "playlists",
}
_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)
_VERSION_RE = re.compile(r"^\d+(?:\.\d+)+$")


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
        "OCTO_FIESTA_MATCH_THRESHOLD",
        errors,
        default="72",
        minimum=0.0,
        maximum=100.0,
    )

    for name, default in (
        ("DATA_DIR", "/app/data"),
        ("UPLOAD_FOLDER", "/app/data/uploads"),
        ("NAVIDROME_PLAYLISTS_DIR", "/app/data/navidrome_playlists"),
    ):
        if not _get_value(values, name, default):
            errors.append(f"{name} cannot be empty.")

    for name in (
        "LISTENBRAINZ_API_BASE_URL",
        "LISTENBRAINZ_JSPF_URL",
        "OCTO_FIESTA_BASE_URL",
        "NAVIDROME_BASE_URL",
    ):
        value = _get_value(values, name)
        if value and not _is_http_url(value):
            errors.append(f"{name} must be a valid http:// or https:// URL.")

    playlist_type = _get_value(values, "LISTENBRAINZ_PLAYLIST_TYPE", "createdfor").lower()
    if playlist_type not in _ALLOWED_PLAYLIST_TYPES:
        errors.append("LISTENBRAINZ_PLAYLIST_TYPE must be one of: createdfor, user, playlists.")

    playlist_id = _get_value(values, "LISTENBRAINZ_PLAYLIST_ID")
    if playlist_id and not (_UUID_RE.fullmatch(playlist_id) or "/playlist/" in playlist_id):
        errors.append(
            "LISTENBRAINZ_PLAYLIST_ID must be a playlist UUID or a ListenBrainz playlist URL."
        )

    handoff_mode = _get_value(values, "OCTO_FIESTA_HANDOFF_MODE", "preview").lower()
    if handoff_mode not in _ALLOWED_HANDOFF_MODES:
        errors.append("OCTO_FIESTA_HANDOFF_MODE must be either 'preview' or 'download'.")

    api_version = _get_value(values, "OCTO_FIESTA_API_VERSION", "1.16.1")
    if api_version and not _VERSION_RE.fullmatch(api_version):
        errors.append("OCTO_FIESTA_API_VERSION must look like a version string such as 1.16.1.")

    octo_base_url = _get_value(values, "OCTO_FIESTA_BASE_URL")
    octo_username = _get_value(values, "OCTO_FIESTA_USERNAME")
    octo_password = _get_value(values, "OCTO_FIESTA_PASSWORD")
    octo_token = _get_value(values, "OCTO_FIESTA_TOKEN")
    octo_salt = _get_value(values, "OCTO_FIESTA_SALT")

    octo_configured = any([octo_base_url, octo_username, octo_password, octo_token, octo_salt])
    if octo_configured:
        if not octo_base_url:
            errors.append(
                "OCTO_FIESTA_BASE_URL is required when Octo-Fiesta integration is configured."
            )
        if not octo_username:
            errors.append(
                "OCTO_FIESTA_USERNAME is required when Octo-Fiesta integration is configured."
            )
        if octo_token and not octo_salt:
            errors.append("OCTO_FIESTA_SALT is required when OCTO_FIESTA_TOKEN is set.")
        if octo_salt and not octo_token:
            errors.append("OCTO_FIESTA_TOKEN is required when OCTO_FIESTA_SALT is set.")
        if not octo_password and not (octo_token and octo_salt):
            errors.append(
                "Set OCTO_FIESTA_PASSWORD, or both OCTO_FIESTA_TOKEN and OCTO_FIESTA_SALT."
            )

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
