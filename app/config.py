from __future__ import annotations

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent


class Config:
    SECRET_KEY = os.getenv("SECRET_KEY", "dev-only-change-me")
    FLASK_ENV = os.getenv("FLASK_ENV", "development")
    APP_PORT = int(os.getenv("APP_PORT", "3000"))
    DATA_DIR = os.getenv("DATA_DIR", str(BASE_DIR / "data"))
    UPLOAD_FOLDER = os.getenv("UPLOAD_FOLDER", str(Path(DATA_DIR) / "uploads"))
    MAX_CONTENT_LENGTH = 16 * 1024 * 1024

    LISTENBRAINZ_API_BASE_URL = os.getenv(
        "LISTENBRAINZ_API_BASE_URL", "https://api.listenbrainz.org"
    ).rstrip("/")
    LISTENBRAINZ_USERNAME = os.getenv("LISTENBRAINZ_USERNAME", "")
    LISTENBRAINZ_AUTH_TOKEN = os.getenv("LISTENBRAINZ_AUTH_TOKEN", "")
    LISTENBRAINZ_PLAYLIST_TYPE = (
        os.getenv("LISTENBRAINZ_PLAYLIST_TYPE", "createdfor").strip().lower()
    )
    LISTENBRAINZ_PLAYLIST_ID = os.getenv("LISTENBRAINZ_PLAYLIST_ID", "")
    LISTENBRAINZ_JSPF_URL = os.getenv("LISTENBRAINZ_JSPF_URL", "")
    OCTO_FIESTA_BASE_URL = os.getenv("OCTO_FIESTA_BASE_URL", "").rstrip("/")
    OCTO_FIESTA_HANDOFF_MODE = os.getenv("OCTO_FIESTA_HANDOFF_MODE", "preview")
    OCTO_FIESTA_USERNAME = os.getenv("OCTO_FIESTA_USERNAME", "")
    OCTO_FIESTA_PASSWORD = os.getenv("OCTO_FIESTA_PASSWORD", "")
    OCTO_FIESTA_TOKEN = os.getenv("OCTO_FIESTA_TOKEN", "")
    OCTO_FIESTA_SALT = os.getenv("OCTO_FIESTA_SALT", "")
    OCTO_FIESTA_CLIENT_NAME = os.getenv("OCTO_FIESTA_CLIENT_NAME", "jspf-converter")
    OCTO_FIESTA_API_VERSION = os.getenv("OCTO_FIESTA_API_VERSION", "1.16.1")
    OCTO_FIESTA_PROVIDER = os.getenv("OCTO_FIESTA_PROVIDER", "deezer").lower()
    OCTO_FIESTA_MATCH_THRESHOLD = float(os.getenv("OCTO_FIESTA_MATCH_THRESHOLD", "72"))
    SYNC_MAX_TRACKS = int(os.getenv("SYNC_MAX_TRACKS", "100"))
    NAVIDROME_BASE_URL = os.getenv("NAVIDROME_BASE_URL", "").rstrip("/")
    NAVIDROME_PLAYLISTS_DIR = os.getenv(
        "NAVIDROME_PLAYLISTS_DIR", str(Path(DATA_DIR) / "navidrome_playlists")
    )
    SETTINGS_FILE = os.getenv("SETTINGS_FILE", str(Path(DATA_DIR) / "settings.json"))
    PLAYLIST_DB_PATH = os.getenv("PLAYLIST_DB_PATH", str(Path(DATA_DIR) / "playlist_history.db"))
