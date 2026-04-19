from __future__ import annotations

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent


class Config:
    SECRET_KEY = os.getenv("SECRET_KEY", "dev-only-change-me")
    FLASK_ENV = os.getenv("FLASK_ENV", "development")
    APP_PORT = int(os.getenv("APP_PORT", "3000"))
    DATA_DIR = os.getenv("DATA_DIR", str(BASE_DIR / "data"))
    UPLOAD_FOLDER = str(Path(DATA_DIR) / "uploads")
    MAX_CONTENT_LENGTH = 16 * 1024 * 1024

    LISTENBRAINZ_API_BASE_URL = os.getenv(
        "LISTENBRAINZ_API_BASE_URL", "https://api.listenbrainz.org"
    ).rstrip("/")
    LISTENBRAINZ_USERNAME = os.getenv("LISTENBRAINZ_USERNAME", "")
    LISTENBRAINZ_AUTH_TOKEN = os.getenv("LISTENBRAINZ_AUTH_TOKEN", "")
    ACOUSTID_API_KEY = os.getenv("ACOUSTID_API_KEY", "")
    ACOUSTID_LOOKUP_TIMEOUT = float(os.getenv("ACOUSTID_LOOKUP_TIMEOUT", "20"))
    ACOUSTID_SCORE_THRESHOLD = float(os.getenv("ACOUSTID_SCORE_THRESHOLD", "0.9"))
    ACOUSTID_FINGERPRINT_LENGTH = int(os.getenv("ACOUSTID_FINGERPRINT_LENGTH", "120"))
    FPCALC_BIN = os.getenv("FPCALC_BIN", "")
    SYNC_MAX_TRACKS = int(os.getenv("SYNC_MAX_TRACKS", "100"))
    NAVIDROME_BASE_URL = os.getenv("NAVIDROME_BASE_URL", "").rstrip("/")
    NAVIDROME_PLAYLIST_DIR = (
        os.getenv("NAVIDROME_PLAYLIST_DIR")
        or os.getenv("NAVIDROME_PLAYLISTS_DIR")
        or "/navidrome/playlist"
    )
    NAVIDROME_PLAYLISTS_DIR = NAVIDROME_PLAYLIST_DIR
    NAVIDROME_MUSIC_ROOT = os.getenv("NAVIDROME_MUSIC_ROOT", "/navidrome/root")
    NAVIDROME_M3U_PATH_PREFIX = os.getenv("NAVIDROME_M3U_PATH_PREFIX", "..")
    DOWNLOAD_PATH_TEMPLATE = os.getenv(
        "DOWNLOAD_PATH_TEMPLATE",
        "{artist}/{album}/{artist} - {track} - {title}",
    )
    DEEZER_ARL = os.getenv("DEEZER_ARL", "")
    DEEZER_QUALITY = os.getenv("DEEZER_QUALITY", "FLAC").upper()
    DEEZER_MATCH_THRESHOLD = float(os.getenv("DEEZER_MATCH_THRESHOLD", "72"))
    SOUNDCLOUD_FALLBACK_ENABLED = os.getenv(
        "SOUNDCLOUD_FALLBACK",
        os.getenv("SOUNDCLOUD_FALLBACK_ENABLED", "1"),
    )
    SOUNDCLOUD_MATCH_THRESHOLD = float(
        os.getenv("SOUNDCLOUD_MATCH_THRESHOLD", str(DEEZER_MATCH_THRESHOLD))
    )
    SOUNDCLOUD_REQUEST_TIMEOUT = float(os.getenv("SOUNDCLOUD_REQUEST_TIMEOUT", "25"))
    SOUNDCLOUD_REQUEST_RETRIES = int(os.getenv("SOUNDCLOUD_REQUEST_RETRIES", "3"))
    SOUNDCLOUD_FORCE_IPV4 = os.getenv("SOUNDCLOUD_FORCE_IPV4", "1")
    YOUTUBE_FALLBACK_ENABLED = os.getenv(
        "YOUTUBE_FALLBACK",
        os.getenv("YOUTUBE_FALLBACK_ENABLED", "0"),
    )
    YOUTUBE_MATCH_THRESHOLD = float(
        os.getenv("YOUTUBE_MATCH_THRESHOLD", str(DEEZER_MATCH_THRESHOLD))
    )
    YOUTUBE_REQUEST_TIMEOUT = float(os.getenv("YOUTUBE_REQUEST_TIMEOUT", "25"))
    YOUTUBE_REQUEST_RETRIES = int(os.getenv("YOUTUBE_REQUEST_RETRIES", "3"))
    YOUTUBE_FORCE_IPV4 = os.getenv("YOUTUBE_FORCE_IPV4", "1")
    SETTINGS_FILE = os.getenv("SETTINGS_FILE", str(Path(DATA_DIR) / "settings.json"))
    PLAYLIST_DB_PATH = os.getenv(
        "PLAYLIST_DB_PATH", str(Path(DATA_DIR) / "playlist_history.db")
    )
    LIBRARY_INDEX_DB_PATH = os.getenv(
        "LIBRARY_INDEX_DB_PATH",
        str(Path(DATA_DIR) / "library_index.db"),
    )
    DOWNLOAD_THREADS = int(os.getenv("DOWNLOAD_THREADS", "1"))
