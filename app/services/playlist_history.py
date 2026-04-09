from __future__ import annotations

import json
import re
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from werkzeug.utils import secure_filename

from app.services.listenbrainz import extract_listenbrainz_playlist_id
from app.services.navidrome_playlists import export_navidrome_playlist

_BRACKET_PREFIX_RE = re.compile(r"^\s*\[[^\]]+\]\s*")
_RECURRING_DATE_RE = re.compile(
    r",?\s*(?:week|day)\s+of\s+\d{4}-\d{2}-\d{2}(?:\s+[A-Za-z]{3})?",
    re.IGNORECASE,
)
_RECURRING_FOR_RE = re.compile(r"\s+for\s+[^,]+$", re.IGNORECASE)


def init_playlist_history(db_path: str | Path) -> None:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with _connect(path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS playlist_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                playlist_name TEXT NOT NULL,
                playlist_stem TEXT NOT NULL,
                source_kind TEXT NOT NULL,
                original_name TEXT NOT NULL,
                remote_url TEXT NOT NULL,
                saved_path TEXT NOT NULL,
                export_path TEXT NOT NULL,
                export_written INTEGER NOT NULL DEFAULT 0,
                requested_count INTEGER NOT NULL DEFAULT 0,
                processed_count INTEGER NOT NULL DEFAULT 0,
                downloaded_count INTEGER NOT NULL DEFAULT 0,
                already_available_count INTEGER NOT NULL DEFAULT 0,
                low_confidence_count INTEGER NOT NULL DEFAULT 0,
                not_found_count INTEGER NOT NULL DEFAULT 0,
                failed_count INTEGER NOT NULL DEFAULT 0,
                playable_count INTEGER NOT NULL DEFAULT 0,
                missing_count INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                raw_result_json TEXT NOT NULL DEFAULT '{}'
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS playlist_tracks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL,
                track_index INTEGER NOT NULL DEFAULT 0,
                title TEXT NOT NULL,
                artist TEXT NOT NULL,
                album TEXT NOT NULL,
                duration_seconds INTEGER,
                source TEXT NOT NULL,
                status TEXT NOT NULL,
                match_id TEXT NOT NULL,
                provider TEXT NOT NULL,
                score REAL,
                local_path TEXT NOT NULL,
                deezer_id TEXT NOT NULL,
                raw_track_json TEXT NOT NULL DEFAULT '{}',
                FOREIGN KEY(run_id) REFERENCES playlist_runs(id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_playlist_runs_stem ON playlist_runs(playlist_stem)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_playlist_tracks_run_id ON playlist_tracks(run_id)"
        )
        conn.commit()


def record_playlist_run(
    db_path: str | Path,
    *,
    playlist_name: str,
    source_kind: str,
    original_name: str,
    remote_url: str = "",
    saved_path: str = "",
    sync_result: dict[str, Any] | None = None,
    export_result: dict[str, Any] | None = None,
) -> int:
    init_playlist_history(db_path)

    payload = sync_result or {}
    summary = payload.get("summary", {}) if isinstance(payload, dict) else {}
    results = payload.get("results", []) if isinstance(payload, dict) else []
    if not isinstance(results, list):
        results = []
    summary = _derive_summary(summary, results)

    export = export_result or {}
    export_path = str(export.get("target_path", "") or export.get("filename", "")).strip()
    playlist_stem = Path(export_path).stem or _build_playlist_stem(playlist_name)
    created_at = str(
        payload.get("completed_at") or payload.get("started_at") or datetime.now(UTC).isoformat()
    )

    with _connect(db_path) as conn:
        cursor = conn.execute(
            """
            INSERT INTO playlist_runs (
                playlist_name,
                playlist_stem,
                source_kind,
                original_name,
                remote_url,
                saved_path,
                export_path,
                export_written,
                requested_count,
                processed_count,
                downloaded_count,
                already_available_count,
                low_confidence_count,
                not_found_count,
                failed_count,
                playable_count,
                missing_count,
                created_at,
                raw_result_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                playlist_name,
                playlist_stem,
                source_kind,
                original_name,
                remote_url,
                saved_path,
                export_path,
                int(bool(export.get("written"))),
                int(summary.get("requested", len(results) or export.get("entry_count", 0) or 0)),
                int(summary.get("processed", len(results))),
                int(summary.get("downloaded", 0)),
                int(summary.get("already_available", 0)),
                int(summary.get("low_confidence", 0)),
                int(summary.get("not_found", 0)),
                int(summary.get("failed", 0)),
                int(export.get("playable_count", 0)),
                int(export.get("missing_count", 0)),
                created_at,
                json.dumps(payload, ensure_ascii=False),
            ),
        )
        run_id = int(cursor.lastrowid or 0)
        if run_id <= 0:
            raise ValueError("Could not record playlist history.")

        for position, item in enumerate(results, start=1):
            track = item.get("track") if isinstance(item, dict) else {}
            match = item.get("match") if isinstance(item, dict) else {}
            resolved_match = item.get("resolved_match") if isinstance(item, dict) else {}

            track_title = _text_value(track.get("title")) if isinstance(track, dict) else ""
            track_artist = _text_value(track.get("artist")) if isinstance(track, dict) else ""
            track_album = _text_value(track.get("album")) if isinstance(track, dict) else ""
            track_source = _text_value(track.get("source")) if isinstance(track, dict) else ""
            match_id = _text_value(match.get("id")) if isinstance(match, dict) else ""
            provider = _text_value(match.get("provider")) if isinstance(match, dict) else ""
            deezer_id = match_id if match_id.startswith("ext-deezer-") else ""
            score = match.get("score") if isinstance(match, dict) else None
            status = _text_value(item.get("status")) if isinstance(item, dict) else ""
            local_path = _first_non_empty(
                _text_value(resolved_match.get("path")) if isinstance(resolved_match, dict) else "",
                _text_value(match.get("path")) if isinstance(match, dict) else "",
            )
            if not local_path and status in {"already_available", "downloaded"}:
                local_path = _find_previous_local_path(
                    conn,
                    title=track_title,
                    artist=track_artist,
                    album=track_album,
                )
            if not status:
                status = "already_available" if local_path else "not_found"

            conn.execute(
                """
                INSERT INTO playlist_tracks (
                    run_id,
                    track_index,
                    title,
                    artist,
                    album,
                    duration_seconds,
                    source,
                    status,
                    match_id,
                    provider,
                    score,
                    local_path,
                    deezer_id,
                    raw_track_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    position,
                    track_title,
                    track_artist,
                    track_album,
                    _int_value(track.get("duration_seconds")) if isinstance(track, dict) else None,
                    track_source,
                    status,
                    match_id,
                    provider,
                    float(score) if isinstance(score, int | float) else None,
                    local_path,
                    deezer_id,
                    json.dumps(item, ensure_ascii=False),
                ),
            )

        conn.commit()

    return run_id


def list_tracked_playlists(db_path: str | Path, limit: int = 25) -> list[dict[str, Any]]:
    init_playlist_history(db_path)

    with _connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT pr.*, grouped.run_count
            FROM playlist_runs pr
            JOIN (
                SELECT playlist_stem, MAX(id) AS latest_id, COUNT(*) AS run_count
                FROM playlist_runs
                GROUP BY playlist_stem
            ) AS grouped
              ON pr.id = grouped.latest_id
            ORDER BY pr.created_at DESC, pr.id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    playlists: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["run_id"] = item.pop("id")
        item["export_written"] = bool(item.get("export_written"))
        item["filename"] = f"{item['playlist_stem']}.m3u"
        playlists.append(item)
    return playlists


def find_recorded_listenbrainz_playlist_ids(db_path: str | Path) -> set[str]:
    init_playlist_history(db_path)

    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT remote_url FROM playlist_runs WHERE TRIM(remote_url) != ''"
        ).fetchall()

    playlist_ids: set[str] = set()
    for row in rows:
        playlist_id = extract_listenbrainz_playlist_id(str(row["remote_url"]))
        if playlist_id:
            playlist_ids.add(playlist_id.lower())
    return playlist_ids


def get_playlist_stats(db_path: str | Path) -> dict[str, Any]:
    init_playlist_history(db_path)

    with _connect(db_path) as conn:
        summary_row = conn.execute(
            """
            SELECT
                COUNT(*) AS total_runs,
                COUNT(DISTINCT playlist_stem) AS total_playlists,
                COALESCE(SUM(requested_count), 0) AS total_tracks,
                COALESCE(SUM(downloaded_count), 0) AS total_downloaded,
                COALESCE(SUM(already_available_count), 0) AS total_already_available,
                COALESCE(SUM(failed_count), 0) AS total_failed,
                COALESCE(SUM(missing_count), 0) AS total_missing
            FROM playlist_runs
            """
        ).fetchone()
        recent_tracks = conn.execute(
            """
            SELECT
                pt.title,
                pt.artist,
                pt.album,
                pt.status,
                pt.deezer_id,
                pt.local_path,
                pr.playlist_name,
                pr.created_at
            FROM playlist_tracks pt
            JOIN playlist_runs pr ON pr.id = pt.run_id
            ORDER BY pr.created_at DESC, pt.track_index ASC
            LIMIT 20
            """
        ).fetchall()
        top_artists = conn.execute(
            """
            SELECT artist, COUNT(*) AS track_count
            FROM playlist_tracks
            WHERE TRIM(artist) != ''
            GROUP BY artist
            ORDER BY track_count DESC, artist ASC
            LIMIT 8
            """
        ).fetchall()

    summary = dict(summary_row) if summary_row is not None else {}
    summary.setdefault("total_runs", 0)
    summary.setdefault("total_playlists", 0)
    summary.setdefault("total_tracks", 0)
    summary.setdefault("total_downloaded", 0)
    summary.setdefault("total_already_available", 0)
    summary.setdefault("total_failed", 0)
    summary.setdefault("total_missing", 0)
    summary["completion_percent"] = _completion_percent(summary)

    return {
        "summary": summary,
        "tracked_playlists": list_tracked_playlists(db_path),
        "recent_tracks": [dict(row) for row in recent_tracks],
        "top_artists": [dict(row) for row in top_artists],
    }


def export_playlist_from_history(
    db_path: str | Path,
    *,
    run_id: int,
    playlist_dir: str | Path,
) -> dict[str, Any]:
    init_playlist_history(db_path)

    with _connect(db_path) as conn:
        run_row = conn.execute(
            "SELECT * FROM playlist_runs WHERE id = ?",
            (run_id,),
        ).fetchone()
        if run_row is None:
            raise ValueError("That saved playlist history entry could not be found.")

        track_rows = conn.execute(
            "SELECT * FROM playlist_tracks WHERE run_id = ? ORDER BY track_index ASC, id ASC",
            (run_id,),
        ).fetchall()

    if not track_rows:
        raise ValueError("No track history was stored for that playlist.")

    sync_results: list[dict[str, Any]] = []
    for row in track_rows:
        item = {
            "track": {
                "title": row["title"],
                "artist": row["artist"],
                "album": row["album"],
                "duration_seconds": row["duration_seconds"],
                "source": row["source"],
            },
            "match": {},
            "resolved_match": {},
        }
        if row["match_id"]:
            item["match"]["id"] = row["match_id"]
        if row["provider"]:
            item["match"]["provider"] = row["provider"]
        if row["score"] is not None:
            item["match"]["score"] = row["score"]
        if row["local_path"]:
            item["resolved_match"]["path"] = row["local_path"]
        sync_results.append(item)

    return export_navidrome_playlist(
        playlist_dir=playlist_dir,
        playlist_name=run_row["playlist_name"],
        sync_results=sync_results,
    )


def _connect(db_path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(Path(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _build_playlist_stem(playlist_name: str) -> str:
    cleaned_name = _BRACKET_PREFIX_RE.sub("", playlist_name).strip() or "playlist"
    lowered = cleaned_name.lower()
    is_recurring = any(marker in lowered for marker in ("daily", "weekly", "day of", "week of"))

    if is_recurring:
        cleaned_name = _RECURRING_DATE_RE.sub("", cleaned_name).strip(" ,-_")
        cleaned_name = _RECURRING_FOR_RE.sub("", cleaned_name).strip(" ,-_")
        return _safe_recurring_filename(cleaned_name)

    return secure_filename(cleaned_name).replace("_", "-").strip(".-").lower() or "playlist"


def _safe_recurring_filename(value: str) -> str:
    sanitized = re.sub(r'[<>:"/\\|?*]+', ' ', value)
    sanitized = re.sub(r'\s+', ' ', sanitized).strip(' .-_')
    if sanitized and sanitized == sanitized.lower():
        sanitized = sanitized.title()
    return sanitized or "Playlist"


def _find_previous_local_path(
    conn: sqlite3.Connection,
    *,
    title: str,
    artist: str,
    album: str,
) -> str:
    normalized_title = title.casefold().strip()
    normalized_artist = artist.casefold().strip()
    normalized_album = album.casefold().strip()
    if not normalized_title or not normalized_artist:
        return ""

    row = conn.execute(
        """
        SELECT local_path
        FROM playlist_tracks
        WHERE TRIM(local_path) != ''
          AND LOWER(TRIM(title)) = ?
          AND LOWER(TRIM(artist)) = ?
          AND (? = '' OR LOWER(TRIM(album)) = ? OR TRIM(album) = '')
        ORDER BY id DESC
        LIMIT 1
        """,
        (normalized_title, normalized_artist, normalized_album, normalized_album),
    ).fetchone()
    return _text_value(row["local_path"]) if row is not None else ""


def _completion_percent(summary: dict[str, Any]) -> int:
    total = int(summary.get("total_tracks", 0))
    if total <= 0:
        return 0
    completed = int(summary.get("total_downloaded", 0)) + int(
        summary.get("total_already_available", 0)
    )
    return int((completed / total) * 100)


def _derive_summary(summary: dict[str, Any], results: list[dict[str, Any]]) -> dict[str, Any]:
    derived = {
        "requested": int(summary.get("requested", len(results))),
        "processed": int(summary.get("processed", len(results))),
        "downloaded": int(summary.get("downloaded", 0)),
        "already_available": int(summary.get("already_available", 0)),
        "low_confidence": int(summary.get("low_confidence", 0)),
        "not_found": int(summary.get("not_found", 0)),
        "failed": int(summary.get("failed", 0)),
    }
    if any(value for key, value in derived.items() if key not in {"requested", "processed"}):
        return derived

    for item in results:
        if not isinstance(item, dict):
            continue
        status = _text_value(item.get("status"))
        if not status:
            track = item.get("track", {})
            resolved_match = item.get("resolved_match", {})
            match = item.get("match", {})
            source = _text_value(track.get("source")) if isinstance(track, dict) else ""
            local_path = _first_non_empty(
                _text_value(resolved_match.get("path")) if isinstance(resolved_match, dict) else "",
                _text_value(match.get("path")) if isinstance(match, dict) else "",
                source if _looks_like_local_media(source) else "",
            )
            status = "already_available" if local_path else "not_found"
        if status in derived:
            derived[status] += 1

    return derived


def _text_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, int | float):
        return str(value)
    return ""


def _int_value(value: Any) -> int | None:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    return None


def _first_non_empty(*values: str) -> str:
    for value in values:
        if value:
            return value
    return ""


def _looks_like_local_media(value: str) -> bool:
    normalized = value.replace("\\", "/").strip().lower()
    if not normalized or normalized.startswith("http://") or normalized.startswith("https://"):
        return False
    return any(normalized.endswith(ext) for ext in (".mp3", ".flac", ".ogg", ".m4a", ".wav"))
