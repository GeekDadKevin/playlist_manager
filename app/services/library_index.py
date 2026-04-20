
from __future__ import annotations

# Utility: List all unique album directories from the DB for a given root
def list_album_dirs_from_db(db_path: str | Path, root: str | Path) -> list[Path]:
    """Return all unique album directories (parent folders of audio files) from the library DB for the given root."""
    init_library_index(db_path)
    root_path = Path(root).expanduser().resolve()
    with _connect_for_read(db_path) as conn:
        rows = conn.execute(
            "SELECT relative_path FROM library_files WHERE root_path = ? AND file_missing = 0",
            (str(root_path),),
        ).fetchall()
    album_dirs = set()
    for row in rows:
        rel_path = row["relative_path"]
        parent = Path(rel_path).parent
        if str(parent) and str(parent) != ".":
            album_dirs.add(root_path / parent)
    return sorted(album_dirs)

import json
import os
import sqlite3
import time
import xml.etree.ElementTree as ET
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app.services.song_metadata import (
    AUDIO_EXTENSIONS,
    guess_preliminary_metadata,
    load_embedded_audio_metadata,
    load_song_metadata_xml,
    normalize_downloaded_from,
)

_DB_CONNECT_RETRIES = 3
_DB_RETRY_DELAY_SECONDS = 0.25
_DB_TIMEOUT_SECONDS = 30.0
_DB_BUSY_TIMEOUT_MS = 30_000
_MUSICBRAINZ_PENDING_WHERE_SQL = (
    "identify_audio_review_status != 'accepted-as-is' AND "
    "(musicbrainz_verified_at = '' OR musicbrainz_verified_at < modified_at)"
)
_MUSICBRAINZ_STALE_WHERE_SQL = (
    "identify_audio_review_status != 'accepted-as-is' AND "
    "musicbrainz_verified_at != '' AND musicbrainz_verified_at < modified_at"
)
_ACCEPTED_AS_IS_WHERE_SQL = "identify_audio_review_status = 'accepted-as-is'"


def init_library_index(db_path: str | Path) -> None:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with _connect(path) as conn:
        _execute_write(
            conn,
            """
            CREATE TABLE IF NOT EXISTS library_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                root_path TEXT NOT NULL,
                relative_path TEXT NOT NULL,
                audio_path TEXT NOT NULL,
                extension TEXT NOT NULL,
                size_bytes INTEGER NOT NULL DEFAULT 0,
                modified_at TEXT NOT NULL,
                discovered_at TEXT NOT NULL,
                last_indexed_at TEXT NOT NULL,
                content_hash TEXT NOT NULL DEFAULT '',
                file_missing INTEGER NOT NULL DEFAULT 0,
                xml_path TEXT NOT NULL DEFAULT '',
                xml_exists INTEGER NOT NULL DEFAULT 0,
                xml_modified_at TEXT NOT NULL DEFAULT '',
                xml_parse_ok INTEGER NOT NULL DEFAULT 0,
                xml_error TEXT NOT NULL DEFAULT '',
                xml_has_title INTEGER NOT NULL DEFAULT 0,
                xml_has_artist INTEGER NOT NULL DEFAULT 0,
                xml_has_album INTEGER NOT NULL DEFAULT 0,
                xml_has_downloaded_from INTEGER NOT NULL DEFAULT 0,
                xml_has_deezer_id INTEGER NOT NULL DEFAULT 0,
                xml_has_musicbrainz_track_id INTEGER NOT NULL DEFAULT 0,
                xml_core_complete INTEGER NOT NULL DEFAULT 0,
                title TEXT NOT NULL DEFAULT '',
                artist TEXT NOT NULL DEFAULT '',
                album TEXT NOT NULL DEFAULT '',
                provider TEXT NOT NULL DEFAULT '',
                downloaded_from TEXT NOT NULL DEFAULT '',
                deezer_id TEXT NOT NULL DEFAULT '',
                musicbrainz_track_id TEXT NOT NULL DEFAULT '',
                embedded_title TEXT NOT NULL DEFAULT '',
                embedded_artist TEXT NOT NULL DEFAULT '',
                embedded_album TEXT NOT NULL DEFAULT '',
                embedded_albumartist TEXT NOT NULL DEFAULT '',
                embedded_track_number TEXT NOT NULL DEFAULT '',
                embedded_musicbrainz_album_id TEXT NOT NULL DEFAULT '',
                embedded_musicbrainz_artist_id TEXT NOT NULL DEFAULT '',
                embedded_musicbrainz_albumartist_id TEXT NOT NULL DEFAULT '',
                embedded_deezer_id TEXT NOT NULL DEFAULT '',
                embedded_musicbrainz_track_id TEXT NOT NULL DEFAULT '',
                musicbrainz_verified_at TEXT NOT NULL DEFAULT '',
                identify_audio_review_status TEXT NOT NULL DEFAULT '',
                embedded_tags_checked_at TEXT NOT NULL DEFAULT '',
                audio_health_status TEXT NOT NULL DEFAULT '',
                audio_health_message TEXT NOT NULL DEFAULT '',
                audio_health_checked_at TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(root_path, relative_path)
            )
            """,
        )
        _execute_write(
            conn,
            """
            CREATE TABLE IF NOT EXISTS library_tool_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tool_name TEXT NOT NULL,
                root_path TEXT NOT NULL,
                run_mode TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT NOT NULL,
                scanned_count INTEGER NOT NULL DEFAULT 0,
                changed_count INTEGER NOT NULL DEFAULT 0,
                error_count INTEGER NOT NULL DEFAULT 0,
                result_json TEXT NOT NULL DEFAULT '{}'
            )
            """,
        )
        _execute_write(
            conn,
            """
            CREATE TABLE IF NOT EXISTS library_xml_sidecars (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                root_path TEXT NOT NULL,
                relative_xml_path TEXT NOT NULL,
                xml_path TEXT NOT NULL,
                paired_audio_path TEXT NOT NULL DEFAULT '',
                paired_audio_exists INTEGER NOT NULL DEFAULT 0,
                modified_at TEXT NOT NULL,
                parse_ok INTEGER NOT NULL DEFAULT 0,
                error_message TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(root_path, relative_xml_path)
            )
            """,
        )
        _execute_write(
            conn,
            "CREATE INDEX IF NOT EXISTS idx_library_files_root ON library_files(root_path)",
        )
        _execute_write(
            conn,
            "CREATE INDEX IF NOT EXISTS idx_library_files_audio_health "
            "ON library_files(root_path, file_missing, audio_health_status, "
            "audio_health_checked_at)",
        )
        _execute_write(
            conn,
            "CREATE INDEX IF NOT EXISTS idx_library_files_xml_state "
            "ON library_files(root_path, file_missing, xml_exists, "
            "xml_core_complete, downloaded_from)",
        )
        _execute_write(
            conn,
            "CREATE INDEX IF NOT EXISTS idx_library_xml_sidecars_root "
            "ON library_xml_sidecars(root_path, paired_audio_exists)",
        )
        _ensure_library_files_column(
            conn,
            "embedded_track_number",
            "TEXT NOT NULL DEFAULT ''",
        )
        _ensure_library_files_column(
            conn,
            "embedded_musicbrainz_album_id",
            "TEXT NOT NULL DEFAULT ''",
        )
        _ensure_library_files_column(
            conn,
            "embedded_musicbrainz_artist_id",
            "TEXT NOT NULL DEFAULT ''",
        )
        _ensure_library_files_column(
            conn,
            "embedded_musicbrainz_albumartist_id",
            "TEXT NOT NULL DEFAULT ''",
        )
        _ensure_library_files_column(
            conn,
            "identify_audio_review_status",
            "TEXT NOT NULL DEFAULT ''",
        )
        _ensure_library_files_column(
            conn,
            "musicbrainz_verified_at",
            "TEXT NOT NULL DEFAULT ''",
        )
        conn.commit()


def refresh_library_index(
    db_path: str | Path,
    root: str | Path,
    *,
    progress_callback: Callable[[str], None] | None = None,
    limit: int | None = None,
    scan_xml_sidecars: bool = True,
) -> dict[str, int]:
    init_library_index(db_path)

    root_path = Path(root).expanduser().resolve()
    if not root_path.exists() or not root_path.is_dir():
        raise ValueError(
            f"Music library root does not exist or is not a directory: {root_path}"
        )

    indexed_at = _utc_now()
    partial_refresh = limit is not None
    scanned = 0
    changed = 0
    unchanged = 0
    xml_changed = 0
    embedded_changed = 0
    xml_scanned = 0
    seen_relative_paths: set[str] = set()

    with _connect(db_path) as conn:
        db_rows = {}
        disk_paths = {}
        to_remove = set()
        to_check = set()
        db_count = 0
        # Query all files in DB for this root
        for row in conn.execute(
            "SELECT * FROM library_files WHERE root_path = ?", (str(root_path),)
        ):
            rel = row["relative_path"]
            db_rows[rel] = row
            db_count += 1
            if progress_callback and (db_count == 1 or db_count % 250 == 0):
                _emit_progress(progress_callback, f"PROGRESS: querying DB for existing files... {db_count} so far")
        if progress_callback:
            _emit_progress(progress_callback, f"PROGRESS: finished DB query, {db_count} files found.")
        disk_count = 0
        # Find all audio files on disk
        for audio_path in root_path.rglob("*"):
            if audio_path.is_file() and audio_path.suffix.lower() in AUDIO_EXTENSIONS:
                rel = audio_path.relative_to(root_path).as_posix()
                disk_paths[rel] = audio_path
                disk_count += 1
                if progress_callback and (disk_count == 1 or disk_count % 250 == 0):
                    _emit_progress(progress_callback, f"PROGRESS: scanning disk for audio files... {disk_count} so far")
        if progress_callback:
            _emit_progress(progress_callback, f"PROGRESS: finished disk scan, {disk_count} files found.")
        if progress_callback:
            _emit_progress(progress_callback, f"PROGRESS: comparing DB and disk state...")
        # Files in DB but not on disk: to_remove
        to_remove = set(db_rows) - set(disk_paths)
        # Files on disk that are in DB: to_check
        to_check = set(db_rows) & set(disk_paths)
        if progress_callback:
            _emit_progress(progress_callback, f"PROGRESS: {len(to_remove)} files to remove, {len(to_check)} files to check for changes...")
        # --- XML sidecar scan (unchanged) ---
        if scan_xml_sidecars:
            _emit_progress(progress_callback, "Step 1: Indexing XML sidecars...")
            if not partial_refresh:
                _execute_write(
                    conn,
                    "DELETE FROM library_xml_sidecars WHERE root_path = ?",
                    (str(root_path),),
                )
            xml_paths = sorted(root_path.rglob("*.xml"))
            if partial_refresh:
                xml_paths = xml_paths[:limit]
            for xml_index, xml_path in enumerate(xml_paths, start=1):
                paired_audio_path = _paired_audio_path_for_xml(xml_path)
                if not xml_path.is_file():
                    continue
                xml_scanned += 1
                relative_xml_path = xml_path.relative_to(root_path).as_posix()
                # Parse XML for error state
                parse_ok = 0
                error_message = ""
                try:
                    import xml.etree.ElementTree as ET
                    ET.parse(xml_path)
                    parse_ok = 1
                except Exception as ex:
                    error_message = str(ex)
                now = _utc_now()
                if xml_index == 1 or xml_index % 250 == 0 or xml_index == len(xml_paths):
                    xml_total = f" / {len(xml_paths)}" if partial_refresh else ""
                    _emit_progress(
                        progress_callback,
                        f"  Indexed {xml_index}{xml_total} XML sidecars...",
                    )
                # Insert XML sidecar
                _execute_write(
                    conn,
                    """
                    INSERT INTO library_xml_sidecars (
                        root_path,
                        relative_xml_path,
                        xml_path,
                        paired_audio_path,
                        paired_audio_exists,
                        modified_at,
                        parse_ok,
                        error_message,
                        created_at,
                        updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(root_path, relative_xml_path) DO UPDATE SET
                        xml_path = excluded.xml_path,
                        paired_audio_path = excluded.paired_audio_path,
                        paired_audio_exists = excluded.paired_audio_exists,
                        modified_at = excluded.modified_at,
                        parse_ok = excluded.parse_ok,
                        error_message = excluded.error_message,
                        updated_at = excluded.updated_at
                    """,
                    (
                        str(root_path),
                        relative_xml_path,
                        str(xml_path),
                        str(paired_audio_path or ""),
                        int(paired_audio_path is not None),
                        _iso_from_timestamp(xml_path.stat().st_mtime),
                        parse_ok,
                        error_message,
                        now,
                        now,
                    ),
                )

        # Remove missing files
        total_to_remove = len(to_remove)
        if total_to_remove > 0 and progress_callback:
            _emit_progress(progress_callback, f"PROGRESS: removing {total_to_remove} missing files from DB...")
        for idx, rel in enumerate(sorted(to_remove), start=1):
            _delete_library_file_row(
                conn,
                root_path=root_path,
                relative_path=rel,
            )
            changed += 1
            if progress_callback and (
                idx == 1 or idx % 100 == 0 or idx == total_to_remove
            ):
                _emit_progress(
                    progress_callback,
                    f"PROGRESS: removed {idx}/{total_to_remove} missing audio files from DB...",
                )

        # Check for changed files (mtime/size)
        total_to_check = len(to_check)
        if total_to_check > 0 and progress_callback:
            _emit_progress(progress_callback, f"PROGRESS: checking {total_to_check} files for changes...")
        for idx, rel in enumerate(sorted(to_check), start=1):
            audio_path = disk_paths[rel]
            row = db_rows[rel]
            stat = audio_path.stat()
            modified_at = _iso_from_timestamp(stat.st_mtime)
            size_bytes = int(stat.st_size)
            if (
                int(row["size_bytes"] or 0) == size_bytes
                and str(row["modified_at"] or "") == modified_at
            ):
                unchanged += 1
                continue
            # Only update if changed
            xml_path = audio_path.with_suffix(".xml")
            xml_exists = xml_path.exists()
            xml_modified_at = (
                _iso_from_timestamp(xml_path.stat().st_mtime) if xml_exists else ""
            )
            xml_state = _summarize_xml_state(xml_path)
            xml_changed += 1 if xml_exists else 0
            embedded_state = _summarize_embedded_state(audio_path)
            embedded_changed += 1
            created_at = str(row["created_at"]) if row is not None else indexed_at
            _execute_write(
                conn,
                """
                INSERT INTO library_files (
                    root_path, relative_path, audio_path, extension, size_bytes, modified_at,
                    discovered_at, last_indexed_at, file_missing, xml_path, xml_exists, xml_modified_at,
                    xml_parse_ok, xml_error, xml_has_title, xml_has_artist, xml_has_album, xml_has_downloaded_from,
                    xml_has_deezer_id, xml_has_musicbrainz_track_id, xml_core_complete, title, artist, album, provider,
                    downloaded_from, deezer_id, musicbrainz_track_id, embedded_title, embedded_artist, embedded_album,
                    embedded_albumartist, embedded_track_number, embedded_musicbrainz_album_id, embedded_musicbrainz_artist_id,
                    embedded_musicbrainz_albumartist_id, embedded_deezer_id, embedded_musicbrainz_track_id, musicbrainz_verified_at,
                    embedded_tags_checked_at, created_at, updated_at
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                ON CONFLICT(root_path, relative_path) DO UPDATE SET
                    audio_path = excluded.audio_path,
                    extension = excluded.extension,
                    size_bytes = excluded.size_bytes,
                    modified_at = excluded.modified_at,
                    last_indexed_at = excluded.last_indexed_at,
                    file_missing = excluded.file_missing,
                    xml_path = excluded.xml_path,
                    xml_exists = excluded.xml_exists,
                    xml_modified_at = excluded.xml_modified_at,
                    xml_parse_ok = excluded.xml_parse_ok,
                    xml_error = excluded.xml_error,
                    xml_has_title = excluded.xml_has_title,
                    xml_has_artist = excluded.xml_has_artist,
                    xml_has_album = excluded.xml_has_album,
                    xml_has_downloaded_from = excluded.xml_has_downloaded_from,
                    xml_has_deezer_id = excluded.deezer_id,
                    xml_has_musicbrainz_track_id = excluded.musicbrainz_track_id,
                    xml_core_complete = excluded.xml_core_complete,
                    title = excluded.title,
                    artist = excluded.artist,
                    album = excluded.album,
                    provider = excluded.provider,
                    downloaded_from = excluded.downloaded_from,
                    deezer_id = excluded.deezer_id,
                    musicbrainz_track_id = excluded.musicbrainz_track_id,
                    embedded_title = excluded.embedded_title,
                    embedded_artist = excluded.embedded_artist,
                    embedded_album = excluded.embedded_album,
                    embedded_albumartist = excluded.embedded_albumartist,
                    embedded_track_number = excluded.embedded_track_number,
                    embedded_musicbrainz_album_id = excluded.embedded_musicbrainz_album_id,
                    embedded_musicbrainz_artist_id = excluded.embedded_musicbrainz_artist_id,
                    embedded_musicbrainz_albumartist_id = excluded.embedded_musicbrainz_albumartist_id,
                    embedded_deezer_id = excluded.embedded_deezer_id,
                    embedded_musicbrainz_track_id = excluded.embedded_musicbrainz_track_id,
                    musicbrainz_verified_at = library_files.musicbrainz_verified_at,
                    embedded_tags_checked_at = excluded.embedded_tags_checked_at,
                    updated_at = excluded.updated_at
                """,
                (
                    str(root_path),
                    rel,
                    str(audio_path),
                    audio_path.suffix.lower(),
                    size_bytes,
                    modified_at,
                    created_at,
                    indexed_at,
                    0,
                    str(xml_path),
                    int(xml_exists),
                    xml_modified_at,
                    xml_state["xml_parse_ok"],
                    xml_state["xml_error"],
                    xml_state["xml_has_title"],
                    xml_state["xml_has_artist"],
                    xml_state["xml_has_album"],
                    xml_state["xml_has_downloaded_from"],
                    xml_state["xml_has_deezer_id"],
                    xml_state["xml_has_musicbrainz_track_id"],
                    xml_state["xml_core_complete"],
                    xml_state["title"],
                    xml_state["artist"],
                    xml_state["album"],
                    xml_state["provider"],
                    xml_state["downloaded_from"],
                    xml_state["deezer_id"],
                    xml_state["musicbrainz_track_id"],
                    embedded_state["embedded_title"],
                    embedded_state["embedded_artist"],
                    embedded_state["embedded_album"],
                    embedded_state["embedded_albumartist"],
                    embedded_state["embedded_track_number"],
                    embedded_state["embedded_musicbrainz_album_id"],
                    embedded_state["embedded_musicbrainz_artist_id"],
                    embedded_state["embedded_musicbrainz_albumartist_id"],
                    embedded_state["embedded_deezer_id"],
                    embedded_state["embedded_musicbrainz_track_id"],
                    str(row["musicbrainz_verified_at"] or ""),
                    embedded_state["embedded_tags_checked_at"],
                    created_at,
                    indexed_at,
                ),
            )
            changed += 1
            if progress_callback and (idx == 1 or idx % 100 == 0 or idx == total_to_check):
                _emit_progress(progress_callback, f"PROGRESS: checked {idx}/{total_to_check} files for changes...")
        conn.commit()

    _emit_progress(
        progress_callback,
        f"PROGRESS: refresh list build complete (xml={xml_scanned}, audio={scanned})",
    )

    return {
        "scanned": scanned,
        "changed": changed,
        "unchanged": unchanged,
        "xml_scanned": xml_scanned,
        "xml_changed": xml_changed,
        "embedded_changed": embedded_changed,
        "partial": int(partial_refresh),
        "limit": limit or 0,
        "xml_scan_skipped": int(not scan_xml_sidecars),
    }


def refresh_library_index_for_paths(
    db_path: str | Path,
    root: str | Path,
    audio_paths: list[str | Path],
    *,
    scan_xml_sidecars: bool = True,
) -> dict[str, int]:
    init_library_index(db_path)

    root_path = Path(root).expanduser().resolve()
    if not root_path.exists() or not root_path.is_dir():
        raise ValueError(
            f"Music library root does not exist or is not a directory: {root_path}"
        )

    indexed_at = _utc_now()
    scanned = 0
    changed = 0
    unchanged = 0
    xml_scanned = 0
    xml_changed = 0
    embedded_changed = 0

    selected_paths = _normalize_selected_audio_paths(root_path, audio_paths)
    if not selected_paths:
        return {
            "scanned": 0,
            "changed": 0,
            "unchanged": 0,
            "xml_scanned": 0,
            "xml_changed": 0,
            "embedded_changed": 0,
            "partial": 1,
            "limit": 0,
            "xml_scan_skipped": int(not scan_xml_sidecars),
        }

    with _connect(db_path) as conn:
        existing_rows = {
            str(row["relative_path"]): row
            for row in conn.execute(
                "SELECT * FROM library_files WHERE root_path = ? AND relative_path IN ({})".format(
                    ", ".join("?" for _ in selected_paths)
                ),
                (
                    str(root_path),
                    *(
                        path.relative_to(root_path).as_posix()
                        for path in selected_paths
                    ),
                ),
            ).fetchall()
        }

        for audio_path in selected_paths:
            relative_path = audio_path.relative_to(root_path).as_posix()
            row = existing_rows.get(relative_path)

            if not audio_path.exists() or not audio_path.is_file():
                if row is not None:
                    _delete_library_file_row(
                        conn,
                        root_path=root_path,
                        relative_path=relative_path,
                    )
                    changed += 1
                continue

            scanned += 1
            stat = audio_path.stat()
            modified_at = _iso_from_timestamp(stat.st_mtime)
            size_bytes = int(stat.st_size)
            xml_path = audio_path.with_suffix(".xml")
            xml_exists = xml_path.exists()
            xml_modified_at = (
                _iso_from_timestamp(xml_path.stat().st_mtime) if xml_exists else ""
            )

            xml_state = _empty_xml_state(xml_path)
            if row is not None:
                xml_state.update(
                    {
                        "xml_parse_ok": int(row["xml_parse_ok"] or 0),
                        "xml_error": str(row["xml_error"] or ""),
                        "xml_has_title": int(row["xml_has_title"] or 0),
                        "xml_has_artist": int(row["xml_has_artist"] or 0),
                        "xml_has_album": int(row["xml_has_album"] or 0),
                        "xml_has_downloaded_from": int(
                            row["xml_has_downloaded_from"] or 0
                        ),
                        "xml_has_deezer_id": int(row["xml_has_deezer_id"] or 0),
                        "xml_has_musicbrainz_track_id": int(
                                1 if row["xml_has_musicbrainz_track_id"] else 0
                            ),
                        "xml_core_complete": int(row["xml_core_complete"] or 0),
                        "title": str(row["title"] or ""),
                        "artist": str(row["artist"] or ""),
                        "album": str(row["album"] or ""),
                        "provider": str(row["provider"] or ""),
                        "downloaded_from": str(row["downloaded_from"] or ""),
                        "deezer_id": str(row["deezer_id"] or ""),
                        "musicbrainz_track_id": str(row["musicbrainz_track_id"] or ""),
                    }
                )
            if scan_xml_sidecars:
                xml_state = _summarize_xml_state(xml_path)
                xml_changed += 1
                if xml_exists:
                    xml_scanned += 1
                    paired_audio_path = _paired_audio_path_for_xml(xml_path)
                    now = _utc_now()
                    _execute_write(
                        conn,
                        """
                        INSERT INTO library_xml_sidecars (
                            root_path,
                            relative_xml_path,
                            xml_path,
                            paired_audio_path,
                            paired_audio_exists,
                            modified_at,
                            parse_ok,
                            error_message,
                            created_at,
                            updated_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(root_path, relative_xml_path) DO UPDATE SET
                            xml_path = excluded.xml_path,
                            paired_audio_path = excluded.paired_audio_path,
                            paired_audio_exists = excluded.paired_audio_exists,
                            modified_at = excluded.modified_at,
                            parse_ok = excluded.parse_ok,
                            error_message = excluded.error_message,
                            updated_at = excluded.updated_at
                        """,
                        (
                            str(root_path),
                            xml_path.relative_to(root_path).as_posix(),
                            str(xml_path),
                            str(paired_audio_path or ""),
                            int(paired_audio_path is not None),
                            _iso_from_timestamp(xml_path.stat().st_mtime),
                            xml_state["xml_parse_ok"],
                            xml_state["xml_error"],
                            now,
                            now,
                        ),
                    )

            embedded_state = _summarize_embedded_state(audio_path)
            embedded_changed += 1
            created_at = str(row["created_at"]) if row is not None else indexed_at

            current_values = {
                "size_bytes": size_bytes,
                "modified_at": modified_at,
                "file_missing": 0,
                "xml_exists": int(xml_exists),
                "xml_modified_at": xml_modified_at,
            }
            if row is None or any(
                current_values[key] != row[key] for key in current_values
            ):
                changed += 1
            else:
                unchanged += 1

            _execute_write(
                conn,
                """
                INSERT INTO library_files (
                    root_path,
                    relative_path,
                    audio_path,
                    extension,
                    size_bytes,
                    modified_at,
                    discovered_at,
                    last_indexed_at,
                    file_missing,
                    xml_path,
                    xml_exists,
                    xml_modified_at,
                    xml_parse_ok,
                    xml_error,
                    xml_has_title,
                    xml_has_artist,
                    xml_has_album,
                    xml_has_downloaded_from,
                    xml_has_deezer_id,
                    xml_has_musicbrainz_track_id,
                    xml_core_complete,
                    title,
                    artist,
                    album,
                    provider,
                    downloaded_from,
                    deezer_id,
                    musicbrainz_track_id,
                    embedded_title,
                    embedded_artist,
                    embedded_album,
                    embedded_albumartist,
                    embedded_track_number,
                    embedded_musicbrainz_album_id,
                    embedded_musicbrainz_artist_id,
                    embedded_musicbrainz_albumartist_id,
                    embedded_deezer_id,
                    embedded_musicbrainz_track_id,
                    musicbrainz_verified_at,
                    embedded_tags_checked_at,
                    created_at,
                    updated_at
                )
                VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                ON CONFLICT(root_path, relative_path) DO UPDATE SET
                    audio_path = excluded.audio_path,
                    extension = excluded.extension,
                    size_bytes = excluded.size_bytes,
                    modified_at = excluded.modified_at,
                    last_indexed_at = excluded.last_indexed_at,
                    file_missing = excluded.file_missing,
                    xml_path = excluded.xml_path,
                    xml_exists = excluded.xml_exists,
                    xml_modified_at = excluded.xml_modified_at,
                    xml_parse_ok = excluded.xml_parse_ok,
                    xml_error = excluded.xml_error,
                    xml_has_title = excluded.xml_has_title,
                    xml_has_artist = excluded.xml_has_artist,
                    xml_has_album = excluded.xml_has_album,
                    xml_has_downloaded_from = excluded.xml_has_downloaded_from,
                    xml_has_deezer_id = excluded.xml_has_deezer_id,
                    xml_has_musicbrainz_track_id = excluded.xml_has_musicbrainz_track_id,
                    xml_core_complete = excluded.xml_core_complete,
                    title = excluded.title,
                    artist = excluded.artist,
                    album = excluded.album,
                    provider = excluded.provider,
                    downloaded_from = excluded.downloaded_from,
                    deezer_id = excluded.deezer_id,
                    musicbrainz_track_id = excluded.musicbrainz_track_id,
                    embedded_title = excluded.embedded_title,
                    embedded_artist = excluded.embedded_artist,
                    embedded_album = excluded.embedded_album,
                    embedded_albumartist = excluded.embedded_albumartist,
                    embedded_track_number = excluded.embedded_track_number,
                    embedded_musicbrainz_album_id = excluded.embedded_musicbrainz_album_id,
                    embedded_musicbrainz_artist_id = excluded.embedded_musicbrainz_artist_id,
                    embedded_musicbrainz_albumartist_id =
                        excluded.embedded_musicbrainz_albumartist_id,
                    embedded_deezer_id = excluded.embedded_deezer_id,
                    embedded_musicbrainz_track_id = excluded.embedded_musicbrainz_track_id,
                    musicbrainz_verified_at = library_files.musicbrainz_verified_at,
                    embedded_tags_checked_at = excluded.embedded_tags_checked_at,
                    updated_at = excluded.updated_at
                """,
                (
                    str(root_path),
                    relative_path,
                    str(audio_path),
                    audio_path.suffix.lower(),
                    size_bytes,
                    modified_at,
                    created_at,
                    indexed_at,
                    0,
                    str(xml_path),
                    int(xml_exists),
                    xml_modified_at,
                    xml_state["xml_parse_ok"],
                    xml_state["xml_error"],
                    xml_state["xml_has_title"],
                    xml_state["xml_has_artist"],
                    xml_state["xml_has_album"],
                    xml_state["xml_has_downloaded_from"],
                    xml_state["xml_has_deezer_id"],
                    xml_state["xml_has_musicbrainz_track_id"],
                    xml_state["xml_core_complete"],
                    xml_state["title"],
                    xml_state["artist"],
                    xml_state["album"],
                    xml_state["provider"],
                    xml_state["downloaded_from"],
                    xml_state["deezer_id"],
                    xml_state["musicbrainz_track_id"],
                    embedded_state["embedded_title"],
                    embedded_state["embedded_artist"],
                    embedded_state["embedded_album"],
                    embedded_state["embedded_albumartist"],
                    embedded_state["embedded_track_number"],
                    embedded_state["embedded_musicbrainz_album_id"],
                    embedded_state["embedded_musicbrainz_artist_id"],
                    embedded_state["embedded_musicbrainz_albumartist_id"],
                    embedded_state["embedded_deezer_id"],
                    embedded_state["embedded_musicbrainz_track_id"],
                    (
                        str(row["musicbrainz_verified_at"] or "")
                        if row is not None
                        else ""
                    ),
                    embedded_state["embedded_tags_checked_at"],
                    created_at,
                    indexed_at,
                ),
            )

        conn.commit()

    return {
        "scanned": scanned,
        "changed": changed,
        "unchanged": unchanged,
        "xml_scanned": xml_scanned,
        "xml_changed": xml_changed,
        "embedded_changed": embedded_changed,
        "partial": 1,
        "limit": len(selected_paths),
        "xml_scan_skipped": int(not scan_xml_sidecars),
    }


def _emit_progress(
    progress_callback: Callable[[str], None] | None, message: str
) -> None:
    if progress_callback is not None:
        progress_callback(message)


def list_orphaned_xml_paths(
    db_path: str | Path,
    root: str | Path,
    *,
    limit: int | None = None,
) -> list[Path]:
    init_library_index(db_path)
    root_path = Path(root).expanduser().resolve()
    query = (
        "SELECT xml_path FROM library_xml_sidecars "
        "WHERE root_path = ? AND paired_audio_exists = 0 "
        "ORDER BY relative_xml_path ASC"
    )
    params: list[Any] = [str(root_path)]
    if limit is not None:
        query += " LIMIT ?"
        params.append(limit)
    with _connect(db_path) as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
    return [Path(str(row["xml_path"])) for row in rows]


def list_missing_xml_audio_paths(
    db_path: str | Path,
    root: str | Path,
    *,
    limit: int | None = None,
) -> list[Path]:
    init_library_index(db_path)
    root_path = Path(root).expanduser().resolve()
    query = (
        "SELECT audio_path FROM library_files "
        "WHERE root_path = ? AND file_missing = 0 AND xml_exists = 0 "
        "ORDER BY relative_path ASC"
    )
    params: list[Any] = [str(root_path)]
    if limit is not None:
        query += " LIMIT ?"
        params.append(limit)
    with _connect(db_path) as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
    return [Path(str(row["audio_path"])) for row in rows]


def list_incomplete_xml_pairs(
    db_path: str | Path,
    root: str | Path,
    *,
    limit: int | None = None,
) -> list[tuple[Path, Path]]:
    init_library_index(db_path)
    root_path = Path(root).expanduser().resolve()
    query = (
        "SELECT xml_path, audio_path FROM library_files "
        "WHERE root_path = ? AND file_missing = 0 AND xml_exists = 1 AND ("
        "xml_has_title = 0 OR xml_has_artist = 0 OR xml_has_album = 0 OR "
        "xml_has_downloaded_from = 0 OR xml_has_musicbrainz_track_id = 0 OR "
        "(downloaded_from = 'deezer' AND xml_has_deezer_id = 0)"
        ") ORDER BY relative_path ASC"
    )
    params: list[Any] = [str(root_path)]
    if limit is not None:
        query += " LIMIT ?"
        params.append(limit)
    with _connect(db_path) as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
    return [(Path(str(row["xml_path"])), Path(str(row["audio_path"]))) for row in rows]


def list_xml_id_repair_candidates(
    db_path: str | Path,
    root: str | Path,
    *,
    force_full: bool = False,
    limit: int | None = None,
) -> list[tuple[Path, Path]]:
    init_library_index(db_path)
    root_path = Path(root).expanduser().resolve()
    query = [
        "SELECT xml_path, audio_path FROM library_files ",
        "WHERE root_path = ? AND file_missing = 0 AND xml_exists = 1",
    ]
    params: list[Any] = [str(root_path)]
    if not force_full:
        query.append(
            "AND ((xml_has_musicbrainz_track_id = 0 AND embedded_musicbrainz_track_id != '') "
            "OR (xml_has_deezer_id = 0 AND embedded_deezer_id != '' AND "
            "(downloaded_from = 'deezer' OR embedded_deezer_id != '')))"
        )
    query.append("ORDER BY relative_path ASC")
    if limit is not None:
        query.append("LIMIT ?")
        params.append(limit)
    with _connect(db_path) as conn:
        rows = conn.execute(" ".join(query), tuple(params)).fetchall()
    return [(Path(str(row["xml_path"])), Path(str(row["audio_path"]))) for row in rows]


def get_library_report_counts(db_path: str | Path, root: str | Path) -> dict[str, int]:
    if not _library_index_exists(db_path):
        return {}
    root_path = Path(root).expanduser().resolve()
    with _connect_for_read(db_path) as conn:
        indexed_audio = _scalar(
            conn,
            "SELECT COUNT(*) FROM library_files WHERE root_path = ? AND file_missing = 0",
            (str(root_path),),
        )
        corrupted_audio = _scalar(
            conn,
            "SELECT COUNT(*) FROM library_files WHERE root_path = ? AND file_missing = 0 "
            "AND audio_health_status IN ('warning', 'error')",
            (str(root_path),),
        )
        missing_xml = _scalar(
            conn,
            "SELECT COUNT(*) FROM library_files WHERE root_path = ? AND file_missing = 0 "
            "AND xml_exists = 0",
            (str(root_path),),
        )
        incomplete_xml = _scalar(
            conn,
            "SELECT COUNT(*) FROM library_files WHERE root_path = ? AND file_missing = 0 "
            "AND xml_exists = 1 AND (xml_has_title = 0 OR xml_has_artist = 0 OR "
            "xml_has_album = 0 OR xml_has_downloaded_from = 0 OR "
            "xml_has_musicbrainz_track_id = 0 OR "
            "(downloaded_from = 'deezer' AND xml_has_deezer_id = 0))",
            (str(root_path),),
        )
        non_deezer_source = _scalar(
            conn,
            "SELECT COUNT(*) FROM library_files WHERE root_path = ? AND file_missing = 0 "
            "AND downloaded_from NOT IN ('', 'unknown', 'deezer')",
            (str(root_path),),
        )
        orphaned_xml = _scalar(
            conn,
            "SELECT COUNT(*) FROM library_xml_sidecars "
            "WHERE root_path = ? AND paired_audio_exists = 0",
            (str(root_path),),
        )
        musicbrainz_pending = _scalar(
            conn,
            "SELECT COUNT(*) FROM library_files WHERE root_path = ? AND file_missing = 0 "
            f"AND {_MUSICBRAINZ_PENDING_WHERE_SQL}",
            (str(root_path),),
        )
        musicbrainz_stale = _scalar(
            conn,
            "SELECT COUNT(*) FROM library_files WHERE root_path = ? AND file_missing = 0 "
            f"AND {_MUSICBRAINZ_STALE_WHERE_SQL}",
            (str(root_path),),
        )
        accepted_as_is = _scalar(
            conn,
            "SELECT COUNT(*) FROM library_files WHERE root_path = ? AND file_missing = 0 "
            f"AND {_ACCEPTED_AS_IS_WHERE_SQL}",
            (str(root_path),),
        )
    return {
        "indexed_audio": indexed_audio,
        "musicbrainz_pending": musicbrainz_pending,
        "musicbrainz_stale": musicbrainz_stale,
        "accepted_as_is": accepted_as_is,
        "corrupted_audio": corrupted_audio,
        "missing_xml": missing_xml,
        "incomplete_xml": incomplete_xml,
        "non_deezer_source": non_deezer_source,
        "orphaned_xml": orphaned_xml,
    }


def list_library_report_items(
    db_path: str | Path,
    root: str | Path,
    *,
    report_filter: str,
    limit: int = 100,
) -> list[dict[str, str]]:
    if not _library_index_exists(db_path):
        return []
    root_path = Path(root).expanduser().resolve()
    normalized_filter = str(report_filter or "").strip().lower()
    with _connect_for_read(db_path) as conn:
        if normalized_filter == "musicbrainz-pending":
            rows = conn.execute(
                "SELECT relative_path, musicbrainz_verified_at, modified_at "
                "FROM library_files WHERE root_path = ? AND file_missing = 0 "
                f"AND {_MUSICBRAINZ_PENDING_WHERE_SQL} "
                "ORDER BY relative_path ASC LIMIT ?",
                (str(root_path), limit),
            ).fetchall()
            return [
                {
                    "path": str(row["relative_path"]),
                    "detail": _describe_musicbrainz_pending(row),
                    "badge": _musicbrainz_pending_badge(row),
                }
                for row in rows
            ]
        if normalized_filter == "accepted-as-is":
            rows = conn.execute(
                "SELECT relative_path FROM library_files WHERE root_path = ? AND file_missing = 0 "
                f"AND {_ACCEPTED_AS_IS_WHERE_SQL} "
                "ORDER BY relative_path ASC LIMIT ?",
                (str(root_path), limit),
            ).fetchall()
            return [
                {
                    "path": str(row["relative_path"]),
                    "detail": "Accepted during fingerprint review without changing tags or XML.",
                    "badge": "kept",
                }
                for row in rows
            ]
        if normalized_filter == "corrupted-audio":
            rows = conn.execute(
                "SELECT relative_path, audio_health_status, audio_health_message, downloaded_from "
                "FROM library_files WHERE root_path = ? AND file_missing = 0 "
                "AND audio_health_status IN ('warning', 'error') "
                "ORDER BY relative_path ASC LIMIT ?",
                (str(root_path), limit),
            ).fetchall()
            return [
                {
                    "path": str(row["relative_path"]),
                    "detail": str(
                        row["audio_health_message"] or row["audio_health_status"]
                    ),
                    "badge": str(row["audio_health_status"] or "error"),
                }
                for row in rows
            ]
        if normalized_filter == "missing-xml":
            rows = conn.execute(
                "SELECT relative_path, downloaded_from FROM library_files WHERE root_path = ? "
                "AND file_missing = 0 AND xml_exists = 0 ORDER BY relative_path ASC LIMIT ?",
                (str(root_path), limit),
            ).fetchall()
            return [
                {
                    "path": str(row["relative_path"]),
                    "detail": "Missing XML sidecar",
                    "badge": str(row["downloaded_from"] or "unknown"),
                }
                for row in rows
            ]
        if normalized_filter == "incomplete-xml":
            rows = conn.execute(
                "SELECT relative_path, downloaded_from, xml_has_musicbrainz_track_id, "
                "xml_has_deezer_id, xml_has_downloaded_from, xml_has_title, "
                "xml_has_artist, xml_has_album "
                "FROM library_files WHERE root_path = ? AND file_missing = 0 AND xml_exists = 1 "
                "AND (xml_has_title = 0 OR xml_has_artist = 0 OR xml_has_album = 0 OR "
                "xml_has_downloaded_from = 0 OR xml_has_musicbrainz_track_id = 0 OR "
                "(downloaded_from = 'deezer' AND xml_has_deezer_id = 0)) "
                "ORDER BY relative_path ASC LIMIT ?",
                (str(root_path), limit),
            ).fetchall()
            return [
                {
                    "path": str(row["relative_path"]),
                    "detail": _describe_incomplete_xml(row),
                    "badge": str(row["downloaded_from"] or "unknown"),
                }
                for row in rows
            ]
        if normalized_filter == "non-deezer-source":
            rows = conn.execute(
                "SELECT relative_path, downloaded_from, provider FROM library_files "
                "WHERE root_path = ? "
                "AND file_missing = 0 AND downloaded_from NOT IN ('', 'unknown', 'deezer') "
                "ORDER BY relative_path ASC LIMIT ?",
                (str(root_path), limit),
            ).fetchall()
            return [
                {
                    "path": str(row["relative_path"]),
                    "detail": str(row["provider"] or row["downloaded_from"]),
                    "badge": str(row["downloaded_from"] or "unknown"),
                }
                for row in rows
            ]
        if normalized_filter == "orphaned-xml":
            rows = conn.execute(
                "SELECT relative_xml_path, error_message FROM library_xml_sidecars "
                "WHERE root_path = ? "
                "AND paired_audio_exists = 0 ORDER BY relative_xml_path ASC LIMIT ?",
                (str(root_path), limit),
            ).fetchall()
            return [
                {
                    "path": str(row["relative_xml_path"]),
                    "detail": str(row["error_message"] or "Orphaned XML sidecar"),
                    "badge": "orphan",
                }
                for row in rows
            ]
    return []


def _describe_musicbrainz_pending(row: sqlite3.Row) -> str:
    verified_at = str(row["musicbrainz_verified_at"] or "").strip()
    modified_at = str(row["modified_at"] or "").strip()
    if not verified_at:
        return "Not yet verified against MusicBrainz."
    if verified_at < modified_at:
        return "File changed after the last MusicBrainz verification."
    return "Waiting for MusicBrainz verification."


def _musicbrainz_pending_badge(row: sqlite3.Row) -> str:
    return (
        "stale" if str(row["musicbrainz_verified_at"] or "").strip() else "unverified"
    )


def list_audio_health_candidates(
    db_path: str | Path,
    root: str | Path,
    *,
    force_full: bool = False,
    limit: int | None = None,
) -> list[Path]:
    init_library_index(db_path)
    root_path = Path(root).expanduser().resolve()

    query = [
        "SELECT audio_path FROM library_files WHERE root_path = ? AND file_missing = 0",
    ]
    params: list[Any] = [str(root_path)]
    if not force_full:
        query.append(
            "AND (audio_health_checked_at = '' OR audio_health_checked_at < modified_at)"
        )
    query.append("ORDER BY relative_path ASC")
    if limit is not None:
        query.append("LIMIT ?")
        params.append(limit)

    with _connect(db_path) as conn:
        rows = conn.execute(" ".join(query), tuple(params)).fetchall()

    return [Path(str(row["audio_path"])) for row in rows]


def list_tag_fix_candidates(
    db_path: str | Path,
    root: str | Path,
    *,
    force_full: bool = False,
    limit: int | None = None,
) -> list[Path]:
    init_library_index(db_path)
    root_path = Path(root).expanduser().resolve()

    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT relative_path, audio_path, embedded_artist, embedded_albumartist "
            "FROM library_files WHERE root_path = ? AND file_missing = 0 "
            "ORDER BY relative_path ASC",
            (str(root_path),),
        ).fetchall()

    candidates: list[Path] = []
    for row in rows:
        relative_path = str(row["relative_path"] or "")
        expected = _expected_tag_targets(relative_path)
        if expected is None:
            continue

        if force_full:
            candidates.append(Path(str(row["audio_path"])))
        else:
            current_artist = str(row["embedded_artist"] or "").strip()
            current_albumartist = str(row["embedded_albumartist"] or "").strip()
            expected_artist = expected["artist"]
            expected_albumartist = expected["albumartist"]
            if (
                current_artist != expected_artist
                or current_albumartist != expected_albumartist
            ):
                candidates.append(Path(str(row["audio_path"])))

        if limit is not None and len(candidates) >= limit:
            break

    return candidates


def list_musicbrainz_tag_candidates(
    db_path: str | Path,
    root: str | Path,
    *,
    force_full: bool = False,
    limit: int | None = None,
) -> list[Path]:
    init_library_index(db_path)
    root_path = Path(root).expanduser().resolve()

    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT audio_path, embedded_title, embedded_artist, embedded_album, "
            "embedded_albumartist, embedded_track_number, "
            "embedded_musicbrainz_album_id, embedded_musicbrainz_artist_id, "
            "embedded_musicbrainz_albumartist_id, embedded_musicbrainz_track_id, "
            "musicbrainz_verified_at, modified_at, "
            "identify_audio_review_status "
            "FROM library_files WHERE root_path = ? AND file_missing = 0 "
            "ORDER BY relative_path ASC",
            (str(root_path),),
        ).fetchall()

    candidates: list[Path] = []
    for row in rows:
        review_status = str(row["identify_audio_review_status"] or "").strip().lower()
        if review_status == "accepted-as-is":
            continue
        if force_full:
            candidates.append(Path(str(row["audio_path"])))
        else:
            title = str(row["embedded_title"] or "").strip()
            artist = str(row["embedded_artist"] or "").strip()
            album = str(row["embedded_album"] or "").strip()
            albumartist = str(row["embedded_albumartist"] or "").strip()
            track_number = str(row["embedded_track_number"] or "").strip()
            musicbrainz_album_id = str(
                row["embedded_musicbrainz_album_id"] or ""
            ).strip()
            musicbrainz_artist_id = str(
                row["embedded_musicbrainz_artist_id"] or ""
            ).strip()
            musicbrainz_albumartist_id = str(
                row["embedded_musicbrainz_albumartist_id"] or ""
            ).strip()
            musicbrainz_track_id = str(
                row["embedded_musicbrainz_track_id"] or ""
            ).strip()
            musicbrainz_verified_at = str(row["musicbrainz_verified_at"] or "").strip()
            modified_at = str(row["modified_at"] or "").strip()

            if (
                not title
                or not artist
                or not album
                or not albumartist
                or _track_number_missing(track_number)
                or not musicbrainz_album_id
                or not musicbrainz_artist_id
                or not musicbrainz_albumartist_id
                or not musicbrainz_track_id
                or not musicbrainz_verified_at
                or musicbrainz_verified_at < modified_at
            ):
                candidates.append(Path(str(row["audio_path"])))

        if limit is not None and len(candidates) >= limit:
            break

    return candidates


def list_structure_tag_candidates(
    db_path: str | Path,
    root: str | Path,
    *,
    force_full: bool = False,
    limit: int | None = None,
) -> list[Path]:
    init_library_index(db_path)
    root_path = Path(root).expanduser().resolve()

    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT audio_path, embedded_title, embedded_artist, embedded_album, "
            "embedded_albumartist, embedded_track_number "
            "FROM library_files WHERE root_path = ? AND file_missing = 0 "
            "ORDER BY relative_path ASC",
            (str(root_path),),
        ).fetchall()

    candidates: list[Path] = []
    for row in rows:
        audio_path = Path(str(row["audio_path"]))
        guessed = guess_preliminary_metadata(audio_path, root=root_path)
        if force_full:
            candidates.append(audio_path)
        else:
            if (
                (guessed.get("title") and not str(row["embedded_title"] or "").strip())
                or (
                    guessed.get("artist")
                    and not str(row["embedded_artist"] or "").strip()
                )
                or (
                    guessed.get("album")
                    and not str(row["embedded_album"] or "").strip()
                )
                or (
                    guessed.get("albumartist")
                    and not str(row["embedded_albumartist"] or "").strip()
                )
                or (
                    guessed.get("track_number")
                    and _track_number_missing(str(row["embedded_track_number"] or ""))
                )
            ):
                candidates.append(audio_path)

        if limit is not None and len(candidates) >= limit:
            break

    return candidates


def update_identify_audio_review_status(
    db_path: str | Path,
    audio_path: str | Path,
    *,
    status: str,
    root: str | Path | None = None,
) -> None:
    init_library_index(db_path)
    path = Path(audio_path).expanduser().resolve()
    updated_at = _utc_now()
    normalized_status = str(status or "").strip()

    with _connect(db_path) as conn:
        result = _execute_write(
            conn,
            """
            UPDATE library_files
            SET identify_audio_review_status = ?,
                updated_at = ?
            WHERE audio_path = ?
            """,
            (normalized_status, updated_at, str(path)),
        )
        if result.rowcount == 0:
            _upsert_audio_file_stub(
                conn,
                audio_path=path,
                root=root,
                indexed_at=updated_at,
            )
            _execute_write(
                conn,
                """
                UPDATE library_files
                SET identify_audio_review_status = ?,
                    updated_at = ?
                WHERE audio_path = ?
                """,
                (normalized_status, updated_at, str(path)),
            )
        conn.commit()


def record_musicbrainz_verification(
    db_path: str | Path,
    audio_path: str | Path,
    *,
    root: str | Path | None = None,
) -> None:
    init_library_index(db_path)
    path = Path(audio_path).expanduser().resolve()
    verified_at = _utc_now()

    with _connect(db_path) as conn:
        result = _execute_write(
            conn,
            """
            UPDATE library_files
            SET musicbrainz_verified_at = ?,
                updated_at = ?
            WHERE audio_path = ?
            """,
            (verified_at, verified_at, str(path)),
        )
        if result.rowcount == 0:
            _upsert_audio_file_stub(
                conn,
                audio_path=path,
                root=root,
                indexed_at=verified_at,
            )
            _execute_write(
                conn,
                """
                UPDATE library_files
                SET musicbrainz_verified_at = ?,
                    updated_at = ?
                WHERE audio_path = ?
                """,
                (verified_at, verified_at, str(path)),
            )
        conn.commit()


def count_indexed_audio_files(db_path: str | Path, root: str | Path) -> int:
    if not _library_index_exists(db_path):
        return 0
    root_path = Path(root).expanduser().resolve()
    with _connect_for_read(db_path) as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS total FROM library_files WHERE root_path = ? AND file_missing = 0",
            (str(root_path),),
        ).fetchone()
    return int(row["total"] if row is not None else 0)


def record_audio_health_result(
    db_path: str | Path,
    audio_path: str | Path,
    *,
    status: str,
    message: str = "",
    root: str | Path | None = None,
) -> None:
    init_library_index(db_path)
    path = Path(audio_path).expanduser().resolve()
    checked_at = _utc_now()
    with _connect(db_path) as conn:
        result = _execute_write(
            conn,
            """
            UPDATE library_files
            SET audio_health_status = ?,
                audio_health_message = ?,
                audio_health_checked_at = ?,
                updated_at = ?
            WHERE audio_path = ?
            """,
            (status, message, checked_at, checked_at, str(path)),
        )
        if result.rowcount == 0:
            _upsert_audio_file_stub(
                conn,
                audio_path=path,
                root=root,
                indexed_at=checked_at,
            )
            _execute_write(
                conn,
                """
                UPDATE library_files
                SET audio_health_status = ?,
                    audio_health_message = ?,
                    audio_health_checked_at = ?,
                    updated_at = ?
                WHERE audio_path = ?
                """,
                (status, message, checked_at, checked_at, str(path)),
            )
        conn.commit()


def record_library_tool_run(
    db_path: str | Path,
    *,
    tool_name: str,
    root: str | Path,
    run_mode: str,
    started_at: str,
    completed_at: str,
    scanned_count: int,
    changed_count: int,
    error_count: int,
    result: dict[str, Any],
) -> None:
    init_library_index(db_path)
    with _connect(db_path) as conn:
        _execute_write(
            conn,
            """
            INSERT INTO library_tool_runs (
                tool_name,
                root_path,
                run_mode,
                started_at,
                completed_at,
                scanned_count,
                changed_count,
                error_count,
                result_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                tool_name,
                str(Path(root).expanduser().resolve()),
                run_mode,
                started_at,
                completed_at,
                scanned_count,
                changed_count,
                error_count,
                json.dumps(result, ensure_ascii=False),
            ),
        )
        conn.commit()


def load_latest_library_tool_run(
    db_path: str | Path,
    *,
    tool_name: str,
    root: str | Path,
) -> dict[str, Any] | None:
    init_library_index(db_path)
    root_path = str(Path(root).expanduser().resolve())
    with _connect_for_read(db_path) as conn:
        row = conn.execute(
            "SELECT id, tool_name, root_path, run_mode, started_at, completed_at, "
            "scanned_count, changed_count, error_count, result_json "
            "FROM library_tool_runs WHERE tool_name = ? AND root_path = ? "
            "ORDER BY completed_at DESC, id DESC LIMIT 1",
            (str(tool_name or "").strip(), root_path),
        ).fetchone()
    if row is None:
        return None

    try:
        result = json.loads(row["result_json"] or "{}")
    except json.JSONDecodeError:
        result = {}

    return {
        "id": int(row["id"]),
        "tool_name": str(row["tool_name"] or ""),
        "root_path": str(row["root_path"] or ""),
        "run_mode": str(row["run_mode"] or ""),
        "started_at": str(row["started_at"] or ""),
        "completed_at": str(row["completed_at"] or ""),
        "scanned_count": int(row["scanned_count"] or 0),
        "changed_count": int(row["changed_count"] or 0),
        "error_count": int(row["error_count"] or 0),
        "result": result if isinstance(result, dict) else {},
    }


def load_library_tool_run(db_path: str | Path, run_id: int) -> dict[str, Any] | None:
    init_library_index(db_path)
    with _connect_for_read(db_path) as conn:
        row = conn.execute(
            "SELECT id, tool_name, root_path, run_mode, started_at, completed_at, "
            "scanned_count, changed_count, error_count, result_json "
            "FROM library_tool_runs WHERE id = ?",
            (int(run_id),),
        ).fetchone()
    if row is None:
        return None

    try:
        result = json.loads(row["result_json"] or "{}")
    except json.JSONDecodeError:
        result = {}

    return {
        "id": int(row["id"]),
        "tool_name": str(row["tool_name"] or ""),
        "root_path": str(row["root_path"] or ""),
        "run_mode": str(row["run_mode"] or ""),
        "started_at": str(row["started_at"] or ""),
        "completed_at": str(row["completed_at"] or ""),
        "scanned_count": int(row["scanned_count"] or 0),
        "changed_count": int(row["changed_count"] or 0),
        "error_count": int(row["error_count"] or 0),
        "result": result if isinstance(result, dict) else {},
    }


def update_library_tool_run_result(
    db_path: str | Path,
    *,
    run_id: int,
    result: dict[str, Any],
) -> None:
    init_library_index(db_path)
    with _connect(db_path) as conn:
        _execute_write(
            conn,
            "UPDATE library_tool_runs SET result_json = ? WHERE id = ?",
            (json.dumps(result, ensure_ascii=False), int(run_id)),
        )
        conn.commit()


def _iter_audio_files(
    root: Path,
    *,
    progress_callback: Callable[[str], None] | None = None,
):
    discovered = 0
    for current_root, dirnames, filenames in os.walk(root):
        dirnames.sort()
        filenames.sort()
        current_path = Path(current_root)
        relative_dir = (
            current_path.relative_to(root).as_posix() if current_path != root else "."
        )

        for filename in filenames:
            path = current_path / filename
            if not path.is_file() or path.suffix.lower() not in AUDIO_EXTENSIONS:
                continue
            discovered += 1
            if discovered == 1 or discovered % 250 == 0:
                _emit_progress(
                    progress_callback,
                    f"PROGRESS: discovered {discovered} audio file(s) so far under {relative_dir}",
                )
            yield path


def _summarize_xml_state(xml_path: Path) -> dict[str, Any]:
    state = _empty_xml_state(xml_path)
    if not xml_path.exists() or not xml_path.is_file():
        return state
    # Parse XML for fields
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
        def get_text(tag):
            el = root.find(tag)
            return el.text.strip() if el is not None and el.text else ""
        title = get_text("title")
        artist = get_text("performingartist") or get_text("artist")
        album = get_text("albumtitle") or get_text("album")
        provider = get_text("provider")
        downloaded_from = get_text("downloaded_from") or get_text("downloadedfrom") or "unknown"
        deezer_id = get_text("deezerid")
        musicbrainz_track_id = get_text("musicbrainztrackid")
    except Exception:
        title = artist = album = provider = downloaded_from = deezer_id = musicbrainz_track_id = ""
    state.update(
        {
            "xml_parse_ok": 1,
            "xml_has_title": int(bool(title)),
            "xml_has_artist": int(bool(artist)),
            "xml_has_album": int(bool(album)),
            "xml_has_downloaded_from": int(downloaded_from != "unknown"),
            "xml_has_deezer_id": int(bool(deezer_id)),
            "xml_has_musicbrainz_track_id": int(bool(musicbrainz_track_id)),
            "xml_core_complete": int(
                bool(title and artist and album and downloaded_from != "unknown")
            ),
            "title": title,
            "artist": artist,
            "album": album,
            "provider": provider,
            "downloaded_from": downloaded_from,
            "deezer_id": deezer_id,
            "musicbrainz_track_id": musicbrainz_track_id,
        }
    )
    return state


def _summarize_embedded_state(audio_path: Path) -> dict[str, Any]:
    metadata = load_embedded_audio_metadata(audio_path)
    return {
        "embedded_title": str(metadata.get("title") or "").strip(),
        "embedded_artist": str(metadata.get("artist") or "").strip(),
        "embedded_album": str(metadata.get("album") or "").strip(),
        "embedded_albumartist": str(metadata.get("albumartist") or "").strip(),
        "embedded_track_number": _normalize_track_number_text(
            metadata.get("track_number")
        ),
        "embedded_musicbrainz_album_id": str(
            metadata.get("musicbrainz_album_id") or ""
        ).strip(),
        "embedded_musicbrainz_artist_id": str(
            metadata.get("musicbrainz_artist_id") or ""
        ).strip(),
        "embedded_musicbrainz_albumartist_id": str(
            metadata.get("musicbrainz_albumartist_id") or ""
        ).strip(),
        "embedded_deezer_id": str(metadata.get("deezer_id") or "").strip(),
        "embedded_musicbrainz_track_id": str(
            metadata.get("musicbrainz_track_id") or ""
        ).strip(),
        "embedded_tags_checked_at": _utc_now(),
    }


def _paired_audio_path_for_xml(xml_path: Path) -> Path | None:
    for extension in AUDIO_EXTENSIONS:
        candidate = xml_path.with_suffix(extension)
        if candidate.exists() and candidate.is_file():
            return candidate
    return None


def _describe_incomplete_xml(row: sqlite3.Row) -> str:
    missing: list[str] = []
    if int(row["xml_has_title"] or 0) == 0:
        missing.append("title")
    if int(row["xml_has_artist"] or 0) == 0:
        missing.append("artist")
    if int(row["xml_has_album"] or 0) == 0:
        missing.append("album")
    if int(row["xml_has_downloaded_from"] or 0) == 0:
        missing.append("downloaded_from")
    if int(row["xml_has_musicbrainz_track_id"] or 0) == 0:
        missing.append("musicbrainztrackid")
    if (
        str(row["downloaded_from"] or "") == "deezer"
        and int(row["xml_has_deezer_id"] or 0) == 0
    ):
        missing.append("deezerid")
    return "Missing: " + ", ".join(missing)


def _expected_tag_targets(relative_path: str) -> dict[str, str] | None:
    parts = [
        part.strip() for part in str(relative_path or "").split("/") if part.strip()
    ]
    if len(parts) < 3:
        return None

    artist_dir = parts[0]
    stem = Path(parts[-1]).stem
    if _is_va_name(artist_dir):
        track_artist = _parse_artist_from_stem(stem)
        if not track_artist:
            return None
        return {
            "artist": track_artist,
            "albumartist": artist_dir,
        }

    return {
        "artist": artist_dir,
        "albumartist": artist_dir,
    }


def _is_va_name(folder_name: str) -> bool:
    return folder_name.strip().lower() in {
        "various artists",
        "various",
        "va",
        "v.a.",
        "v.a",
        "compilations",
        "compilation",
        "soundtracks",
        "soundtrack",
        "ost",
    }


def _parse_artist_from_stem(stem: str) -> str:
    parts = [part.strip() for part in str(stem or "").split(" - ") if part.strip()]
    if len(parts) < 2:
        return ""
    candidate = parts[0]
    return "" if candidate.isdigit() else candidate


def _scalar(conn: sqlite3.Connection, query: str, params: tuple[Any, ...]) -> int:
    row = conn.execute(query, params).fetchone()
    if row is None:
        return 0
    return int(row[0] or 0)


def _empty_xml_state(xml_path: Path) -> dict[str, Any]:
    return {
        "xml_parse_ok": 0,
        "xml_error": "",
        "xml_has_title": 0,
        "xml_has_artist": 0,
        "xml_has_album": 0,
        "xml_has_downloaded_from": 0,
        "xml_has_deezer_id": 0,
        "xml_has_musicbrainz_track_id": 0,
        "xml_core_complete": 0,
        "title": "",
        "artist": "",
        "album": "",
        "provider": "",
        "downloaded_from": "unknown",
        "deezer_id": "",
        "musicbrainz_track_id": "",
    }


def _empty_embedded_state() -> dict[str, Any]:
    return {
        "embedded_title": "",
        "embedded_artist": "",
        "embedded_album": "",
        "embedded_albumartist": "",
        "embedded_track_number": "",
        "embedded_musicbrainz_album_id": "",
        "embedded_musicbrainz_artist_id": "",
        "embedded_musicbrainz_albumartist_id": "",
        "embedded_deezer_id": "",
        "embedded_musicbrainz_track_id": "",
        "embedded_tags_checked_at": "",
    }


def _ensure_library_files_column(
    conn: sqlite3.Connection,
    column_name: str,
    definition: str,
) -> None:
    existing_columns = {
        str(row[1])
        for row in conn.execute("PRAGMA table_info(library_files)").fetchall()
    }
    if column_name in existing_columns:
        return
    _execute_write(
        conn, f"ALTER TABLE library_files ADD COLUMN {column_name} {definition}"
    )


def _delete_library_file_row(
    conn: sqlite3.Connection,
    *,
    root_path: Path,
    relative_path: str,
) -> None:
    _execute_write(
        conn,
        "DELETE FROM library_files WHERE root_path = ? AND relative_path = ?",
        (str(root_path), relative_path),
    )


def _normalize_track_number_text(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    primary = text.split("/", 1)[0].strip()
    if not primary or primary == "0":
        return ""
    return primary


def _track_number_missing(value: str) -> bool:
    normalized = _normalize_track_number_text(value)
    return normalized == ""


def _normalize_selected_audio_paths(
    root_path: Path, audio_paths: list[str | Path]
) -> list[Path]:
    normalized: list[Path] = []
    seen: set[str] = set()
    for value in audio_paths:
        path = Path(value).expanduser().resolve()
        try:
            relative = path.relative_to(root_path).as_posix()
        except ValueError:
            continue
        if relative in seen or path.suffix.lower() not in AUDIO_EXTENSIONS:
            continue
        seen.add(relative)
        normalized.append(path)
    normalized.sort(key=lambda item: item.relative_to(root_path).as_posix())
    return normalized


def _upsert_audio_file_stub(
    conn: sqlite3.Connection,
    *,
    audio_path: Path,
    root: str | Path | None,
    indexed_at: str,
) -> None:
    resolved_root = _resolve_root_path(audio_path, root)
    relative_path = audio_path.relative_to(resolved_root).as_posix()
    stat = audio_path.stat()
    xml_path = audio_path.with_suffix(".xml")
    xml_exists = xml_path.exists()

    _execute_write(
        conn,
        """
        INSERT INTO library_files (
            root_path,
            relative_path,
            audio_path,
            extension,
            size_bytes,
            modified_at,
            discovered_at,
            last_indexed_at,
            file_missing,
            xml_path,
            xml_exists,
            xml_modified_at,
            musicbrainz_verified_at,
            created_at,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(root_path, relative_path) DO UPDATE SET
            audio_path = excluded.audio_path,
            extension = excluded.extension,
            size_bytes = excluded.size_bytes,
            modified_at = excluded.modified_at,
            last_indexed_at = excluded.last_indexed_at,
            file_missing = excluded.file_missing,
            xml_path = excluded.xml_path,
            xml_exists = excluded.xml_exists,
            xml_modified_at = excluded.xml_modified_at,
            musicbrainz_verified_at = library_files.musicbrainz_verified_at,
            updated_at = excluded.updated_at
        """,
        (
            str(resolved_root),
            relative_path,
            str(audio_path),
            audio_path.suffix.lower(),
            int(stat.st_size),
            _iso_from_timestamp(stat.st_mtime),
            indexed_at,
            indexed_at,
            0,
            str(xml_path),
            int(xml_exists),
            _iso_from_timestamp(xml_path.stat().st_mtime) if xml_exists else "",
            "",
            indexed_at,
            indexed_at,
        ),
    )


def _resolve_root_path(audio_path: Path, root: str | Path | None) -> Path:
    if root:
        root_path = Path(root).expanduser().resolve()
        if audio_path.is_relative_to(root_path):
            return root_path
    return audio_path.parent


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _iso_from_timestamp(value: float) -> str:
    return datetime.fromtimestamp(value, tz=UTC).isoformat()


def _connect(db_path: str | Path) -> sqlite3.Connection:
    last_error: sqlite3.OperationalError | None = None

    for attempt in range(_DB_CONNECT_RETRIES):
        try:
            conn = sqlite3.connect(str(db_path), timeout=_DB_TIMEOUT_SECONDS)
            conn.row_factory = sqlite3.Row
            conn.execute(f"PRAGMA busy_timeout = {_DB_BUSY_TIMEOUT_MS}")
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = NORMAL")
            return conn
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower() or attempt == _DB_CONNECT_RETRIES - 1:
                raise
            last_error = exc
            time.sleep(_DB_RETRY_DELAY_SECONDS)

    if last_error is not None:
        raise last_error
    raise sqlite3.OperationalError("Could not connect to the library index database.")


def _connect_for_read(db_path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), timeout=_DB_TIMEOUT_SECONDS)
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout = {_DB_BUSY_TIMEOUT_MS}")
    return conn


def _execute_write(
    conn: sqlite3.Connection,
    sql: str,
    params: tuple[Any, ...] = (),
) -> sqlite3.Cursor:
    last_error: sqlite3.OperationalError | None = None
    for attempt in range(_DB_CONNECT_RETRIES):
        try:
            return conn.execute(sql, params)
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower() or attempt == _DB_CONNECT_RETRIES - 1:
                raise
            last_error = exc
            time.sleep(_DB_RETRY_DELAY_SECONDS)
    if last_error is not None:
        raise last_error
    raise sqlite3.OperationalError("Could not execute SQLite write statement.")


def _library_index_exists(db_path: str | Path) -> bool:
    path = Path(db_path)
    return path.exists() and path.is_file() and path.stat().st_size > 0
