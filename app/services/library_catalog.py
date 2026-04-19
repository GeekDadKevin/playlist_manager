from __future__ import annotations

import importlib.util
import json
import shutil
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app.models import PlaylistTrack
from app.services.deezer_download import DeezerDownloadService
from app.services.library_index import (
    init_library_index,
    refresh_library_index_for_paths,
)
from app.services.song_metadata import (
    load_embedded_audio_metadata,
    load_song_metadata_xml,
)
from app.services.tool_output import emit_console_line

CATALOG_FILTERS: list[tuple[str, str]] = [
    ("any-anomaly", "Any Anomaly"),
    ("missing-info", "Missing Info"),
    ("musicbrainz-pending", "Needs MB Verify"),
    ("accepted-as-is", "Accepted As Is"),
    ("integrity-failures", "Integrity Failures"),
    ("missing-xml", "Missing XML"),
    ("incomplete-xml", "Incomplete XML"),
    ("non-deezer-source", "Non-Deezer Source"),
    ("unchecked-audio", "Unchecked Audio"),
    ("all", "All Tracks"),
]
CATALOG_PAGE_SIZE = 25
_BATCH_ACTIONS: dict[str, str] = {
    "check-audio": "Check Audio Integrity",
    "identify-structure": "Identify By Structure",
    "fix-tags": "Fix Audio Tags",
    "identify-audio": "Identify Tracks By Audio",
    "sync-xml": "Sync XML Sidecars",
    "redownload": "Redownload from Deezer",
}
_SORT_COLUMNS = {
    "path": "relative_path",
    "artist": "embedded_artist",
    "album": "embedded_album",
    "source": "downloaded_from",
    "integrity": "audio_health_status",
    "updated": "updated_at",
}
_MISSING_INFO_SQL = (
    "("
    + " OR ".join(
        [
            "embedded_title = ''",
            "embedded_artist = ''",
            "embedded_album = ''",
            "embedded_albumartist = ''",
            "embedded_track_number = ''",
            "embedded_musicbrainz_album_id = ''",
            "embedded_musicbrainz_artist_id = ''",
            "embedded_musicbrainz_albumartist_id = ''",
            "embedded_musicbrainz_track_id = ''",
        ]
    )
    + ")"
)
_INCOMPLETE_XML_SQL = (
    "xml_exists = 1 AND (xml_has_title = 0 OR xml_has_artist = 0 OR xml_has_album = 0 OR "
    "xml_has_downloaded_from = 0 OR xml_has_musicbrainz_track_id = 0 OR "
    "(downloaded_from = 'deezer' AND xml_has_deezer_id = 0))"
)
_UNCHECKED_AUDIO_SQL = (
    "(audio_health_checked_at = '' OR audio_health_checked_at < modified_at)"
)
_MUSICBRAINZ_PENDING_SQL = (
    "(identify_audio_review_status != 'accepted-as-is' AND "
    "(musicbrainz_verified_at = '' OR musicbrainz_verified_at < modified_at))"
)
_ACCEPTED_AS_IS_SQL = "(identify_audio_review_status = 'accepted-as-is')"
_ANY_ANOMALY_SQL = (
    "("
    + " OR ".join(
        [
            _MISSING_INFO_SQL,
            _MUSICBRAINZ_PENDING_SQL,
            _ACCEPTED_AS_IS_SQL,
            "audio_health_status IN ('warning', 'error')",
            "xml_exists = 0",
            _INCOMPLETE_XML_SQL,
            "downloaded_from NOT IN ('', 'unknown', 'deezer')",
            _UNCHECKED_AUDIO_SQL,
        ]
    )
    + ")"
)


def catalog_batch_actions() -> list[tuple[str, str]]:
    return list(_BATCH_ACTIONS.items())


def catalog_batch_action_label(action: str) -> str:
    return _BATCH_ACTIONS.get(str(action or "").strip().lower(), "Catalog Batch")


def catalog_filter_counts(db_path: str | Path, root: str | Path) -> dict[str, int]:
    init_library_index(db_path)
    root_path = Path(root).expanduser().resolve()
    if not root_path.is_dir():
        return {filter_id: 0 for filter_id, _label in CATALOG_FILTERS}

    counts: dict[str, int] = {}
    with _connect_read(db_path) as conn:
        for filter_id, _label in CATALOG_FILTERS:
            where_sql, params = _filter_where_sql(filter_id)
            row = conn.execute(
                "SELECT COUNT(*) AS total FROM library_files "
                "WHERE root_path = ? AND file_missing = 0 AND "
                f"{where_sql}",
                (str(root_path), *params),
            ).fetchone()
            counts[filter_id] = int(row["total"] if row is not None else 0)
    return counts


def list_catalog_tracks(
    db_path: str | Path,
    root: str | Path,
    *,
    issue_filter: str,
    search: str = "",
    sort_by: str = "path",
    sort_dir: str = "asc",
    page: int = 1,
    per_page: int = CATALOG_PAGE_SIZE,
) -> dict[str, Any]:
    init_library_index(db_path)
    root_path = Path(root).expanduser().resolve()
    normalized_filter = _normalize_filter(issue_filter)
    normalized_sort = sort_by if sort_by in _SORT_COLUMNS else "path"
    normalized_dir = "desc" if str(sort_dir).strip().lower() == "desc" else "asc"
    page = max(int(page or 1), 1)
    per_page = CATALOG_PAGE_SIZE
    search_text = str(search or "").strip()

    if not root_path.is_dir():
        return {
            "items": [],
            "page": 1,
            "pages": 1,
            "per_page": per_page,
            "total": 0,
            "sort_by": normalized_sort,
            "sort_dir": normalized_dir,
            "filter": normalized_filter,
            "search": search_text,
        }

    where_sql, filter_params = _filter_where_sql(normalized_filter)
    params: list[Any] = [str(root_path), *filter_params]
    search_sql = ""
    if search_text:
        wildcard = f"%{search_text.lower()}%"
        search_sql = (
            " AND (lower(relative_path) LIKE ? OR lower(embedded_title) LIKE ? OR "
            "lower(embedded_artist) LIKE ? OR lower(embedded_album) LIKE ? OR "
            "lower(downloaded_from) LIKE ?)"
        )
        params.extend([wildcard, wildcard, wildcard, wildcard, wildcard])

    sort_column = _SORT_COLUMNS[normalized_sort]
    offset = (page - 1) * per_page
    with _connect_read(db_path) as conn:
        count_row = conn.execute(
            "SELECT COUNT(*) AS total FROM library_files WHERE root_path = ? AND file_missing = 0 "
            f"AND {where_sql}{search_sql}",
            tuple(params),
        ).fetchone()
        total = int(count_row["total"] if count_row is not None else 0)
        pages = max((total + per_page - 1) // per_page, 1)
        if page > pages:
            page = pages
            offset = (page - 1) * per_page
        rows = conn.execute(
            "SELECT relative_path, audio_path, xml_path, xml_exists, "
            "xml_has_title, xml_has_artist, xml_has_album, "
            "xml_has_downloaded_from, xml_has_deezer_id, "
            "xml_has_musicbrainz_track_id, downloaded_from, provider, "
            "identify_audio_review_status, musicbrainz_verified_at, "
            "audio_health_status, audio_health_message, "
            "audio_health_checked_at, modified_at, updated_at, "
            "embedded_title, embedded_artist, embedded_album, "
            "embedded_albumartist, embedded_track_number, "
            "embedded_musicbrainz_album_id, embedded_musicbrainz_artist_id, "
            "embedded_musicbrainz_albumartist_id, embedded_musicbrainz_track_id "
            "FROM library_files WHERE root_path = ? AND file_missing = 0 "
            f"AND {where_sql}{search_sql} "
            f"ORDER BY {sort_column} {normalized_dir.upper()}, relative_path ASC "
            "LIMIT ? OFFSET ?",
            (*params, per_page, offset),
        ).fetchall()

    return {
        "items": [_catalog_row_to_item(row) for row in rows],
        "page": page,
        "pages": pages,
        "per_page": per_page,
        "total": total,
        "sort_by": normalized_sort,
        "sort_dir": normalized_dir,
        "filter": normalized_filter,
        "search": search_text,
    }


def load_last_catalog_batch_result(data_dir: str | Path) -> dict[str, Any] | None:
    path = Path(data_dir) / "catalog_batch_last_run.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def run_catalog_batch_action(
    config: dict[str, Any],
    *,
    action: str,
    relative_paths: list[str],
    dry_run: bool = False,
) -> dict[str, Any]:
    action = str(action or "").strip().lower()
    if action not in _BATCH_ACTIONS:
        raise ValueError("Unknown batch action.")

    music_root = (
        Path(str(config.get("NAVIDROME_MUSIC_ROOT", "")).strip()).expanduser().resolve()
    )
    if not music_root.is_dir():
        raise ValueError("NAVIDROME_MUSIC_ROOT is not configured correctly.")

    selected_paths = _resolve_relative_audio_paths(music_root, relative_paths)
    if not selected_paths:
        raise ValueError("Choose at least one track first.")
    if len(selected_paths) > CATALOG_PAGE_SIZE:
        raise ValueError(
            f"Batch actions are limited to {CATALOG_PAGE_SIZE} tracks at a time right now."
        )

    result: dict[str, Any]
    if action == "redownload":
        result = _redownload_selected_tracks(
            config, music_root, selected_paths, dry_run=dry_run
        )
    else:
        module_name, func_name, kwargs = _batch_tool_target(action, selected_paths)
        module = _load_script_module(module_name)
        lines, exit_code = getattr(module, func_name)(
            music_root,
            dry_run=dry_run,
            db_path=config.get("LIBRARY_INDEX_DB_PATH"),
            **kwargs,
        )
        result = {
            "action": action,
            "label": _BATCH_ACTIONS[action],
            "selected_count": len(selected_paths),
            "exit_code": exit_code if isinstance(exit_code, int) else 0,
            "summary_line": _summary_line(lines),
            "lines": lines[-160:],
            "completed_at": datetime.now(UTC).isoformat(),
        }

    if not dry_run:
        refresh_library_index_for_paths(
            config["LIBRARY_INDEX_DB_PATH"],
            music_root,
            selected_paths,
        )

    _save_last_catalog_batch_result(config.get("DATA_DIR", music_root), result)
    return result


def _batch_tool_target(
    action: str, selected_paths: list[Path]
) -> tuple[str, str, dict[str, Any]]:
    if action == "check-audio":
        return (
            "check_audio_health.py",
            "check_library",
            {"selected_paths": selected_paths},
        )
    if action == "fix-tags":
        return "fix_audio_tags.py", "fix_tags", {"selected_paths": selected_paths}
    if action == "identify-structure":
        return (
            "identify_tracks_from_layout.py",
            "identify_tracks_from_layout",
            {"selected_paths": selected_paths},
        )
    if action == "identify-audio":
        return (
            "identify_tracks_by_audio.py",
            "identify_tracks_by_audio",
            {"selected_paths": selected_paths},
        )
    if action == "sync-xml":
        return (
            "rebuild_song_xml.py",
            "rebuild",
            {"selected_audio_paths": selected_paths},
        )
    raise ValueError("Unknown batch action.")


def _redownload_selected_tracks(
    config: dict[str, Any],
    root: Path,
    selected_paths: list[Path],
    *,
    dry_run: bool,
) -> dict[str, Any]:
    service = DeezerDownloadService.from_config(config)
    if not service.is_configured():
        raise ValueError("Configure Deezer before using redownload.")

    lines: list[str] = []
    downloaded = 0
    failed = 0
    unresolved = 0

    def emit(line: str) -> None:
        emit_console_line(line)
        lines.append(line)

    for index, audio_path in enumerate(selected_paths, start=1):
        relative = audio_path.relative_to(root)
        emit(f"REDOWNLOAD: {index}/{len(selected_paths)}  {relative}")
        embedded = load_embedded_audio_metadata(audio_path)
        xml_data = load_song_metadata_xml(audio_path.with_suffix(".xml"))
        track = PlaylistTrack(
            title=str(
                embedded.get("title") or xml_data.get("title") or audio_path.stem
            ).strip(),
            artist=str(
                embedded.get("artist")
                or embedded.get("albumartist")
                or xml_data.get("performingartist")
                or xml_data.get("artist")
                or ""
            ).strip(),
            album=str(
                embedded.get("album") or xml_data.get("albumtitle") or ""
            ).strip(),
            source=str(audio_path),
        )
        if embedded.get("track_number"):
            try:
                track.track_number = int(
                    str(embedded.get("track_number") or "").split("/", 1)[0]
                )
            except ValueError:
                track.track_number = None

        if not track.title or not track.artist:
            unresolved += 1
            emit(f"WARN: missing title/artist metadata for {relative}")
            continue

        preferred_deezer_id = str(
            xml_data.get("deezerid") or embedded.get("deezer_id") or ""
        ).strip()
        try:
            candidates = service.search_track(track, limit=8)
        except Exception as exc:
            failed += 1
            emit(f"ERROR: search failed for {relative}  [{exc}]")
            continue

        match = None
        if preferred_deezer_id:
            match = next(
                (
                    candidate
                    for candidate in candidates
                    if str(candidate.get("deezer_id") or "").strip()
                    == preferred_deezer_id
                ),
                None,
            )
        if match is None:
            match = next(
                (
                    candidate
                    for candidate in candidates
                    if candidate.get("provider", "deezer") == "deezer"
                    and candidate.get("accepted", False)
                ),
                None,
            )
        if match is None:
            unresolved += 1
            emit(f"WARN: no accepted Deezer match for {relative}")
            continue

        if dry_run:
            emit(
                f"[DRY-RUN] would redownload {relative} from Deezer ID {match.get('deezer_id')}"
            )
            downloaded += 1
            continue

        try:
            downloaded_path, metadata_path = service._download_track(
                match.get("deezer_id"),
                match,
                track,
            )
            if downloaded_path != audio_path:
                if audio_path.exists():
                    audio_path.unlink()
                audio_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(downloaded_path), str(audio_path))
            target_xml = audio_path.with_suffix(".xml")
            if metadata_path != target_xml and metadata_path.exists():
                if target_xml.exists():
                    target_xml.unlink()
                shutil.move(str(metadata_path), str(target_xml))
        except Exception as exc:
            failed += 1
            emit(f"ERROR: redownload failed for {relative}  [{exc}]")
            continue

        downloaded += 1
        emit(f"UPDATED: {relative}  [deezer_id={match.get('deezer_id')}]")

    emit(
        f"SUMMARY  selected={len(selected_paths)}  downloaded={downloaded}  "
        f"unresolved={unresolved}  failed={failed}  dry_run={str(dry_run).lower()}"
    )
    return {
        "action": "redownload",
        "label": _BATCH_ACTIONS["redownload"],
        "selected_count": len(selected_paths),
        "exit_code": 0 if failed == 0 else 1,
        "summary_line": _summary_line(lines),
        "lines": lines[-160:],
        "completed_at": datetime.now(UTC).isoformat(),
    }


def _catalog_row_to_item(row: sqlite3.Row) -> dict[str, Any]:
    anomalies = _row_anomalies(row)
    musicbrainz_status = _musicbrainz_status(row)
    return {
        "relative_path": str(row["relative_path"] or ""),
        "audio_path": str(row["audio_path"] or ""),
        "title": str(row["embedded_title"] or ""),
        "artist": str(row["embedded_artist"] or ""),
        "album": str(row["embedded_album"] or ""),
        "albumartist": str(row["embedded_albumartist"] or ""),
        "track_number": str(row["embedded_track_number"] or ""),
        "source": str(row["downloaded_from"] or "unknown"),
        "provider": str(row["provider"] or ""),
        "integrity_status": str(row["audio_health_status"] or "unchecked"),
        "integrity_message": str(row["audio_health_message"] or ""),
        "musicbrainz_status": str(musicbrainz_status["status"]),
        "musicbrainz_label": str(musicbrainz_status["label"]),
        "musicbrainz_tone": str(musicbrainz_status["tone"]),
        "musicbrainz_detail": str(musicbrainz_status["detail"]),
        "xml_exists": bool(int(row["xml_exists"] or 0)),
        "updated_at": str(row["updated_at"] or ""),
        "modified_at": str(row["modified_at"] or ""),
        "anomalies": anomalies,
    }


def _row_anomalies(row: sqlite3.Row) -> list[dict[str, str]]:
    anomalies: list[dict[str, str]] = []
    missing_fields = _missing_info_fields(row)
    if missing_fields:
        anomalies.append(
            {
                "label": "Missing Info",
                "tone": "warning",
                "detail": ", ".join(missing_fields[:5]),
            }
        )
    musicbrainz_status = _musicbrainz_status(row)
    if musicbrainz_status["status"] in {"unverified", "stale"}:
        anomalies.append(
            {
                "label": str(musicbrainz_status["label"]),
                "tone": str(musicbrainz_status["tone"]),
                "detail": str(musicbrainz_status["detail"]),
            }
        )
    integrity = str(row["audio_health_status"] or "").strip().lower()
    if integrity in {"warning", "error"}:
        anomalies.append(
            {
                "label": "Integrity",
                "tone": "error" if integrity == "error" else "warning",
                "detail": str(row["audio_health_message"] or integrity),
            }
        )
    if int(row["xml_exists"] or 0) == 0:
        anomalies.append(
            {"label": "Missing XML", "tone": "muted", "detail": "No sidecar XML"}
        )
    elif _row_has_incomplete_xml(row):
        anomalies.append(
            {
                "label": "Incomplete XML",
                "tone": "warning",
                "detail": "Sidecar is missing expected metadata fields",
            }
        )
    downloaded_from = str(row["downloaded_from"] or "").strip().lower()
    if downloaded_from not in {"", "unknown", "deezer"}:
        anomalies.append(
            {
                "label": "Non-Deezer",
                "tone": "muted",
                "detail": str(row["downloaded_from"] or "unknown"),
            }
        )
    if (
        not str(row["audio_health_checked_at"] or "").strip()
        or str(row["audio_health_checked_at"] or "").strip()
        < str(row["modified_at"] or "").strip()
    ):
        anomalies.append(
            {
                "label": "Unchecked",
                "tone": "muted",
                "detail": "Integrity scan is missing or stale",
            }
        )
    return anomalies


def _row_has_incomplete_xml(row: sqlite3.Row) -> bool:
    return (
        int(row["xml_has_title"] or 0) == 0
        or int(row["xml_has_artist"] or 0) == 0
        or int(row["xml_has_album"] or 0) == 0
        or int(row["xml_has_downloaded_from"] or 0) == 0
        or int(row["xml_has_musicbrainz_track_id"] or 0) == 0
        or (
            str(row["downloaded_from"] or "").strip().lower() == "deezer"
            and int(row["xml_has_deezer_id"] or 0) == 0
        )
    )


def _musicbrainz_status(row: sqlite3.Row) -> dict[str, str]:
    review_status = str(row["identify_audio_review_status"] or "").strip().lower()
    if review_status == "accepted-as-is":
        return {
            "status": "accepted-as-is",
            "label": "Accepted As Is",
            "tone": "muted",
            "detail": "Skipped automatic retagging during fingerprint review.",
        }

    verified_at = str(row["musicbrainz_verified_at"] or "").strip()
    modified_at = str(row["modified_at"] or "").strip()
    if not verified_at:
        return {
            "status": "unverified",
            "label": "MB Unverified",
            "tone": "warning",
            "detail": "Not yet confirmed against MusicBrainz.",
        }
    if verified_at < modified_at:
        return {
            "status": "stale",
            "label": "MB Stale",
            "tone": "warning",
            "detail": "File changed after the last MusicBrainz verification.",
        }
    return {
        "status": "verified",
        "label": "MB Verified",
        "tone": "ok",
        "detail": f"Verified {verified_at}",
    }


def _missing_info_fields(row: sqlite3.Row) -> list[str]:
    fields: list[str] = []
    for key, label in (
        ("embedded_title", "title"),
        ("embedded_artist", "artist"),
        ("embedded_album", "album"),
        ("embedded_albumartist", "album artist"),
        ("embedded_track_number", "track #"),
        ("embedded_musicbrainz_album_id", "MB album"),
        ("embedded_musicbrainz_artist_id", "MB artist"),
        ("embedded_musicbrainz_albumartist_id", "MB album artist"),
        ("embedded_musicbrainz_track_id", "MB track"),
    ):
        if not str(row[key] or "").strip():
            fields.append(label)
    return fields


def _filter_where_sql(filter_id: str) -> tuple[str, tuple[Any, ...]]:
    normalized = _normalize_filter(filter_id)
    if normalized == "missing-info":
        return _MISSING_INFO_SQL, ()
    if normalized == "musicbrainz-pending":
        return _MUSICBRAINZ_PENDING_SQL, ()
    if normalized == "accepted-as-is":
        return _ACCEPTED_AS_IS_SQL, ()
    if normalized == "integrity-failures":
        return "audio_health_status IN ('warning', 'error')", ()
    if normalized == "missing-xml":
        return "xml_exists = 0", ()
    if normalized == "incomplete-xml":
        return _INCOMPLETE_XML_SQL, ()
    if normalized == "non-deezer-source":
        return "downloaded_from NOT IN ('', 'unknown', 'deezer')", ()
    if normalized == "unchecked-audio":
        return _UNCHECKED_AUDIO_SQL, ()
    if normalized == "all":
        return "1 = 1", ()
    return _ANY_ANOMALY_SQL, ()


def _normalize_filter(filter_id: str) -> str:
    known = {item[0] for item in CATALOG_FILTERS}
    normalized = str(filter_id or "any-anomaly").strip().lower()
    return normalized if normalized in known else "any-anomaly"


def _connect_read(db_path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _resolve_relative_audio_paths(root: Path, relative_paths: list[str]) -> list[Path]:
    resolved: list[Path] = []
    seen: set[str] = set()
    for value in relative_paths:
        rel = str(value or "").strip().replace("\\", "/")
        if not rel or rel in seen:
            continue
        candidate = (root / rel).resolve()
        try:
            candidate.relative_to(root)
        except ValueError:
            continue
        if candidate.is_file():
            resolved.append(candidate)
            seen.add(rel)
    return resolved


def _summary_line(lines: list[str]) -> str:
    for line in reversed(lines):
        if line.lower().startswith("summary"):
            return line
    return lines[-1] if lines else ""


def _save_last_catalog_batch_result(
    data_dir: str | Path, result: dict[str, Any]
) -> None:
    path = Path(data_dir) / "catalog_batch_last_run.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")


def _load_script_module(script_name: str):
    scripts_dir = Path(__file__).resolve().parent.parent.parent / "scripts"
    module_path = scripts_dir / script_name
    spec = importlib.util.spec_from_file_location(
        f"catalog_batch_{module_path.stem}", module_path
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load {script_name}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
