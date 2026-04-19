"""Set preliminary embedded tags from directory structure and filename layout.

This pass is intentionally heuristic. It uses the folder structure and file name to
fill in missing title, artist, album, albumartist, and track number tags before the
more authoritative Fix Audio Tags and MusicBrainz enrichment passes run.

Run:
    uv run python scripts/identify_tracks_from_layout.py [MUSIC_ROOT] [--dry-run] [--limit N]

MUSIC_ROOT defaults to NAVIDROME_MUSIC_ROOT from .env.
A timestamped log is written to MUSIC_ROOT/identify_tracks_from_layout_<timestamp>.log
(or cwd if the root is not writable).
"""

from __future__ import annotations

import argparse
import datetime
import os
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=False)

from app.services.library_index import (  # noqa: E402
    list_structure_tag_candidates,  # Only queries the DB, never scans filesystem
    record_library_tool_run,
    refresh_library_index_for_paths,
)
from app.services.musicbrainz_tag_writer import write_musicbrainz_tags  # noqa: E402
from app.services.song_metadata import (  # noqa: E402
    guess_preliminary_metadata,
    load_embedded_audio_metadata,
)
from app.services.tool_output import emit_console_line  # noqa: E402


def _emit(line: str, lines: list[str]) -> None:
    emit_console_line(line)
    lines.append(line)


def _writer_details(
    metadata: dict[str, Any],
    guessed: dict[str, str],
    *,
    full_scan: bool,
) -> dict[str, Any]:
    def _value(key: str, guessed_key: str | None = None) -> str:
        source_key = guessed_key or key
        current = str(metadata.get(key) or "").strip()
        guessed_value = str(guessed.get(source_key) or "").strip()
        if full_scan and guessed_value:
            return guessed_value
        return current or guessed_value

    return {
        "title": _value("title"),
        "artist": _value("artist"),
        "album": _value("album"),
        "albumartist": _value("albumartist"),
        "track_number": _value("track_number"),
        "track_total": str(metadata.get("track_total") or "").strip(),
        "disc_number": str(metadata.get("disc_number") or "").strip(),
        "disc_total": str(metadata.get("disc_total") or "").strip(),
        "artist_sort": str(metadata.get("artist_sort") or "").strip(),
        "albumartist_sort": str(metadata.get("albumartist_sort") or "").strip(),
        "date": str(metadata.get("date") or "").strip(),
        "original_date": str(metadata.get("original_date") or "").strip(),
        "genre": str(metadata.get("genre") or "").strip(),
        "isrc": str(metadata.get("isrc") or "").strip(),
        "barcode": str(metadata.get("barcode") or "").strip(),
        "label": str(metadata.get("label") or "").strip(),
        "catalog_number": str(metadata.get("catalog_number") or "").strip(),
        "media_format": str(metadata.get("media_format") or "").strip(),
        "release_country": str(metadata.get("release_country") or "").strip(),
        "release_status": str(metadata.get("release_status") or "").strip(),
        "release_type": str(metadata.get("release_type") or "").strip(),
        "release_secondary_types": str(
            metadata.get("release_secondary_types") or ""
        ).strip(),
        "language": str(metadata.get("language") or "").strip(),
        "script": str(metadata.get("script") or "").strip(),
        "recording_disambiguation": str(
            metadata.get("recording_disambiguation") or ""
        ).strip(),
        "album_disambiguation": str(metadata.get("album_disambiguation") or "").strip(),
        "recording_mbid": str(metadata.get("musicbrainz_track_id") or "").strip(),
        "release_mbid": str(metadata.get("musicbrainz_album_id") or "").strip(),
        "release_group_mbid": str(
            metadata.get("musicbrainz_release_group_id") or ""
        ).strip(),
        "artist_mbid": str(metadata.get("musicbrainz_artist_id") or "").strip(),
        "albumartist_mbid": str(
            metadata.get("musicbrainz_albumartist_id") or ""
        ).strip(),
    }


def _planned_changes(
    current: dict[str, Any],
    guessed: dict[str, str],
    *,
    full_scan: bool,
) -> dict[str, str]:
    changes: dict[str, str] = {}
    field_map = {
        "title": "title",
        "artist": "artist",
        "album": "album",
        "albumartist": "albumartist",
        "track_number": "track_number",
    }
    for metadata_key, guessed_key in field_map.items():
        guessed_value = str(guessed.get(guessed_key) or "").strip()
        if not guessed_value:
            continue
        current_value = str(current.get(metadata_key) or "").strip()
        if metadata_key == "track_number":
            current_value = current_value.split("/", 1)[0].strip()
            if current_value == "0":
                current_value = ""
        if full_scan:
            if current_value != guessed_value:
                changes[metadata_key] = guessed_value
        elif not current_value:
            changes[metadata_key] = guessed_value
    return changes


def identify_tracks_from_layout(
    root: Path,
    *,
    dry_run: bool = False,
    limit: int | None = None,
    full_scan: bool = False,
    db_path: str | Path | None = None,
    selected_paths: list[Path] | None = None,
) -> tuple[list[str], int]:
    lines: list[str] = []
    started_at = datetime.datetime.now(datetime.UTC).isoformat()
    started = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    library_index_db = str(
        db_path
        or os.getenv(
            "LIBRARY_INDEX_DB_PATH",
            Path(__file__).resolve().parent.parent / "data" / "library_index.db",
        )
    )

    _emit(
        "identify_tracks_from_layout  "
        f"root={root}  dry_run={dry_run}  limit={limit}  full_scan={full_scan}  started={started}",
        lines,
    )
    _emit("=" * 72, lines)

    if selected_paths is not None:
        inventory_summary = None
        candidates = (
            selected_paths[:limit] if limit is not None else list(selected_paths)
        )
        _emit(f"PROGRESS: using explicit selection of {len(candidates)} audio file(s) for structure tagging.", lines)
    else:
        inventory_summary = None
        _emit(f"PROGRESS: querying DB for structure-tag candidates...", lines)
        candidates = list_structure_tag_candidates(
            library_index_db,
            root,
            force_full=full_scan,
            limit=limit if full_scan else None,
        )
        _emit(f"PROGRESS: selected {len(candidates)} candidate file(s) for structure tagging (from DB).", lines)

    total = len(candidates)
    _emit(f"PROGRESS: found {total} candidate audio file(s) to check (from DB)", lines)

    scanned = 0
    updated = 0
    unchanged = 0
    failed = 0
    written_paths: list[Path] = []

    for index, audio_path in enumerate(candidates, start=1):
        scanned += 1
        relative = audio_path.relative_to(root)
        _emit(f"PROGRESS: checking file {index}/{total}: {relative}", lines)
        current = load_embedded_audio_metadata(audio_path)
        guessed = guess_preliminary_metadata(audio_path, root=root)
        changes = _planned_changes(current, guessed, full_scan=full_scan)
        if not changes:
            _emit(f"PROGRESS: no changes needed for {relative} ({index}/{total})", lines)
            unchanged += 1
            continue

        planned = ", ".join(f"{key}={value!r}" for key, value in changes.items())
        if dry_run:
            _emit(f"PROGRESS: [DRY-RUN] would set {relative}  [{planned}] ({index}/{total})", lines)
            updated += 1
            continue

        _emit(f"PROGRESS: writing tags for {relative} ({index}/{total})", lines)
        try:
            write_musicbrainz_tags(
                audio_path,
                _writer_details(current, guessed, full_scan=full_scan),
            )
        except Exception as exc:
            _emit(f"PROGRESS: ERROR writing {relative}: {exc} ({index}/{total})", lines)
            failed += 1
            continue

        written_paths.append(audio_path)
        updated += 1
        _emit(f"PROGRESS: tagged {relative} [{planned}] ({index}/{total})", lines)

    if not dry_run and written_paths:
        refresh_library_index_for_paths(
            library_index_db,
            root,
            written_paths,
            scan_xml_sidecars=False,
        )

    _emit("", lines)
    _emit("=" * 72, lines)
    _emit(
        f"SUMMARY  scanned={scanned}  updated={updated}  unchanged={unchanged}  "
        f"failed={failed}  dry_run={dry_run}  full_scan={full_scan}",
        lines,
    )

    record_library_tool_run(
        library_index_db,
        tool_name="identify-structure",
        root=root,
        run_mode="full" if full_scan else "incremental",
        started_at=started_at,
        completed_at=datetime.datetime.now(datetime.UTC).isoformat(),
        scanned_count=scanned,
        changed_count=updated,
        error_count=failed,
        result={
            "inventory": inventory_summary,
            "dry_run": dry_run,
            "full_scan": full_scan,
            "summary": {
                "scanned": scanned,
                "updated": updated,
                "unchanged": unchanged,
                "failed": failed,
            },
        },
    )
    return lines, 0 if failed == 0 else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "root",
        nargs="?",
        default=os.getenv("NAVIDROME_MUSIC_ROOT", ""),
        help="Music library root. Defaults to NAVIDROME_MUSIC_ROOT from .env.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be updated without writing tags.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Only scan the first N candidate audio files after sorting by path.",
    )
    parser.add_argument(
        "--full-scan",
        action="store_true",
        help="Rewrite all candidate files from structure instead of only filling blanks.",
    )
    args = parser.parse_args()

    if not args.root:
        print(
            "ERROR: no music root provided. Pass it as an argument or set "
            "NAVIDROME_MUSIC_ROOT in .env.",
            file=sys.stderr,
        )
        return 1

    root = Path(args.root).expanduser().resolve()
    if not root.is_dir():
        print(f"ERROR: {root} is not a directory.", file=sys.stderr)
        return 1

    log_lines, exit_code = identify_tracks_from_layout(
        root,
        dry_run=args.dry_run,
        limit=args.limit,
        full_scan=args.full_scan,
    )

    log_ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_name = f"identify_tracks_from_layout_{log_ts}.log"
    log_dir = root if os.access(root, os.W_OK) else Path.cwd()
    log_path = log_dir / log_name

    try:
        log_path.write_text("\n".join(log_lines) + "\n", encoding="utf-8")
        print(f"\nLog written to: {log_path}")
    except OSError as exc:
        print(f"\nWARNING: could not write log file: {exc}", file=sys.stderr)

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
