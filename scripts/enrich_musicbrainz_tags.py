"""Enrich embedded audio tags from MusicBrainz.

Scans audio files that are missing core tags, MusicBrainz recording IDs, or
usable track numbers, then resolves richer metadata from ListenBrainz and
MusicBrainz. If the embedded artist looks wrong, the tool retries lookup using
the artist inferred from the library directory structure before falling back to
local guesses alone.

Run:
    uv run python scripts/enrich_musicbrainz_tags.py [MUSIC_ROOT] [--dry-run] [--limit N]

MUSIC_ROOT defaults to NAVIDROME_MUSIC_ROOT from .env.
A timestamped log is written to MUSIC_ROOT/enrich_musicbrainz_tags_<timestamp>.log
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

from app.services.audio_identification import lookup_musicbrainz_metadata_match  # noqa: E402
from app.services.library_index import (  # noqa: E402
    list_musicbrainz_tag_candidates,
    record_library_tool_run,
    record_musicbrainz_verification,
    refresh_library_index,
    refresh_library_index_for_paths,
)
from app.services.listenbrainz import ListenBrainzService  # noqa: E402
from app.services.musicbrainz import MusicBrainzService  # noqa: E402
from app.services.song_metadata import (  # noqa: E402
    guess_track_metadata,
    load_embedded_audio_metadata,
    load_song_metadata_xml,
)
from app.services.tool_output import emit_console_line  # noqa: E402

try:
    from app.services.musicbrainz_tag_writer import (  # type: ignore  # noqa: E402
        normalize_musicbrainz_details,
        write_musicbrainz_tags,
    )

    _HAS_MUTAGEN = True
except ImportError:
    _HAS_MUTAGEN = False


def _emit(line: str, lines: list[str]) -> None:
    emit_console_line(line)
    lines.append(line)


def _artist_dir_for(audio_path: Path, root: Path) -> str:
    try:
        rel = audio_path.relative_to(root)
    except ValueError:
        return ""
    if len(rel.parts) < 2:
        return ""
    return rel.parts[0].strip()


def _is_va_folder(folder_name: str) -> bool:
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
    parts = [part.strip() for part in stem.split(" - ") if part.strip()]
    if len(parts) < 2:
        return ""
    candidate = parts[0]
    return "" if candidate.isdigit() else candidate


def _track_number_missing(value: str) -> bool:
    text = str(value or "").strip()
    if not text:
        return True
    primary = text.split("/", 1)[0].strip()
    return primary in {"", "0"}


def _build_search_variants(audio_path: Path, root: Path) -> list[dict[str, str]]:
    embedded = load_embedded_audio_metadata(audio_path)
    xml_data = load_song_metadata_xml(audio_path.with_suffix(".xml"))
    guessed = guess_track_metadata(audio_path)

    title = str(
        embedded.get("title")
        or xml_data.get("title")
        or guessed.get("title")
        or audio_path.stem
    ).strip()
    artist = str(
        embedded.get("artist")
        or embedded.get("albumartist")
        or xml_data.get("performingartist")
        or xml_data.get("artist")
        or guessed.get("artist")
    ).strip()
    album = str(
        embedded.get("album")
        or xml_data.get("albumtitle")
        or xml_data.get("album")
        or guessed.get("album")
    ).strip()
    albumartist = str(
        embedded.get("albumartist")
        or xml_data.get("albumartist")
        or artist
    ).strip()

    variants: list[dict[str, str]] = []
    primary = {
        "title": title,
        "artist": artist,
        "album": album,
        "albumartist": albumartist,
    }
    if title:
        variants.append(primary)

    dir_artist = _artist_dir_for(audio_path, root)
    if dir_artist:
        fallback_artist = (
            _parse_artist_from_stem(audio_path.stem)
            if _is_va_folder(dir_artist)
            else dir_artist
        )
        fallback_albumartist = dir_artist if _is_va_folder(dir_artist) else fallback_artist
        fallback = {
            "title": title,
            "artist": fallback_artist or artist,
            "album": album or audio_path.parent.name.strip(),
            "albumartist": fallback_albumartist or fallback_artist or albumartist,
        }
        if fallback["title"] and fallback not in variants:
            variants.append(fallback)

    return variants


def _lookup_details(
    audio_path: Path,
    root: Path,
    *,
    listenbrainz: ListenBrainzService,
    musicbrainz: MusicBrainzService,
) -> tuple[dict[str, Any], dict[str, str]]:
    for variant in _build_search_variants(audio_path, root):
        if not variant.get("title") or not variant.get("artist"):
            continue

        recording_mbid = ""
        release_mbid = ""
        try:
            lb_metadata = listenbrainz.lookup_recording_metadata(
                artist_name=variant["artist"],
                recording_name=variant["title"],
                release_name=variant["album"],
            )
        except Exception:
            lb_metadata = {}
        recording_mbid = str(lb_metadata.get("recording_mbid") or "").strip()
        release_mbid = str(lb_metadata.get("release_mbid") or "").strip()

        try:
            details = musicbrainz.lookup_recording_details(
                title=variant["title"],
                artist_name=variant["artist"],
                album_name=variant["album"],
                recording_mbid=recording_mbid,
                release_mbid=release_mbid,
            )
        except Exception:
            details = {}
        if details:
            return details, variant
    return {}, {}


def _write_tags(audio_path: Path, details: dict[str, Any]) -> None:
    write_musicbrainz_tags(audio_path, details)


def enrich_musicbrainz_tags(
    root: Path,
    *,
    dry_run: bool = False,
    limit: int | None = None,
    full_scan: bool = False,
    db_path: str | Path | None = None,
    tool_name: str = "enrich-musicbrainz-tags",
    record_run: bool = True,
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
        "enrich_musicbrainz_tags  "
        f"root={root}  dry_run={dry_run}  limit={limit}  full_scan={full_scan}  started={started}",
        lines,
    )
    _emit("=" * 72, lines)

    if not _HAS_MUTAGEN:
        _emit("ERROR: mutagen is not installed. Run: uv add mutagen", lines)
        return lines, 1

    if selected_paths is not None:
        inventory_summary = None
        candidates = selected_paths[:limit] if limit is not None else list(selected_paths)
        _emit(
            "  Using explicit selection of "
            f"{len(candidates)} audio file(s) for MusicBrainz enrichment.",
            lines,
        )
    else:
        inventory_summary = refresh_library_index(
            library_index_db,
            root,
            progress_callback=lambda line: _emit(line, lines),
            limit=limit,
            scan_xml_sidecars=False,
        )
        candidates = list_musicbrainz_tag_candidates(
            library_index_db,
            root,
            force_full=full_scan,
            limit=limit,
        )
        _emit(
            "  Indexed "
            f"{inventory_summary['scanned']} audio file(s); "
            f"selected {len(candidates)} candidate file(s) for MusicBrainz enrichment.",
            lines,
        )

    listenbrainz = ListenBrainzService.from_config(os.environ)
    musicbrainz = MusicBrainzService.from_config(os.environ)
    updated = 0
    unresolved: list[str] = []
    failed = 0

    for index, audio_path in enumerate(candidates, start=1):
        relative_path = audio_path.relative_to(root)
        _emit(f"CHECK MB TAGS: {index}/{len(candidates)}  {relative_path}", lines)
        details, variant = _lookup_details(
            audio_path,
            root,
            listenbrainz=listenbrainz,
            musicbrainz=musicbrainz,
        )
        used_metadata_fallback = False
        if not details:
            details = lookup_musicbrainz_metadata_match(
                audio_path,
                musicbrainz_service=musicbrainz,
                root=root,
            )
            if details:
                variant = {
                    "artist": str(details.get("artist") or "").strip(),
                    "title": str(details.get("title") or "").strip(),
                    "album": str(details.get("album") or "").strip(),
                    "albumartist": str(details.get("albumartist") or "").strip(),
                }
                used_metadata_fallback = True
        if not details:
            unresolved.append(str(relative_path))
            _emit(f"WARN: no MusicBrainz match for {relative_path}", lines)
            continue

        current = load_embedded_audio_metadata(audio_path)
        current_track = str(current.get("track_number") or "").strip()
        normalized = normalize_musicbrainz_details(details)
        changes: list[str] = []
        for field, new_value in (
            ("title", normalized["title"]),
            ("artist", normalized["artist"]),
            ("album", normalized["album"]),
            ("albumartist", normalized["albumartist"]),
            ("artist_sort", normalized["artist_sort"]),
            ("albumartist_sort", normalized["albumartist_sort"]),
            ("musicbrainz_album_id", normalized["release_mbid"]),
            ("musicbrainz_artist_id", normalized["artist_mbid"]),
            (
                "musicbrainz_albumartist_id",
                normalized["albumartist_mbid"],
            ),
            ("musicbrainz_track_id", normalized["recording_mbid"]),
            ("musicbrainz_release_group_id", normalized["release_group_mbid"]),
            ("track_total", normalized["track_total"]),
            ("disc_number", normalized["disc_number"]),
            ("disc_total", normalized["disc_total"]),
            ("date", normalized["date"]),
            ("original_date", normalized["original_date"]),
            ("genre", normalized["genre"]),
            ("isrc", normalized["isrc"]),
            ("barcode", normalized["barcode"]),
            ("label", normalized["label"]),
            ("catalog_number", normalized["catalog_number"]),
            ("media_format", normalized["media_format"]),
            ("release_country", normalized["release_country"]),
            ("release_status", normalized["release_status"]),
            ("release_type", normalized["release_type"]),
            ("release_secondary_types", normalized["release_secondary_types"]),
            ("language", normalized["language"]),
            ("script", normalized["script"]),
            ("recording_disambiguation", normalized["recording_disambiguation"]),
            ("album_disambiguation", normalized["album_disambiguation"]),
        ):
            if new_value and str(current.get(field) or "").strip() != new_value:
                changes.append(field)
        if details.get("track_number") and _track_number_missing(current_track):
            changes.append("track_number")

        if not changes:
            if not dry_run:
                record_musicbrainz_verification(library_index_db, audio_path, root=root)
            continue

        if dry_run:
            _emit(
                f"  [DRY-RUN] would update {relative_path} using MusicBrainz match "
                f"artist={variant.get('artist', '')!r} fields={', '.join(changes)} "
                f"source={'musicbrainz-metadata' if used_metadata_fallback else 'query'}",
                lines,
            )
            updated += 1
            continue

        try:
            _write_tags(audio_path, details)
            record_musicbrainz_verification(library_index_db, audio_path, root=root)
        except Exception as exc:
            failed += 1
            _emit(f"ERROR: could not update {relative_path}  [{exc}]", lines)
            continue

        updated += 1
        _emit(
            "UPDATED: "
            f"{relative_path}  [fields={', '.join(changes)}  "
            f"match_artist={variant.get('artist', '')!r}  "
            f"source={'musicbrainz-metadata' if used_metadata_fallback else 'query'}]",
            lines,
        )

    if unresolved:
        _emit("", lines)
        _emit("Unresolved MusicBrainz matches:", lines)
        for item in unresolved[:50]:
            _emit(f"  MISSING: {item}", lines)
        if len(unresolved) > 50:
            _emit(f"  ... and {len(unresolved) - 50} more unresolved file(s)", lines)

    _emit("", lines)
    _emit("=" * 72, lines)
    _emit(
        f"SUMMARY  scanned={len(candidates)}  updated={updated}  unresolved={len(unresolved)}  "
        f"failed={failed}  dry_run={dry_run}  full_scan={full_scan}",
        lines,
    )

    if not dry_run:
        if selected_paths is not None:
            refresh_library_index_for_paths(
                library_index_db,
                root,
                candidates,
                scan_xml_sidecars=False,
            )
        else:
            refresh_library_index(
                library_index_db,
                root,
                progress_callback=lambda line: _emit(line, lines),
                limit=limit,
                scan_xml_sidecars=False,
            )

    if record_run:
        record_library_tool_run(
            library_index_db,
            tool_name=tool_name,
            root=root,
            run_mode="full" if full_scan else "incremental",
            started_at=started_at,
            completed_at=datetime.datetime.now(datetime.UTC).isoformat(),
            scanned_count=len(candidates),
            changed_count=updated,
            error_count=failed + len(unresolved),
            result={
                "inventory": inventory_summary,
                "updated": updated,
                "unresolved": unresolved,
                "failed": failed,
                "dry_run": dry_run,
                "full_scan": full_scan,
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
        help="Show what would be updated without writing any tags.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Only inspect the first N candidate audio files after sorting by path.",
    )
    parser.add_argument(
        "--full-scan",
        action="store_true",
        help="Recheck all indexed audio files instead of only files missing MusicBrainz/core tags.",
    )
    args = parser.parse_args()

    if not args.root:
        print(
            "ERROR: no music root provided. "
            "Pass it as an argument or set NAVIDROME_MUSIC_ROOT in .env.",
            file=sys.stderr,
        )
        return 1

    root = Path(args.root).expanduser().resolve()
    if not root.is_dir():
        print(f"ERROR: {root} is not a directory.", file=sys.stderr)
        return 1

    log_lines, exit_code = enrich_musicbrainz_tags(
        root,
        dry_run=args.dry_run,
        limit=args.limit,
        full_scan=args.full_scan,
    )

    log_ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_name = f"enrich_musicbrainz_tags_{log_ts}.log"
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
