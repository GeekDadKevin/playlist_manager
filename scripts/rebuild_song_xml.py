"""Rebuild song XML sidecars to match the current state of the music library.

NOTE: This tool uses only the library database for all file lists and XML operations. Run the catalog refresh tool first to update the DB.

Steps:
    1. Delete every .xml sidecar whose audio file is no longer present in the library database.
    2. Create a new .xml sidecar for every audio file in the database that is missing one.

Run:
        uv run python scripts/rebuild_song_xml.py [MUSIC_ROOT] [--dry-run]

MUSIC_ROOT defaults to NAVIDROME_MUSIC_ROOT from .env.
A timestamped log is written to MUSIC_ROOT/rebuild_song_xml_<timestamp>.log
(or cwd if the root is not writable).
"""
from __future__ import annotations

import argparse
import datetime
import os
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

# Allow importing from the repo root even when run directly.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=False)

from app.services.library_index import (  # noqa: E402
    list_incomplete_xml_pairs,
    list_missing_xml_audio_paths,
    list_orphaned_xml_paths,
    record_library_tool_run,
    refresh_library_index,
    refresh_library_index_for_paths,
)
from app.services.song_metadata import (  # noqa: E402
    guess_track_metadata,
    load_embedded_audio_metadata,
    write_song_metadata_xml,
)
from app.services.tool_output import emit_console_line  # noqa: E402

# ---------------------------------------------------------------------------
# Optional: try to read real embedded tags via mutagen before falling back
# to filename-based guessing.
# ---------------------------------------------------------------------------
try:
    from mutagen import File as MutagenFile  # type: ignore
    _HAS_MUTAGEN = True
except ImportError:
    _HAS_MUTAGEN = False


def _artist_dir_for(audio_path: Path, root: Path) -> str:
    """Return the name of the direct child of *root* that contains *audio_path*.

    This is always the artist folder regardless of how many disc/subfolder
    levels exist below it (e.g. Artist/Album/CD1/track.flac).
    Returns an empty string when the file sits directly in *root*.
    """
    try:
        rel = audio_path.relative_to(root)
    except ValueError:
        return ""
    if len(rel.parts) < 2:
        return ""
    return rel.parts[0].strip()


def _read_tags(audio_path: Path, root: Path) -> dict[str, str]:
    """Return tags from embedded metadata, falling back to directory layout.

    Always returns keys: title, artist, albumartist, album, performing_artist.
    The artist fallback is the direct child of *root* that contains the file
    (i.e. the artist folder), so extra disc subfolders don't corrupt the value.
    """
    fallback = guess_track_metadata(audio_path)
    dir_artist = _artist_dir_for(audio_path, root)
    embedded = load_embedded_audio_metadata(audio_path)

    if not _HAS_MUTAGEN:
        return {
            "title": embedded.get("title") or fallback["title"],
            "artist": embedded.get("artist") or "",
            "albumartist": embedded.get("albumartist") or "",
            "album": embedded.get("album") or fallback["album"],
            "performing_artist": (
                embedded.get("artist") or embedded.get("albumartist") or dir_artist
            ),
            "musicbrainz_track_id": embedded.get("musicbrainz_track_id", ""),
            "deezer_id": embedded.get("deezer_id", ""),
            "deezer_artist_id": embedded.get("deezer_artist_id", ""),
            "deezer_album_id": embedded.get("deezer_album_id", ""),
            "deezer_link": embedded.get("deezer_link", ""),
        }

    try:
        mf = MutagenFile(audio_path, easy=True)
    except Exception:
        mf = None

    if mf is None:
        return {
            "title": embedded.get("title") or fallback["title"],
            "artist": embedded.get("artist") or "",
            "albumartist": embedded.get("albumartist") or "",
            "album": embedded.get("album") or fallback["album"],
            "performing_artist": (
                embedded.get("artist") or embedded.get("albumartist") or dir_artist
            ),
            "musicbrainz_track_id": embedded.get("musicbrainz_track_id", ""),
            "deezer_id": embedded.get("deezer_id", ""),
            "deezer_artist_id": embedded.get("deezer_artist_id", ""),
            "deezer_album_id": embedded.get("deezer_album_id", ""),
            "deezer_link": embedded.get("deezer_link", ""),
        }

    def _first(key: str) -> str:
        val = mf.get(key)
        if val:
            return str(val[0]).strip()
        return ""

    title = _first("title") or embedded.get("title") or fallback["title"]
    artist = _first("artist") or embedded.get("artist", "")
    albumartist = _first("albumartist") or embedded.get("albumartist", "")
    album = _first("album") or embedded.get("album") or fallback["album"]
    # Prefer embedded artist tag; fall back to albumartist; last resort: artist dir.
    performing_artist = artist or albumartist or dir_artist
    return {
        "title": title,
        "artist": artist,
        "albumartist": albumartist,
        "album": album,
        "performing_artist": performing_artist,
        "musicbrainz_track_id": embedded.get("musicbrainz_track_id", ""),
        "deezer_id": embedded.get("deezer_id", ""),
        "deezer_artist_id": embedded.get("deezer_artist_id", ""),
        "deezer_album_id": embedded.get("deezer_album_id", ""),
        "deezer_link": embedded.get("deezer_link", ""),
    }


def _update_xml_fields(xml_path: Path, tags: dict[str, str], *, dry_run: bool) -> bool:
    """Patch recovered metadata fields in an existing XML sidecar.

    Returns True if any change was made.
    """
    try:
        tree = ET.parse(xml_path)
    except Exception:
        return False
    xml_root = tree.getroot()
    changed = False

    def _set_field(tag: str, new_value: str) -> bool:
        nonlocal changed
        if not new_value:
            return False
        elem = xml_root.find(tag)
        if elem is None:
            elem = ET.SubElement(xml_root, tag)
            elem.text = ""
        current = (elem.text or "").strip()
        if current == new_value:
            return False
        if not dry_run:
            elem.text = new_value
        return True

    fields = {
        "performingartist": tags["performing_artist"],
        "albumartist": tags["albumartist"] or tags["performing_artist"],
        "musicbrainztrackid": tags.get("musicbrainz_track_id", ""),
        "deezerid": tags.get("deezer_id", ""),
        "deezerartistid": tags.get("deezer_artist_id", ""),
        "deezeralbumid": tags.get("deezer_album_id", ""),
        "deezerlink": tags.get("deezer_link", ""),
    }

    for field_name, field_value in fields.items():
        changed = _set_field(field_name, field_value) or changed

    if changed and not dry_run:
        ET.indent(tree, space="  ")
        tree.write(xml_path, encoding="utf-8", xml_declaration=True)
    return changed


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def _emit(line: str, lines: list[str]) -> None:
    """Print a line immediately and append it to the log buffer."""
    emit_console_line(line)
    lines.append(line)


def rebuild(
    root: Path,
    *,
    dry_run: bool = False,
    limit: int | None = None,
    full_scan: bool = False,
    db_path: str | Path | None = None,
    selected_audio_paths: list[Path] | None = None,
) -> list[str]:
    """Stream progress to stdout and return all log lines for the log file."""
    lines: list[str] = []
    started_at = datetime.datetime.now(datetime.UTC).isoformat()
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    library_index_db = str(
        db_path
        or os.getenv(
            "LIBRARY_INDEX_DB_PATH",
            Path(__file__).resolve().parent.parent / "data" / "library_index.db",
        )
    )
    _emit(
        "rebuild_song_xml  "
        f"root={root}  dry_run={dry_run}  full_scan={full_scan}  started={ts}",
        lines,
    )
    _emit("=" * 72, lines)

    # No inventory scan: rely on DB only. User must refresh catalog first.
    inventory_summary = None

    deleted = 0
    created = 0
    failed = 0
    scanned_xml = 0
    scanned_audio = 0

    # ------------------------------------------------------------------
    # Pass 1: delete orphaned XML sidecars.
    # An XML is orphaned when no audio file with the same stem exists in
    # the same directory.
    # ------------------------------------------------------------------
    _emit("", lines)
    _emit("--- Pass 1: scanning for orphaned XML sidecars ---", lines)

    all_xml = list_orphaned_xml_paths(
        library_index_db,
        root,
        limit=limit if dry_run else None,
    )
    total_xml = len(all_xml)
    _emit(f"  Found {total_xml} orphaned XML file(s) to check (from DB)", lines)
    for xml_path in all_xml:
        scanned_xml += 1
        _emit(f"CHECK ORPHAN XML: {scanned_xml}/{total_xml}  {xml_path.relative_to(root)}", lines)
        if scanned_xml % 100 == 0:
            _emit(
                f"  ... scanned {scanned_xml}/{total_xml} XML files "
                f"({deleted} orphaned so far)",
                lines,
            )
        if dry_run and limit is not None and deleted >= limit:
            _emit(f"  ... (dry-run limit of {limit} reached, stopping preview)", lines)
            break
        action = "[DRY-RUN] would delete" if dry_run else "DELETED"
        if not dry_run:
            try:
                xml_path.unlink()
            except OSError as exc:
                _emit(f"  ERROR deleting {xml_path}: {exc}", lines)
                failed += 1
                continue
        _emit(f"  {action}: {xml_path}", lines)
        deleted += 1

    _emit(
        f"  => Pass 1 done. Scanned {scanned_xml} XML(s), orphaned "
        f"{'would be ' if dry_run else ''}removed: {deleted}",
        lines,
    )

    # ------------------------------------------------------------------
    # Pass 2: create missing XML sidecars for every audio file.
    # ------------------------------------------------------------------
    _emit("", lines)
    _emit("--- Pass 2: scanning for audio files missing XML sidecars ---", lines)

    all_audio = list_missing_xml_audio_paths(
        library_index_db,
        root,
        limit=limit if dry_run else None,
    )
    total_audio = len(all_audio)
    _emit(f"  Found {total_audio} audio file(s) missing XML sidecars (from DB)", lines)
    for audio_path in all_audio:
        scanned_audio += 1
        _emit(
            f"CHECK MISSING XML: {scanned_audio}/{total_audio}  {audio_path.relative_to(root)}",
            lines,
        )
        if scanned_audio % 100 == 0:
            _emit(
                f"  ... scanned {scanned_audio}/{total_audio} audio files "
                f"({created} missing XMLs so far)",
                lines,
            )
        xml_path = audio_path.with_suffix(".xml")
        if dry_run and limit is not None and created >= limit:
            _emit(f"  ... (dry-run limit of {limit} reached, stopping preview)", lines)
            break
        _emit(f"  Reading tags: {audio_path.name}", lines)
        tags = _read_tags(audio_path, root)
        action = "[DRY-RUN] would create" if dry_run else "CREATED"
        if not dry_run:
            try:
                write_song_metadata_xml(
                    audio_path,
                    title=tags["title"],
                    artist=tags["performing_artist"],
                    album=tags["album"],
                    deezer_id=tags.get("deezer_id", ""),
                    deezer_artist_id=tags.get("deezer_artist_id", ""),
                    deezer_album_id=tags.get("deezer_album_id", ""),
                    deezer_link=tags.get("deezer_link", ""),
                    source=str(audio_path),
                    downloaded_from="library",
                    musicbrainz_track_id=tags.get("musicbrainz_track_id", ""),
                    overwrite=True,
                )
            except Exception as exc:
                _emit(f"  ERROR creating {xml_path}: {exc}", lines)
                failed += 1
                continue
        _emit(
            f"  {action}: {xml_path.name}"
            f"  [title={tags['title']!r}  performingartist={tags['performing_artist']!r}"
            f"  album={tags['album']!r}]",
            lines,
        )
        created += 1

    _emit(
        f"  => Pass 2 done. Scanned {scanned_audio} audio file(s), XMLs "
        f"{'would be ' if dry_run else ''}created: {created}",
        lines,
    )

    # ------------------------------------------------------------------
    # Pass 3: fix recovered metadata in every existing XML sidecar.
    # Re-reads embedded tags from the paired audio file and updates the
    # XML when the stored value does not match.
    # ------------------------------------------------------------------
    _emit("", lines)
    _emit("--- Pass 3: fixing recovered metadata in existing XML sidecars ---", lines)

    fixed = 0
    scanned_fix = 0
    # Re-scan so we also catch XMLs that already existed before Pass 2.
    all_xml_fix = (
        [
            (audio_path.with_suffix(".xml"), audio_path)
            for audio_path in selected_audio_paths
            if audio_path.with_suffix(".xml").exists()
        ]
        if selected_audio_paths is not None
        else list_incomplete_xml_pairs(
            library_index_db,
            root,
            limit=None if full_scan else (limit if dry_run else None),
        )
    )
    if full_scan:
        all_xml_fix = [
            (xml_path, audio_path)
            for xml_path, audio_path in [
                (path.with_suffix(".xml"), path)
                for path in list_missing_xml_audio_paths(library_index_db, root, limit=None)
            ]
            if xml_path.exists()
        ] + all_xml_fix
    total_xml_fix = len(all_xml_fix)
    _emit(f"  Found {total_xml_fix} XML file(s) needing metadata refresh", lines)

    for xml_path, audio_path in all_xml_fix:
        if not xml_path.is_file() or not audio_path.is_file():
            continue

        scanned_fix += 1
        _emit(
            f"CHECK XML METADATA: {scanned_fix}/{total_xml_fix}  {xml_path.relative_to(root)}",
            lines,
        )
        if scanned_fix % 100 == 0:
            _emit(
                f"  ... scanned {scanned_fix}/{total_xml_fix} XML files "
                f"({fixed} fixed so far)",
                lines,
            )

        tags = _read_tags(audio_path, root)
        changed = _update_xml_fields(xml_path, tags, dry_run=dry_run)
        if changed:
            action = "[DRY-RUN] would fix" if dry_run else "FIXED"
            _emit(
                f"  {action}: {xml_path.name}"
                f"  recovered metadata refreshed",
                lines,
            )
            fixed += 1

    _emit(
        f"  => Pass 3 done. Scanned {scanned_fix} XML(s), "
        f"{'would be ' if dry_run else ''}fixed: {fixed}",
        lines,
    )

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    _emit("", lines)
    _emit("=" * 72, lines)
    _emit(
        f"SUMMARY  deleted={deleted}  created={created}  fixed={fixed}  failed={failed}"
        f"  dry_run={dry_run}  full_scan={full_scan}",
        lines,
    )
    if not dry_run:
        if selected_audio_paths is not None:
            refresh_library_index_for_paths(
                library_index_db,
                root,
                selected_audio_paths,
            )
        else:
            refresh_library_index(
                library_index_db,
                root,
                progress_callback=lambda line: _emit(line, lines),
                limit=limit,
            )
    record_library_tool_run(
        library_index_db,
        tool_name="rebuild-xml",
        root=root,
        run_mode="full" if full_scan else "incremental",
        started_at=started_at,
        completed_at=datetime.datetime.now(datetime.UTC).isoformat(),
        scanned_count=scanned_xml + scanned_audio + scanned_fix,
        changed_count=deleted + created + fixed,
        error_count=failed,
        result={
            "inventory": inventory_summary,
            "deleted": deleted,
            "created": created,
            "fixed": fixed,
            "failed": failed,
            "dry_run": dry_run,
            "full_scan": full_scan,
        },
    )
    return lines


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

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
        help="Show what would be changed without touching any files.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5,
        metavar="N",
        help=(
            "Max items to preview per pass in --dry-run mode (default: 5). "
            "Ignored when not dry-running."
        ),
    )
    parser.add_argument(
        "--full-scan",
        action="store_true",
        help="Ignore incremental worklists and process the full catalog when supported.",
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

    log_lines = rebuild(
        root,
        dry_run=args.dry_run,
        limit=args.limit if args.dry_run else None,
        full_scan=args.full_scan,
    )

    # Write log file.
    log_ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_name = f"rebuild_song_xml_{log_ts}.log"
    log_dir = root if os.access(root, os.W_OK) else Path.cwd()
    log_path = log_dir / log_name

    try:
        log_path.write_text("\n".join(log_lines) + "\n", encoding="utf-8")
        print(f"\nLog written to: {log_path}")
    except OSError as exc:
        print(f"\nWARNING: could not write log file: {exc}", file=sys.stderr)

    # Return non-zero only on unexpected failures (not on normal deletes/creates).
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
