"""Fix embedded tags using folder layout plus MusicBrainz enrichment.

NOTE: This tool uses only the library database for all audio file lists and tag operations. Run the catalog refresh tool first to update the DB.

Pass 1 aligns artist and albumartist tags with the directory layout:

        {MUSIC_ROOT}/{artist}/{album}/{artist} - {album} - {track#} - {title}.flac

Pass 2 enriches missing track numbers and MusicBrainz IDs from
ListenBrainz/MusicBrainz, retrying with directory-derived artist fallbacks when
the embedded artist looks wrong.

Folder pass — Normal albums:
    The first directory under MUSIC_ROOT is treated as the authoritative artist.
    Both ``artist`` and ``albumartist`` tags are set to that folder name.

Folder pass — Various-artist albums:
    When the artist folder is a known VA placeholder ("Various Artists", etc.)
    the per-track artist is parsed from the filename stem using the convention
    ``{artist} - {album} - {track#} - {title}`` (first " - " segment).
    Only the ``artist`` tag is updated; ``albumartist`` is left as the folder name.

Run:
        uv run python scripts/fix_audio_tags.py [MUSIC_ROOT] [--dry-run] [--limit N]

MUSIC_ROOT defaults to NAVIDROME_MUSIC_ROOT from .env.
A timestamped log is written to MUSIC_ROOT/fix_audio_tags_<timestamp>.log
(or cwd if the root is not writable).
"""

from __future__ import annotations

import argparse
import datetime
import importlib.util
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=False)

from app.services.library_index import (  # noqa: E402
    list_tag_fix_candidates,
    record_library_tool_run,
    refresh_library_index,
    refresh_library_index_for_paths,
)
from app.services.tool_output import emit_console_line  # noqa: E402

try:
    from mutagen import File as MutagenFile  # type: ignore

    _HAS_MUTAGEN = True
except ImportError:
    _HAS_MUTAGEN = False


def _emit(line: str, lines: list[str]) -> None:
    emit_console_line(line)
    lines.append(line)


def _artist_dir_for(audio_path: Path, root: Path) -> str:
    """Return the name of the direct child of *root* that contains *audio_path*.

    That directory is always the artist folder regardless of how many
    disc/subfolder levels exist below it (e.g. {artist}/{album}/CD1/{file}).
    Returns an empty string when the file is directly in *root* (no artist dir).
    """
    try:
        rel = audio_path.relative_to(root)
    except ValueError:
        return ""
    if len(rel.parts) < 2:  # file is directly in root, no artist folder
        return ""
    return rel.parts[0].strip()


# Folder names that signal a Various-Artists compilation.
# Comparison is case-insensitive and stripped.
_VA_NAMES: frozenset[str] = frozenset(
    {
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
)


def _is_va_folder(folder_name: str) -> bool:
    return folder_name.strip().lower() in _VA_NAMES


def _parse_artist_from_stem(stem: str) -> str:
    """Extract the leading artist segment from a filename stem.

    Expects at least ``{artist} - {rest}`` format.  Returns empty string when
    the filename does not match (e.g. no " - " separator, or the first segment
    looks like a bare track number).
    """
    parts = [p.strip() for p in stem.split(" - ") if p.strip()]
    if len(parts) < 2:
        return ""
    candidate = parts[0]
    # Reject pure track-number prefixes like "01" or "1".
    if candidate.isdigit():
        return ""
    return candidate


def _load_musicbrainz_enrich_module():
    module_path = Path(__file__).resolve().parent / "enrich_musicbrainz_tags.py"
    spec = importlib.util.spec_from_file_location(
        "fix_audio_tags_enrich_musicbrainz_module",
        module_path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load {module_path.name}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _parse_summary_counts(lines: list[str]) -> dict[str, int]:
    for line in reversed(lines):
        if not line.lower().startswith("summary"):
            continue
        counts: dict[str, int] = {}
        for token in line.split()[1:]:
            if "=" not in token:
                continue
            key, value = token.split("=", 1)
            try:
                counts[key] = int(value)
            except ValueError:
                continue
        return counts
    return {}


def _run_musicbrainz_enrichment(
    root: Path,
    *,
    dry_run: bool,
    limit: int | None,
    full_scan: bool,
    db_path: str | Path | None,
    selected_paths: list[Path] | None = None,
) -> tuple[list[str], int]:
    enrich_module = _load_musicbrainz_enrich_module()
    return enrich_module.enrich_musicbrainz_tags(
        root,
        dry_run=dry_run,
        limit=limit,
        full_scan=full_scan,
        db_path=db_path,
        tool_name="fix-tags",
        record_run=False,
        selected_paths=selected_paths,
    )


def fix_tags(
    root: Path,
    *,
    dry_run: bool = False,
    limit: int | None = None,
    full_scan: bool = False,
    db_path: str | Path | None = None,
    selected_paths: list[Path] | None = None,
) -> list[str]:
    """Walk *root* and fix artist/albumartist tags.

    Pass 1 fixes normal artist folders (both artist + albumartist tags).
    Pass 2 fixes Various-Artist folders (artist tag from filename; albumartist
    stays as the VA folder name).

    *limit* caps the total number of fixes across both passes.
    Returns all log lines (also printed to stdout as they are generated).
    """
    lines: list[str] = []
    inventory_summary = None
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
        "fix_audio_tags  "
        f"root={root}  dry_run={dry_run}  limit={limit}  full_scan={full_scan}  started={ts}",
        lines,
    )
    _emit("=" * 72, lines)

    if not _HAS_MUTAGEN:
        _emit("ERROR: mutagen is not installed.  Run: uv add mutagen", lines)
        return lines

    total_fixed = 0
    changed_paths: list[Path] = []
    if selected_paths is not None:
        all_audio = (
            selected_paths[:limit] if limit is not None else list(selected_paths)
        )
        _emit(f"PROGRESS: using explicit selection of {len(all_audio)} audio file(s) for tag review.", lines)
    else:
        _emit(f"PROGRESS: querying DB for tag-fix candidates...", lines)
        all_audio = list_tag_fix_candidates(
            library_index_db,
            root,
            force_full=full_scan,
            limit=limit if full_scan else None,
        )
        _emit(f"PROGRESS: selected {len(all_audio)} candidate file(s) for tag review (from DB).", lines)

    total = len(all_audio)
    _emit(f"PROGRESS: found {total} candidate audio file(s) to check (from DB)", lines)

    # Partition files into normal vs various-artist folders.
    _emit(f"PROGRESS: partitioning files into normal and VA folders...", lines)
    normal: list[Path] = []
    va: list[Path] = []
    skipped_depth = 0
    for audio_path in all_audio:
        canonical_artist = _artist_dir_for(audio_path, root)
        if not canonical_artist:
            skipped_depth += 1
            continue
        try:
            rel = audio_path.relative_to(root)
        except ValueError:
            skipped_depth += 1
            continue
        if len(rel.parts) < 3:
            skipped_depth += 1
            continue
        if _is_va_folder(canonical_artist):
            va.append(audio_path)
        else:
            normal.append(audio_path)

    if skipped_depth:
        _emit(f"PROGRESS: skipped {skipped_depth} file(s) with insufficient folder depth.", lines)
    _emit(f"PROGRESS: normal files: {len(normal)}  VA files: {len(va)}", lines)

    # ------------------------------------------------------------------
    # Pass 1: normal artist folders — set both artist and albumartist.
    # ------------------------------------------------------------------
    _emit("", lines)
    _emit("--- Pass 1: fixing artist / albumartist for normal folders ---", lines)

    p1_fixed = 0
    p1_skipped = 0
    p1_failed = 0

    if normal:
        _emit(f"PROGRESS: starting Pass 1 (normal folders) for {len(normal)} files...", lines)
    for idx, audio_path in enumerate(normal):
        if limit is not None and total_fixed >= limit:
            _emit(f"PROGRESS: limit of {limit} reached, stopping Pass 1 early.", lines)
            break

        if idx == 0 or (idx + 1) % 100 == 0 or (idx + 1) == len(normal):
            _emit(f"PROGRESS: Pass 1 progress: {idx + 1}/{len(normal)} files ({p1_fixed} fixed so far)", lines)

        canonical_artist = _artist_dir_for(audio_path, root)
        _emit(f"CHECK TAGS: {idx + 1}/{len(normal)}  {audio_path.relative_to(root)}", lines)

        try:
            mf = MutagenFile(audio_path, easy=True)
        except Exception as exc:
            _emit(f"  ERROR reading {audio_path.name}: {exc}", lines)
            p1_failed += 1
            continue

        if mf is None:
            p1_skipped += 1
            continue

        def _first(audio_file, key: str) -> str:
            val = audio_file.get(key)
            return str(val[0]).strip() if val else ""

        current_artist = _first(mf, "artist")
        current_albumartist = _first(mf, "albumartist")
        needs_artist = current_artist != canonical_artist
        needs_albumartist = current_albumartist != canonical_artist

        if not needs_artist and not needs_albumartist:
            continue

        changes: list[str] = []
        if needs_artist:
            changes.append(f"artist: {current_artist!r} → {canonical_artist!r}")
        if needs_albumartist:
            changes.append(
                f"albumartist: {current_albumartist!r} → {canonical_artist!r}"
            )

        action = "[DRY-RUN] would fix" if dry_run else "FIXED"
        if not dry_run:
            try:
                if needs_artist:
                    mf["artist"] = [canonical_artist]
                if needs_albumartist:
                    mf["albumartist"] = [canonical_artist]
                mf.save()
            except Exception as exc:
                _emit(f"  ERROR writing {audio_path.name}: {exc}", lines)
                p1_failed += 1
                continue

        _emit(
            f"  {action}: {audio_path.relative_to(root)}  [{', '.join(changes)}]",
            lines,
        )
        p1_fixed += 1
        total_fixed += 1
        changed_paths.append(audio_path)

    _emit(
        f"  => Pass 1 done. fixed={p1_fixed}  skipped={p1_skipped}  failed={p1_failed}",
        lines,
    )

    # ------------------------------------------------------------------
    # Pass 2: various-artist folders — fix artist tag from filename only.
    # albumartist stays as the VA folder name.
    # ------------------------------------------------------------------
    _emit("", lines)
    _emit("--- Pass 2: fixing artist tag for Various-Artist folders ---", lines)
    _emit(f"  VA folder names matched: {sorted(_VA_NAMES)}", lines)

    p2_fixed = 0
    p2_skipped = 0
    p2_failed = 0
    p2_no_parse = 0

    if va:
        _emit(f"PROGRESS: starting Pass 2 (VA folders) for {len(va)} files...", lines)
    for idx, audio_path in enumerate(va):
        if limit is not None and total_fixed >= limit:
            _emit(f"PROGRESS: limit of {limit} reached, skipping rest of Pass 2.", lines)
            break

        if idx == 0 or (idx + 1) % 100 == 0 or (idx + 1) == len(va):
            _emit(f"PROGRESS: Pass 2 progress: {idx + 1}/{len(va)} VA files ({p2_fixed} fixed so far)", lines)

        canonical_artist = _artist_dir_for(audio_path, root)  # e.g. "Various Artists"
        track_artist = _parse_artist_from_stem(audio_path.stem)
        _emit(f"CHECK VA TAGS: {idx + 1}/{len(va)}  {audio_path.relative_to(root)}", lines)
        if not track_artist:
            p2_no_parse += 1
            continue

        try:
            mf = MutagenFile(audio_path, easy=True)
        except Exception as exc:
            _emit(f"  ERROR reading {audio_path.name}: {exc}", lines)
            p2_failed += 1
            continue

        if mf is None:
            p2_skipped += 1
            continue

        def _firstv(audio_file, key: str) -> str:
            val = audio_file.get(key)
            return str(val[0]).strip() if val else ""

        current_artist = _firstv(mf, "artist")
        current_albumartist = _firstv(mf, "albumartist")

        # artist → parsed from filename; albumartist → VA folder name
        needs_artist = current_artist != track_artist
        needs_albumartist = current_albumartist != canonical_artist

        if not needs_artist and not needs_albumartist:
            continue

        changes = []
        if needs_artist:
            changes.append(f"artist: {current_artist!r} → {track_artist!r}")
        if needs_albumartist:
            changes.append(
                f"albumartist: {current_albumartist!r} → {canonical_artist!r}"
            )

        action = "[DRY-RUN] would fix" if dry_run else "FIXED"
        if not dry_run:
            try:
                if needs_artist:
                    mf["artist"] = [track_artist]
                if needs_albumartist:
                    mf["albumartist"] = [canonical_artist]
                mf.save()
            except Exception as exc:
                _emit(f"  ERROR writing {audio_path.name}: {exc}", lines)
                p2_failed += 1
                continue

        _emit(
            f"  {action}: {audio_path.relative_to(root)}  [{', '.join(changes)}]",
            lines,
        )
        p2_fixed += 1
        total_fixed += 1
        changed_paths.append(audio_path)

    _emit(
        f"  => Pass 2 done. fixed={p2_fixed}  no_parse={p2_no_parse}"
        f"  skipped={p2_skipped}  failed={p2_failed}",
        lines,
    )

    enrichment_limit = None
    if limit is not None:
        enrichment_limit = max(limit - total_fixed, 0)

    musicbrainz_updated = 0
    musicbrainz_failed = 0
    musicbrainz_unresolved = 0
    musicbrainz_exit_code = 0

    _emit("", lines)
    _emit("PROGRESS: starting Pass 3 (MusicBrainz enrichment)...", lines)
    enrich_updated_paths = []
    if enrichment_limit == 0:
        _emit(
            "  ... limit already reached during folder-tag cleanup, skipping pass 3.",
            lines,
        )
    else:
        enrich_lines, musicbrainz_exit_code = _run_musicbrainz_enrichment(
            root,
            dry_run=dry_run,
            limit=enrichment_limit,
            full_scan=full_scan,
            db_path=library_index_db,
            selected_paths=all_audio,
        )
        lines.extend(enrich_lines)
        enrich_summary = _parse_summary_counts(enrich_lines)
        musicbrainz_updated = enrich_summary.get("updated", 0)
        musicbrainz_failed = enrich_summary.get("failed", 0)
        musicbrainz_unresolved = enrich_summary.get("unresolved", 0)
        # Parse updated file paths from enrich_lines
        for line in enrich_lines:
            if line.startswith("UPDATED: "):
                # Format: UPDATED: relative_path [...]
                rel_path = line.split()[1]
                enrich_updated_paths.append(root / rel_path)

    total_changed = total_fixed + musicbrainz_updated
    total_failed = p1_failed + p2_failed + musicbrainz_failed

    _emit("", lines)
    _emit("=" * 72, lines)
    _emit(
        f"SUMMARY  total_changed={total_changed}"
        f"  folder_fixed={total_fixed}  musicbrainz_updated={musicbrainz_updated}"
        f"  unresolved={musicbrainz_unresolved}  failed={total_failed}"
        f"  dry_run={dry_run}  full_scan={full_scan}",
        lines,
    )
    if not dry_run:
        # Only re-index files that were actually changed (fixed or enriched)
        all_changed = changed_paths + enrich_updated_paths
        if all_changed:
            refresh_library_index_for_paths(
                library_index_db,
                root,
                all_changed,
                scan_xml_sidecars=False,
            )
    record_library_tool_run(
        library_index_db,
        tool_name="fix-tags",
        root=root,
        run_mode="full" if full_scan else "incremental",
        started_at=started_at,
        completed_at=datetime.datetime.now(datetime.UTC).isoformat(),
        scanned_count=total
        + musicbrainz_updated
        + musicbrainz_unresolved
        + musicbrainz_failed,
        changed_count=total_changed,
        error_count=total_failed + musicbrainz_unresolved,
        result={
            "inventory": inventory_summary,
            "total_changed": total_changed,
            "folder_fixed": total_fixed,
            "pass1_fixed": p1_fixed,
            "pass2_fixed": p2_fixed,
            "musicbrainz_updated": musicbrainz_updated,
            "musicbrainz_unresolved": musicbrainz_unresolved,
            "musicbrainz_exit_code": musicbrainz_exit_code,
            "failed": total_failed,
            "dry_run": dry_run,
            "full_scan": full_scan,
        },
    )
    return lines


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
        default=None,
        metavar="N",
        help="Stop after fixing (or previewing) N total files across both passes.",
    )
    parser.add_argument(
        "--full-scan",
        action="store_true",
        help="Inspect all indexed audio files instead of only the catalog-reported mismatches.",
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

    log_lines = fix_tags(
        root,
        dry_run=args.dry_run,
        limit=args.limit,
        full_scan=args.full_scan,
    )

    log_ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_name = f"fix_audio_tags_{log_ts}.log"
    log_dir = Path(__file__).resolve().parent.parent / "data" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / log_name

    try:
        log_path.write_text("\n".join(log_lines) + "\n", encoding="utf-8")
        print(f"\nLog written to: {log_path}")
    except OSError as exc:
        print(f"\nWARNING: could not write log file: {exc}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
