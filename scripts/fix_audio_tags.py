"""Fix embedded artist / albumartist tags in audio files so they match the
directory layout:

    {MUSIC_ROOT}/{artist}/{album}/{artist} - {album} - {track#} - {title}.flac

Pass 1 — Normal albums:
  The first directory under MUSIC_ROOT is treated as the authoritative artist.
  Both ``artist`` and ``albumartist`` tags are set to that folder name.

Pass 2 — Various-artist albums:
  When the artist folder is a known VA placeholder (\"Various Artists\", etc.)
  the per-track artist is parsed from the filename stem using the convention
  ``{artist} - {album} - {track#} - {title}`` (first \" - \" segment).
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
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=False)

from app.services.song_metadata import AUDIO_EXTENSIONS

try:
    from mutagen import File as MutagenFile  # type: ignore
    _HAS_MUTAGEN = True
except ImportError:
    _HAS_MUTAGEN = False


def _emit(line: str, lines: list[str]) -> None:
    print(line, flush=True)
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
_VA_NAMES: frozenset[str] = frozenset({
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
})


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


def fix_tags(root: Path, *, dry_run: bool = False, limit: int | None = None) -> list[str]:
    """Walk *root* and fix artist/albumartist tags.

    Pass 1 fixes normal artist folders (both artist + albumartist tags).
    Pass 2 fixes Various-Artist folders (artist tag from filename; albumartist
    stays as the VA folder name).

    *limit* caps the total number of fixes across both passes.
    Returns all log lines (also printed to stdout as they are generated).
    """
    lines: list[str] = []
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    _emit(f"fix_audio_tags  root={root}  dry_run={dry_run}  limit={limit}  started={ts}", lines)
    _emit("=" * 72, lines)

    if not _HAS_MUTAGEN:
        _emit("ERROR: mutagen is not installed.  Run: uv add mutagen", lines)
        return lines

    total_fixed = 0

    all_audio = sorted(
        p for p in root.rglob("*")
        if p.is_file() and p.suffix.lower() in AUDIO_EXTENSIONS
    )
    total = len(all_audio)
    _emit(f"  Found {total} audio file(s) to check", lines)

    # Partition files into normal vs various-artist folders.
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
        _emit(f"  Skipped {skipped_depth} file(s) with insufficient folder depth.", lines)
    _emit(f"  Normal files: {len(normal)}  VA files: {len(va)}", lines)

    # ------------------------------------------------------------------
    # Pass 1: normal artist folders — set both artist and albumartist.
    # ------------------------------------------------------------------
    _emit("", lines)
    _emit("--- Pass 1: fixing artist / albumartist for normal folders ---", lines)

    p1_fixed = 0
    p1_skipped = 0
    p1_failed = 0

    for idx, audio_path in enumerate(normal):
        if limit is not None and total_fixed >= limit:
            _emit(f"  ... limit of {limit} reached, stopping Pass 1 early.", lines)
            break

        if idx % 100 == 0 and idx:
            _emit(
                f"  ... scanned {idx}/{len(normal)} files ({p1_fixed} fixed so far)",
                lines,
            )

        canonical_artist = _artist_dir_for(audio_path, root)

        try:
            mf = MutagenFile(audio_path, easy=True)
        except Exception as exc:
            _emit(f"  ERROR reading {audio_path.name}: {exc}", lines)
            p1_failed += 1
            continue

        if mf is None:
            p1_skipped += 1
            continue

        def _first(key: str) -> str:
            val = mf.get(key)
            return str(val[0]).strip() if val else ""

        current_artist = _first("artist")
        current_albumartist = _first("albumartist")
        needs_artist = current_artist != canonical_artist
        needs_albumartist = current_albumartist != canonical_artist

        if not needs_artist and not needs_albumartist:
            continue

        changes: list[str] = []
        if needs_artist:
            changes.append(f"artist: {current_artist!r} → {canonical_artist!r}")
        if needs_albumartist:
            changes.append(f"albumartist: {current_albumartist!r} → {canonical_artist!r}")

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

    for idx, audio_path in enumerate(va):
        if limit is not None and total_fixed >= limit:
            _emit(f"  ... limit of {limit} reached, skipping rest of Pass 2.", lines)
            break

        if idx % 100 == 0 and idx:
            _emit(
                f"  ... scanned {idx}/{len(va)} VA files ({p2_fixed} fixed so far)",
                lines,
            )

        canonical_artist = _artist_dir_for(audio_path, root)  # e.g. "Various Artists"
        track_artist = _parse_artist_from_stem(audio_path.stem)
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

        def _firstv(key: str) -> str:
            val = mf.get(key)
            return str(val[0]).strip() if val else ""

        current_artist = _firstv("artist")
        current_albumartist = _firstv("albumartist")

        # artist → parsed from filename; albumartist → VA folder name
        needs_artist = current_artist != track_artist
        needs_albumartist = current_albumartist != canonical_artist

        if not needs_artist and not needs_albumartist:
            continue

        changes = []
        if needs_artist:
            changes.append(f"artist: {current_artist!r} → {track_artist!r}")
        if needs_albumartist:
            changes.append(f"albumartist: {current_albumartist!r} → {canonical_artist!r}")

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

    _emit(
        f"  => Pass 2 done. fixed={p2_fixed}  no_parse={p2_no_parse}"
        f"  skipped={p2_skipped}  failed={p2_failed}",
        lines,
    )

    _emit("", lines)
    _emit("=" * 72, lines)
    _emit(
        f"SUMMARY  total_fixed={total_fixed}"
        f"  pass1_fixed={p1_fixed}  pass2_fixed={p2_fixed}"
        f"  failed={p1_failed + p2_failed}  dry_run={dry_run}",
        lines,
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

    log_lines = fix_tags(root, dry_run=args.dry_run, limit=args.limit)

    log_ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_name = f"fix_audio_tags_{log_ts}.log"
    log_dir = root if os.access(root, os.W_OK) else Path.cwd()
    log_path = log_dir / log_name

    try:
        log_path.write_text("\n".join(log_lines) + "\n", encoding="utf-8")
        print(f"\nLog written to: {log_path}")
    except OSError as exc:
        print(f"\nWARNING: could not write log file: {exc}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
