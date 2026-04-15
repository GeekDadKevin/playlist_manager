"""Check audio files for likely corruption.

Runs a read-only validation pass across the music root. When ffmpeg is
available, each file is fully decoded to `/dev/null`/`NUL` so truncated frames
and container errors are caught. Without ffmpeg, the script falls back to a
metadata parser check via mutagen, which is weaker but still catches many
obvious bad files.

Run:
    uv run python scripts/check_audio_health.py [MUSIC_ROOT] [--limit N]

MUSIC_ROOT defaults to NAVIDROME_MUSIC_ROOT from .env.
A timestamped log is written to MUSIC_ROOT/check_audio_health_<timestamp>.log
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

from app.services.audio_health import (  # noqa: E402
    check_audio_file,
    find_ffmpeg_executable,
    iter_audio_files,
)


def _emit(line: str, lines: list[str]) -> None:
    print(line, flush=True)
    lines.append(line)


def check_library(
    root: Path,
    *,
    dry_run: bool = False,
    limit: int | None = None,
) -> tuple[list[str], int]:
    lines: list[str] = []
    started = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ffmpeg_path = find_ffmpeg_executable()

    _emit(
        f"check_audio_health  root={root}  dry_run={dry_run}  limit={limit}  started={started}",
        lines,
    )
    _emit("=" * 72, lines)
    if dry_run:
        _emit("[DRY-RUN] This tool is read-only; dry-run behaves the same as a normal scan.", lines)

    if ffmpeg_path:
        _emit(f"  Validation mode: ffmpeg decode check ({ffmpeg_path})", lines)
    else:
        _emit("  Validation mode: mutagen parser only (ffmpeg not found)", lines)
        _emit("  WARN: parser-only mode can miss corruption that ffmpeg decode would catch.", lines)

    audio_files = iter_audio_files(root)
    total_found = len(audio_files)
    if limit is not None:
        audio_files = audio_files[:limit]

    _emit(f"  Found {total_found} audio file(s); scanning {len(audio_files)} file(s).", lines)

    ok_count = 0
    warning_count = 0
    error_count = 0

    for index, audio_path in enumerate(audio_files, start=1):
        if index % 100 == 0:
            _emit(
                f"  ... scanned {index}/{len(audio_files)} files "
                f"(errors={error_count}, warnings={warning_count})",
                lines,
            )

        result = check_audio_file(audio_path, ffmpeg_path=ffmpeg_path)
        relative_path = audio_path.relative_to(root)

        if result.status == "ok":
            ok_count += 1
            continue

        if result.status == "warning":
            warning_count += 1
            _emit(f"WARN: {relative_path}  [{result.message}]", lines)
            continue

        error_count += 1
        _emit(f"ERROR: {relative_path}  [{result.message}]", lines)

    if error_count == 0 and warning_count == 0:
        _emit("  No suspicious audio files were found in the scanned set.", lines)

    _emit("", lines)
    _emit("=" * 72, lines)
    _emit(
        f"SUMMARY  scanned={len(audio_files)}  ok={ok_count}  warnings={warning_count}  "
        f"errors={error_count}  ffmpeg={'yes' if ffmpeg_path else 'no'}  dry_run={dry_run}",
        lines,
    )
    return lines, 1 if error_count else 0


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
        help="Accepted for UI consistency. This tool is already read-only.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Only scan the first N audio files after sorting by path.",
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

    log_lines, exit_code = check_library(root, dry_run=args.dry_run, limit=args.limit)

    log_ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_name = f"check_audio_health_{log_ts}.log"
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
