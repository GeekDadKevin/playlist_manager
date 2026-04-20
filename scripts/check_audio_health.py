"""Check audio files for likely corruption.

NOTE: This tool uses only the library database for all audio file lists and health checks. Run the catalog refresh tool first to update the DB.

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
from app.services.library_index import (  # noqa: E402
    count_indexed_audio_files,
    list_audio_health_candidates,
    record_audio_health_result,
    record_library_tool_run,
    refresh_library_index,
    refresh_library_index_for_paths,
)
from app.services.tool_output import emit_console_line  # noqa: E402


def _emit(line: str, lines: list[str]) -> None:
    emit_console_line(line)
    lines.append(line)


def check_library(
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
    ffmpeg_path = find_ffmpeg_executable()
    library_index_db = str(
        db_path
        or os.getenv(
            "LIBRARY_INDEX_DB_PATH",
            Path(__file__).resolve().parent.parent / "data" / "library_index.db",
        )
    )

    _emit(
        "check_audio_health  "
        f"root={root}  dry_run={dry_run}  limit={limit}  "
        f"full_scan={full_scan}  started={started}",
        lines,
    )
    _emit("=" * 72, lines)
    if dry_run:
        _emit(
            "[DRY-RUN] This tool is read-only; dry-run behaves the same as a normal scan.",
            lines,
        )

    if ffmpeg_path:
        _emit(f"  Validation mode: ffmpeg decode check ({ffmpeg_path})", lines)
    else:
        _emit("  Validation mode: mutagen parser only (ffmpeg not found)", lines)
        _emit(
            "  WARN: parser-only mode can miss corruption that ffmpeg decode would catch.",
            lines,
        )

    if selected_paths is not None:
        audio_files = (
            selected_paths[:limit] if limit is not None else list(selected_paths)
        )
        total_found = len(audio_files)
        _emit(f"PROGRESS: using explicit selection of {total_found} audio file(s).", lines)
    else:
        _emit(f"PROGRESS: querying DB for indexed audio files...", lines)
        total_found = count_indexed_audio_files(library_index_db, root)
        _emit(f"PROGRESS: selecting candidate files for health check...", lines)
        audio_files = list_audio_health_candidates(
            library_index_db,
            root,
            force_full=full_scan,
            limit=limit,
        )
        _emit(f"PROGRESS: found {total_found} indexed audio file(s); scanning {len(audio_files)} candidate file(s) (from DB).", lines)

    ok_count = 0
    warning_count = 0
    error_count = 0

    if audio_files:
        _emit(f"PROGRESS: starting audio health check for {len(audio_files)} files...", lines)
    for index, audio_path in enumerate(audio_files, start=1):
        relative_path = audio_path.relative_to(root)
        if index == 1 or index % 100 == 0 or index == len(audio_files):
            _emit(f"PROGRESS: checked {index}/{len(audio_files)} files (errors={error_count}, warnings={warning_count})", lines)
        _emit(f"CHECK: {index}/{len(audio_files)}  {relative_path}", lines)

        result = check_audio_file(audio_path, ffmpeg_path=ffmpeg_path)
        record_audio_health_result(
            library_index_db,
            audio_path,
            status=result.status,
            message=result.message,
            root=root,
        )

        if result.status == "ok":
            ok_count += 1
            _emit(f"OK: {relative_path}", lines)
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
        f"errors={error_count}  ffmpeg={'yes' if ffmpeg_path else 'no'}  "
        f"dry_run={dry_run}  full_scan={full_scan}",
        lines,
    )
    if selected_paths is not None and not dry_run:
        refresh_library_index_for_paths(
            library_index_db,
            root,
            audio_files,
            scan_xml_sidecars=False,
        )
    record_library_tool_run(
        library_index_db,
        tool_name="check-audio",
        root=root,
        run_mode="full" if full_scan else "incremental",
        started_at=started_at,
        completed_at=datetime.datetime.now(datetime.UTC).isoformat(),
        scanned_count=len(audio_files),
        changed_count=0,
        error_count=error_count,
        result={
            "ok": ok_count,
            "warnings": warning_count,
            "errors": error_count,
            "dry_run": dry_run,
            "full_scan": full_scan,
        },
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
    parser.add_argument(
        "--full-scan",
        action="store_true",
        help="Revalidate every indexed audio file instead of only changed or unverified files.",
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

    log_lines, exit_code = check_library(
        root,
        dry_run=args.dry_run,
        limit=args.limit,
        full_scan=args.full_scan,
    )

    log_ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_name = f"check_audio_health_{log_ts}.log"
    log_dir = Path(__file__).resolve().parent.parent / "data" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / log_name

    try:
        log_path.write_text("\n".join(log_lines) + "\n", encoding="utf-8")
        print(f"\nLog written to: {log_path}")
    except OSError as exc:
        print(f"\nWARNING: could not write log file: {exc}", file=sys.stderr)

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
