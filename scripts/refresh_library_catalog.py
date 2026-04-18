"""Refresh the library catalog database from the current music root.

Run:
    uv run python scripts/refresh_library_catalog.py [MUSIC_ROOT]

MUSIC_ROOT defaults to NAVIDROME_MUSIC_ROOT from .env.
This tool is read-only with respect to audio and XML files. It rebuilds the
SQLite index used by the maintenance tools and browser reports.
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

from app.services.library_index import (  # noqa: E402
    get_library_report_counts,
    record_library_tool_run,
    refresh_library_index,
)
from app.services.tool_output import emit_console_line  # noqa: E402


def _emit(line: str, lines: list[str]) -> None:
    emit_console_line(line)
    lines.append(line)


def refresh_catalog(
    root: Path,
    *,
    db_path: str | Path | None = None,
    limit: int | None = None,
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

    _emit(f"refresh_library_catalog  root={root}  limit={limit}  started={started}", lines)
    _emit("=" * 72, lines)
    if limit is not None:
        _emit(
            f"NOTE: partial catalog refresh limited to the first {limit} XML and audio entries.",
            lines,
        )

    summary = refresh_library_index(
        library_index_db,
        root,
        progress_callback=lambda line: _emit(line, lines),
        limit=limit,
    )
    counts = get_library_report_counts(library_index_db, root)

    _emit(
        f"  Indexed audio={summary['scanned']}  xml={summary['xml_scanned']}  "
        f"changed={summary['changed']}  unchanged={summary['unchanged']}",
        lines,
    )
    _emit(
        f"  Report counts: missing_xml={counts['missing_xml']}  "
        f"incomplete_xml={counts['incomplete_xml']}  corrupted_audio={counts['corrupted_audio']}  "
        f"orphaned_xml={counts['orphaned_xml']}",
        lines,
    )
    _emit("", lines)
    _emit("=" * 72, lines)
    _emit(
        f"SUMMARY  indexed_audio={counts['indexed_audio']}  missing_xml={counts['missing_xml']}  "
        f"incomplete_xml={counts['incomplete_xml']}  corrupted_audio={counts['corrupted_audio']}  "
        f"orphaned_xml={counts['orphaned_xml']}",
        lines,
    )

    record_library_tool_run(
        library_index_db,
        tool_name="refresh-catalog",
        root=root,
        run_mode="partial" if limit is not None else "full",
        started_at=started_at,
        completed_at=datetime.datetime.now(datetime.UTC).isoformat(),
        scanned_count=summary["scanned"] + summary["xml_scanned"],
        changed_count=summary["changed"],
        error_count=0,
        result={
            "inventory": summary,
            "counts": counts,
            "limit": limit,
        },
    )
    return lines, 0


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
        help="Accepted for UI consistency. This tool always refreshes the catalog.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help=(
            "Refresh at most N XML entries and N audio entries "
            "for a bounded partial catalog update."
        ),
    )
    parser.add_argument(
        "--full-scan",
        action="store_true",
        help="Accepted for UI consistency. This tool always refreshes the full catalog.",
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

    log_lines, exit_code = refresh_catalog(root, limit=args.limit)

    log_ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_name = f"refresh_library_catalog_{log_ts}.log"
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
