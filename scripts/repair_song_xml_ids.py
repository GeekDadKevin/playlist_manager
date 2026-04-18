"""Repair missing MBID and Deezer IDs in existing song XML sidecars.

NOTE: This tool uses only the library database for all audio file and XML sidecar lists. Run the catalog refresh tool first to update the DB.

Scans existing XML sidecars, looks for paired audio files, and backfills
recoverable identifier fields from embedded tags. This tool is useful after
you improve tagging logic or rebuild parts of the library and want a focused
pass over XML IDs without rewriting every sidecar.

Run:
    uv run python scripts/repair_song_xml_ids.py [MUSIC_ROOT] [--dry-run] [--limit N]

MUSIC_ROOT defaults to NAVIDROME_MUSIC_ROOT from .env.
A timestamped log is written to MUSIC_ROOT/repair_song_xml_ids_<timestamp>.log
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

from app.services.library_index import (  # noqa: E402
    list_xml_id_repair_candidates,
    record_library_tool_run,
    refresh_library_index,
    refresh_library_index_for_paths,
)
from app.services.song_metadata import repair_song_metadata_xml_ids  # noqa: E402
from app.services.tool_output import emit_console_line  # noqa: E402


def _emit(line: str, lines: list[str]) -> None:
    emit_console_line(line)
    lines.append(line)


def repair_xml_ids(
    root: Path,
    *,
    dry_run: bool = False,
    limit: int | None = None,
    full_scan: bool = False,
    db_path: str | Path | None = None,
    selected_audio_paths: list[Path] | None = None,
) -> tuple[list[str], int]:
    started_at = datetime.datetime.now(datetime.UTC).isoformat()
    started = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines: list[str] = []
    library_index_db = str(
        db_path
        or os.getenv(
            "LIBRARY_INDEX_DB_PATH",
            Path(__file__).resolve().parent.parent / "data" / "library_index.db",
        )
    )

    _emit(
        "repair_song_xml_ids  "
        f"root={root}  dry_run={dry_run}  limit={limit}  full_scan={full_scan}  started={started}",
        lines,
    )
    _emit("=" * 72, lines)

    if selected_audio_paths is not None:
        chosen_paths = (
            selected_audio_paths[:limit] if limit is not None else list(selected_audio_paths)
        )
        candidates = [
            (audio_path.with_suffix(".xml"), audio_path)
            for audio_path in chosen_paths
            if audio_path.with_suffix(".xml").exists()
        ]
        _emit(
            f"  Using explicit selection of {len(chosen_paths)} audio file(s); "
            f"{len(candidates)} XML sidecar(s) are available for ID repair.",
            lines,
        )
    else:
        candidates = list_xml_id_repair_candidates(
            library_index_db,
            root,
            force_full=full_scan,
            limit=limit,
        )
        _emit(f"  Found {len(candidates)} XML sidecar(s) to inspect for ID repair (from DB).", lines)

    summary = repair_song_metadata_xml_ids(
        root,
        dry_run=dry_run,
        limit=limit,
        xml_paths=[xml_path for xml_path, _audio_path in candidates],
        progress_callback=lambda line: _emit(line, lines),
    )

    for item in summary["written"][:50]:
        action = "[DRY-RUN] would update" if dry_run else "UPDATED"
        relative = Path(item).relative_to(root)
        _emit(f"  {action}: {relative}", lines)

    remaining = len(summary["written"]) - min(len(summary["written"]), 50)
    if remaining > 0:
        _emit(f"  ... and {remaining} more updated XML file(s)", lines)

    for item in summary["unresolved_items"][:50]:
        relative = Path(item["xml_path"]).relative_to(root)
        missing = ", ".join(item["missing_fields"])
        _emit(f"WARN: unresolved IDs in {relative}  [missing: {missing}]", lines)

    unresolved_remaining = len(summary["unresolved_items"]) - min(
        len(summary["unresolved_items"]),
        50,
    )
    if unresolved_remaining > 0:
        _emit(f"  ... and {unresolved_remaining} more unresolved XML file(s)", lines)

    _emit("", lines)
    _emit("=" * 72, lines)
    _emit(
        f"SUMMARY  scanned={summary['scanned']}  updated={summary['updated']}  "
        f"unchanged={summary['unchanged']}  unresolved={summary['unresolved']}  "
        f"failed={summary['failed']}  dry_run={dry_run}  full_scan={full_scan}",
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
        tool_name="repair-xml-ids",
        root=root,
        run_mode="full" if full_scan else "incremental",
        started_at=started_at,
        completed_at=datetime.datetime.now(datetime.UTC).isoformat(),
        scanned_count=summary["scanned"],
        changed_count=summary["updated"],
        error_count=summary["failed"],
        result={
            "inventory": inventory_summary,
            "summary": summary,
            "dry_run": dry_run,
            "full_scan": full_scan,
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
        help="Show what would be updated without writing any XML files.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Only scan the first N XML sidecars after sorting by path.",
    )
    parser.add_argument(
        "--full-scan",
        action="store_true",
        help="Recheck all indexed XML sidecars instead of only unresolved or stale candidates.",
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

    log_lines, exit_code = repair_xml_ids(
        root,
        dry_run=args.dry_run,
        limit=args.limit,
        full_scan=args.full_scan,
    )

    log_ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_name = f"repair_song_xml_ids_{log_ts}.log"
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
