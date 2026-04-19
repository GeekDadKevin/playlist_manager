"""Run a selected-track catalog batch action.

Run:
    uv run python scripts/run_catalog_batch.py [MUSIC_ROOT]
        --action ACTION --relative-path RELPATH [...]

MUSIC_ROOT defaults to NAVIDROME_MUSIC_ROOT from .env.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=False)

from app.services.library_catalog import run_catalog_batch_action  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "root",
        nargs="?",
        default=os.getenv("NAVIDROME_MUSIC_ROOT", ""),
        help="Music library root. Defaults to NAVIDROME_MUSIC_ROOT from .env.",
    )
    parser.add_argument(
        "--action",
        required=True,
        help="Catalog batch action id, such as check-audio or fix-tags.",
    )
    parser.add_argument(
        "--relative-path",
        action="append",
        default=[],
        dest="relative_paths",
        help="Selected relative audio path. Pass once per selected track.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview the selected batch without writing changes.",
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

    try:
        result = run_catalog_batch_action(
            {
                "NAVIDROME_MUSIC_ROOT": str(root),
                "LIBRARY_INDEX_DB_PATH": str(
                    os.getenv(
                        "LIBRARY_INDEX_DB_PATH",
                        Path(__file__).resolve().parent.parent
                        / "data"
                        / "library_index.db",
                    )
                ),
                "DATA_DIR": str(
                    os.getenv(
                        "DATA_DIR",
                        Path(__file__).resolve().parent.parent / "data",
                    )
                ),
                **os.environ,
            },
            action=args.action,
            relative_paths=args.relative_paths,
            dry_run=args.dry_run,
        )
    except Exception as exc:
        print(f"ERROR: {exc}")
        return 1

    print(result.get("summary_line") or "SUMMARY  completed")
    return int(result.get("exit_code", 0) or 0)


if __name__ == "__main__":
    raise SystemExit(main())
