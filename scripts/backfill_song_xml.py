from __future__ import annotations

import argparse
import os
from pathlib import Path

from app.services.song_metadata import backfill_missing_song_xml
from dotenv import load_dotenv


def _default_root() -> str:
    return os.getenv("NAVIDROME_MUSIC_ROOT") or ""


def main() -> int:
    repo_root = Path(__file__).resolve().parent.parent
    load_dotenv(repo_root / ".env", override=False)

    parser = argparse.ArgumentParser(
        description=(
            "Create missing Navidrome-style song XML sidecars and backfill FLAC tags "
            "for local music files."
        )
    )
    parser.add_argument(
        "root",
        nargs="?",
        default=_default_root(),
        help="Music library root to scan. Defaults to NAVIDROME_MUSIC_ROOT from .env.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Rewrite existing XML sidecars instead of skipping them.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be created without writing any files.",
    )
    args = parser.parse_args()

    if not args.root:
        print(
            "No music library root was provided. Set NAVIDROME_MUSIC_ROOT or pass a path."
        )
        return 1

    try:
        summary = backfill_missing_song_xml(
            args.root,
            overwrite=args.overwrite,
            dry_run=args.dry_run,
        )
    except ValueError as exc:
        print(f"Error: {exc}")
        return 1

    action = "Would create" if args.dry_run else "Created"
    tag_action = "would tag" if args.dry_run else "tagged"
    print(
        f"{action} {summary['created']} XML file(s); scanned {summary['scanned']} audio file(s), "
        f"{tag_action} {summary['tagged_flac']} FLAC file(s), "
        f"skipped {summary['skipped_existing']} existing sidecar(s), "
        f"failed {summary['failed']}."
    )

    if summary["written"]:
        label = "Planned" if args.dry_run else "Wrote"
        preview = summary["written"][:10]
        for item in preview:
            print(f"- {label}: {item}")
        remaining = len(summary["written"]) - len(preview)
        if remaining > 0:
            print(f"...and {remaining} more")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
