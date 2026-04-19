"""Fix or backfill missing cover.jpg for every album directory in the music root.

NOTE: This tool uses only the library database for album discovery. Run the catalog refresh tool first to update the DB.

Run:
    uv run python scripts/fix_album_covers.py [MUSIC_ROOT] [--dry-run] [--limit N]

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

# Import the new function after dotenv
from app.services.library_index import list_album_dirs_from_db


def find_album_dirs(music_root: Path, db_path: str | Path) -> set[Path]:
    """Return all unique album directories from the library database."""
    return set(list_album_dirs_from_db(db_path, music_root))


def fix_album_covers(
    music_root: Path,
    db_path: str | Path,
    dry_run: bool = False,
    limit: int | None = None,
) -> list[str]:
    lines = []
    album_dirs = sorted(find_album_dirs(music_root, db_path))
    if limit is not None:
        album_dirs = album_dirs[:limit]
    lines.append(
        f"Scanning {len(album_dirs)} album directories for missing cover.jpg (from DB)..."
    )
    fixed = 0
    for album_dir in album_dirs:
        cover_path = album_dir / "cover.jpg"
        if cover_path.exists():
            continue
        if dry_run:
            lines.append(f"Would create: {cover_path}")
        else:
            ensure_cover_art(album_dir)
            lines.append(f"Created: {cover_path}")
            fixed += 1
    lines.append(f"Done. {fixed} cover.jpg files created.")
    return lines


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fix or backfill missing cover.jpg for every album directory (uses DB only, refresh catalog first)."
    )
    parser.add_argument(
        "music_root",
        nargs="?",
        default=os.getenv("NAVIDROME_MUSIC_ROOT", "/navidrome/root"),
        help="Music root directory",
    )
    parser.add_argument(
        "--db-path",
        default=os.getenv(
            "LIBRARY_INDEX_DB_PATH",
            str(Path(__file__).resolve().parent.parent / "data" / "library_index.db"),
        ),
        help="Path to library index database",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Preview only, do not write files"
    )
    parser.add_argument(
        "--limit", type=int, default=None, help="Limit number of albums to process"
    )
    args = parser.parse_args()
    music_root = Path(args.music_root).expanduser().resolve()
    db_path = Path(args.db_path).expanduser().resolve()
    lines = fix_album_covers(
        music_root, db_path, dry_run=args.dry_run, limit=args.limit
    )
    for line in lines:
        print(line)


if __name__ == "__main__":
    main()
