#!/usr/bin/env python3
"""
(Re)Create and populate the library_index.db database from NAVIDROME_MUSIC_ROOT.
"""
import sys
import os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from app.services.library_index import init_library_index, refresh_library_index


import argparse

def main():
    parser = argparse.ArgumentParser(description="(Re)Create and populate the library_index.db database from NAVIDROME_MUSIC_ROOT.")
    parser.add_argument("--overwrite", action="store_true", help="Delete the existing database before creating a new one.")
    args = parser.parse_args()

    db_path = Path("data/library_index.db")
    music_root = os.getenv("NAVIDROME_MUSIC_ROOT", "/navidrome/root")
    music_root_path = Path(music_root).expanduser().resolve()

    if args.overwrite and db_path.exists():
        print(f"Overwrite enabled: deleting {db_path} ...")
        db_path.unlink()

    print(f"(Re)Creating and populating library index database at {db_path} from {music_root_path} ...")
    init_library_index(db_path)
    summary = refresh_library_index(db_path, music_root_path)
    print(f"Database created and populated. {summary['scanned']} files scanned, {summary['changed']} new or updated.")

if __name__ == "__main__":
    main()
