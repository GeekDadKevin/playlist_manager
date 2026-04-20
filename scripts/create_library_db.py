#!/usr/bin/env python3
"""
create_library_db.py - Create the library_index.db database if it does not exist.
"""
import os
from pathlib import Path
from app.services.library_index import init_library_index

def main():
    db_path = Path("data/library_index.db")
    if db_path.exists():
        print(f"Database already exists at {db_path}. No action taken.")
        return
    print(f"Creating new library index database at {db_path}...")
    init_library_index(db_path)
    print("Database created.")

if __name__ == "__main__":
    main()
