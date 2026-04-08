from __future__ import annotations

import csv
import re
from io import StringIO
from pathlib import Path

from app.models import PlaylistTrack

_TRACK_NUMBER_PREFIX = re.compile(r"^\s*\d{1,3}\s*[-._]\s*")
_DUPLICATE_SUFFIX = re.compile(r"\s*\(\d+\)$")


def parse_navidrome_missing_csv(content: str) -> list[PlaylistTrack]:
    tracks: list[PlaylistTrack] = []
    reader = csv.reader(StringIO(content))

    for row in reader:
        entry = ",".join(row).strip().strip('"')
        if not entry or entry.lower() in {"path", "file", "filepath", "missing_path"}:
            continue

        track = _track_from_missing_path(entry)
        if track.title:
            tracks.append(track)

    return tracks


def _track_from_missing_path(raw_path: str) -> PlaylistTrack:
    normalized_path = raw_path.replace("\\", "/").strip()
    parts = [part.strip() for part in normalized_path.split("/") if part.strip()]

    filename = parts[-1] if parts else normalized_path
    path_artist = parts[-3] if len(parts) >= 3 else ""
    path_album = parts[-2] if len(parts) >= 2 else ""
    stem = Path(filename).stem.strip()

    artist = path_artist
    album = path_album
    title = stem

    tokens = [token.strip() for token in stem.split(" - ")]
    if len(tokens) >= 3 and not tokens[0].isdigit():
        artist = tokens[0] or artist
        album = tokens[1] or album
        title = " - ".join(tokens[2:])

    title = _clean_title(title)

    return PlaylistTrack(
        title=title,
        artist=artist,
        album=album,
        source=raw_path,
        extra={"navidrome_missing": True},
    )


def _clean_title(title: str) -> str:
    cleaned = _TRACK_NUMBER_PREFIX.sub("", title).strip()
    cleaned = _DUPLICATE_SUFFIX.sub("", cleaned).strip()
    return cleaned
