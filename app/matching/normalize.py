from __future__ import annotations

import re
import unicodedata

from app.models import PlaylistTrack

FEATURE_PATTERN = re.compile(r"\b(feat|ft|featuring)\.?\b", re.IGNORECASE)
NON_ALNUM_PATTERN = re.compile(r"[^a-z0-9]+")


def normalize_text(value: str) -> str:
    ascii_value = (
        unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii").lower()
    )
    ascii_value = FEATURE_PATTERN.sub("feat", ascii_value.replace("&", " and "))
    return " ".join(NON_ALNUM_PATTERN.sub(" ", ascii_value).split())


def build_search_queries(track: PlaylistTrack) -> list[str]:
    queries: list[str] = []

    artist = normalize_text(track.artist)
    title = normalize_text(track.title)
    album = normalize_text(track.album)

    if artist and title:
        queries.append(f"{artist} {title}")
    if artist and title and album:
        queries.append(f"{artist} {title} {album}")
    if title:
        queries.append(title)

    seen: set[str] = set()
    unique_queries: list[str] = []
    for query in queries:
        if query and query not in seen:
            seen.add(query)
            unique_queries.append(query)

    return unique_queries
