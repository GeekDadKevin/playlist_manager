from __future__ import annotations

import json
from urllib.parse import urlparse

from app.models import PlaylistTrack


def parse_jspf(content: str | dict) -> list[PlaylistTrack]:
    payload = json.loads(content) if isinstance(content, str) else content
    playlist = payload.get("playlist", {})
    items = playlist.get("track", [])

    tracks: list[PlaylistTrack] = []
    for item in items:
        title = (item.get("title") or _title_from_identifier(item.get("identifier", ""))).strip()
        if not title:
            continue

        tracks.append(
            PlaylistTrack(
                title=title,
                artist=(item.get("creator") or "").strip(),
                album=(item.get("album") or "").strip(),
                duration_seconds=_ms_to_seconds(item.get("duration")),
                source=(item.get("identifier") or "").strip(),
                extra={"annotation": item.get("annotation", "")},
            )
        )

    return tracks


def _ms_to_seconds(value: object) -> int | None:
    if value in (None, "") or not isinstance(value, str | int | float):
        return None

    try:
        return round(int(value) / 1000)
    except (TypeError, ValueError):
        return None


def _title_from_identifier(identifier: str) -> str:
    if not identifier:
        return ""

    parsed = urlparse(identifier)
    name = parsed.path.rsplit("/", maxsplit=1)[-1]
    return name.rsplit(".", maxsplit=1)[0].replace("%20", " ")
