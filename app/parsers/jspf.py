from __future__ import annotations

import json
from urllib.parse import urlparse

from app.models import PlaylistTrack


def parse_jspf(content: str | dict) -> list[PlaylistTrack]:
    payload = json.loads(content.lstrip("\ufeff").strip()) if isinstance(content, str) else content
    if not isinstance(payload, dict):
        raise ValueError("JSPF content must be a JSON object.")

    playlist = payload.get("playlist", {})
    if not isinstance(playlist, dict):
        raise ValueError("JSPF playlist payload is missing the `playlist` object.")

    items = playlist.get("track", [])
    if isinstance(items, dict):
        items = [items]
    elif not isinstance(items, list):
        items = []

    tracks: list[PlaylistTrack] = []
    for item in items:
        if not isinstance(item, dict):
            continue

        identifier = _text_value(item.get("identifier"))
        title = (_text_value(item.get("title")) or _title_from_identifier(identifier)).strip()
        if not title:
            continue

        tracks.append(
            PlaylistTrack(
                title=title,
                artist=_text_value(item.get("creator")).strip(),
                album=_text_value(item.get("album")).strip(),
                duration_seconds=_ms_to_seconds(item.get("duration")),
                source=identifier.strip(),
                extra={"annotation": _text_value(item.get("annotation"))},
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


def _text_value(value: object) -> str:
    if value is None:
        return ""

    if isinstance(value, str):
        return value

    if isinstance(value, int | float):
        return str(value)

    if isinstance(value, list):
        for item in value:
            text = _text_value(item)
            if text:
                return text
        return ""

    if isinstance(value, dict):
        for key in ("value", "@value", "identifier", "id", "name", "title", "text"):
            text = _text_value(value.get(key))
            if text:
                return text

    return ""


def _title_from_identifier(identifier: str) -> str:
    if not identifier:
        return ""

    parsed = urlparse(identifier)
    name = parsed.path.rsplit("/", maxsplit=1)[-1]
    return name.rsplit(".", maxsplit=1)[0].replace("%20", " ")
