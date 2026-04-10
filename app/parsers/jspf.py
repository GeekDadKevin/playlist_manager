from __future__ import annotations

import json
from typing import Any
from urllib.parse import urlparse

from app.models import PlaylistTrack


def parse_jspf(content: str | dict | list) -> list[PlaylistTrack]:
    payload = json.loads(content.lstrip("\ufeff").strip()) if isinstance(content, str) else content
    playlist_data, items = _extract_playlist_items(payload)

    tracks: list[PlaylistTrack] = []
    for item in items:
        if isinstance(item, str):
            identifier = item.strip()
            title = _title_from_identifier(identifier).strip() or identifier
            if not title:
                continue
            tracks.append(PlaylistTrack(title=title, source=identifier))
            continue

        if not isinstance(item, dict):
            continue

        extension_metadata = _track_extension_metadata(item)
        identifier = _text_value(item.get("identifier") or extension_metadata.get("identifier"))
        title = (
            _text_value(item.get("title"))
            or _text_value(item.get("name"))
            or _text_value(extension_metadata.get("title"))
            or _title_from_identifier(identifier)
        ).strip()
        if not title:
            continue

        artist = (
            _text_value(item.get("creator"))
            or _text_value(item.get("artist"))
            or _text_value(extension_metadata.get("artist"))
            or _text_value(extension_metadata.get("artists"))
        ).strip()
        album = (
            _text_value(item.get("album"))
            or _text_value(item.get("albumtitle"))
            or _text_value(extension_metadata.get("album"))
            or _text_value(extension_metadata.get("release_title"))
        ).strip()
        duration_seconds = _ms_to_seconds(
            item.get("duration") or extension_metadata.get("duration")
        )
        annotation = (
            _text_value(item.get("annotation"))
            or _text_value(item.get("description"))
            or _text_value(playlist_data.get("annotation"))
        )

        tracks.append(
            PlaylistTrack(
                title=title,
                artist=artist,
                album=album,
                duration_seconds=duration_seconds,
                source=identifier.strip(),
                extra={"annotation": annotation},
            )
        )

    return tracks


def _extract_playlist_items(payload: Any) -> tuple[dict[str, Any], list[Any]]:
    if isinstance(payload, list):
        return {}, payload
    if not isinstance(payload, dict):
        raise ValueError("JSPF content must be a JSON object or track list.")

    for candidate in _playlist_candidates(payload):
        items = _coerce_item_list(candidate)
        if items is not None:
            return candidate, items

    raise ValueError("JSPF playlist payload is missing track data.")


def _playlist_candidates(payload: dict[str, Any]) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []

    playlist = payload.get("playlist")
    if isinstance(playlist, dict):
        candidates.append(playlist)

    wrapped_payload = payload.get("payload")
    if isinstance(wrapped_payload, dict):
        wrapped_playlist = wrapped_payload.get("playlist")
        if isinstance(wrapped_playlist, dict):
            candidates.append(wrapped_playlist)
        candidates.append(wrapped_payload)

    candidates.append(payload)
    return candidates


def _coerce_item_list(value: dict[str, Any]) -> list[Any] | None:
    for key in ("track", "tracks", "items"):
        items = value.get(key)
        if isinstance(items, list):
            return items
        if isinstance(items, dict):
            return [items]
    return None


def _track_extension_metadata(item: dict[str, Any]) -> dict[str, Any]:
    extension = item.get("extension")
    if not isinstance(extension, dict):
        return {}

    for value in extension.values():
        if isinstance(value, dict):
            additional = value.get("additional_metadata")
            if isinstance(additional, dict):
                return {**value, **additional}
            return value
    return {}


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
