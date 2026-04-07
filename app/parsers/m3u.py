from __future__ import annotations

from pathlib import Path

from app.models import PlaylistTrack


def parse_m3u(content: str) -> list[PlaylistTrack]:
    tracks: list[PlaylistTrack] = []
    pending_duration: int | None = None
    pending_label = ""

    for raw_line in content.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#EXTM3U"):
            continue

        if line.startswith("#EXTINF:"):
            metadata = line.removeprefix("#EXTINF:")
            duration_text, _, label = metadata.partition(",")
            pending_duration = _safe_int(duration_text)
            pending_label = label.strip()
            continue

        if line.startswith("#"):
            continue

        label = pending_label or Path(line).stem
        artist, title = _split_artist_title(label)
        tracks.append(
            PlaylistTrack(
                title=title,
                artist=artist,
                duration_seconds=pending_duration,
                source=line,
            )
        )
        pending_duration = None
        pending_label = ""

    return tracks


def _split_artist_title(label: str) -> tuple[str, str]:
    if " - " not in label:
        return "", label.strip()

    artist, title = label.split(" - ", 1)
    return artist.strip(), title.strip()


def _safe_int(value: str) -> int | None:
    try:
        parsed = int(value)
    except ValueError:
        return None

    return None if parsed < 0 else parsed
