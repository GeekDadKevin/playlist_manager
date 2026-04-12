from __future__ import annotations

from pathlib import Path


def build_download_path(
    root: str | Path,
    template: str,
    *,
    artist: str,
    album: str,
    title: str,
    track_number: int,
    ext: str | None = None,
) -> Path:
    values = {
        "artist": artist,
        "album": album,
        "title": title,
        "track": str(track_number),
    }
    formatted = template.format(**values).strip().lstrip("/\\")
    base = Path(root) / formatted
    if ext:
        if base.suffix:
            return base
        return base.with_suffix(ext)
    return base
