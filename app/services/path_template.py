from __future__ import annotations

from pathlib import Path

_AUDIO_EXTENSIONS = {".aac", ".flac", ".m4a", ".mp3", ".ogg", ".opus", ".wav"}


def apply_audio_extension(path: Path, ext: str | None) -> Path:
    if not ext:
        return path

    normalized_ext = ext if ext.startswith(".") else f".{ext}"
    current_suffix = path.suffix.lower()
    if current_suffix == normalized_ext.lower():
        return path
    if current_suffix in _AUDIO_EXTENSIONS:
        return path.with_suffix(normalized_ext)
    return Path(f"{path}{normalized_ext}")


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
    return apply_audio_extension(base, ext)
