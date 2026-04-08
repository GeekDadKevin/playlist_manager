from __future__ import annotations

import re
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from werkzeug.utils import secure_filename

_BRACKET_PREFIX_RE = re.compile(r"^\s*\[[^\]]+\]\s*")
_RECURRING_DATE_RE = re.compile(
    r",?\s*(?:week|day)\s+of\s+\d{4}-\d{2}-\d{2}(?:\s+[A-Za-z]{3})?",
    re.IGNORECASE,
)
_RECURRING_FOR_RE = re.compile(r"\s+for\s+[^,]+$", re.IGNORECASE)


def export_navidrome_playlist(
    playlist_dir: str | Path,
    playlist_name: str,
    sync_results: list[dict[str, Any]],
) -> dict[str, Any]:
    folder = Path(playlist_dir)
    folder.mkdir(parents=True, exist_ok=True)

    stem, is_recurring = _build_playlist_stem(playlist_name)
    target_path = folder / f"{stem}.m3u"
    removed_files = _remove_recurring_variants(folder, stem, target_path) if is_recurring else []

    lines = ["#EXTM3U"]
    seen_paths: set[str] = set()
    seen_missing: set[tuple[str, str]] = set()
    playable_count = 0
    missing_count = 0

    for item in sync_results:
        track = item.get("track") if isinstance(item, dict) else {}
        artist = _text_value(track.get("artist")) if isinstance(track, dict) else ""
        title = _text_value(track.get("title")) if isinstance(track, dict) else ""
        media_path = _extract_media_path(item)
        label = (
            " - ".join(part for part in (artist, title) if part)
            or title
            or Path(media_path).stem
            or "Unknown track"
        )

        if media_path:
            if media_path in seen_paths:
                continue

            seen_paths.add(media_path)
            duration = track.get("duration_seconds") if isinstance(track, dict) else None
            duration_value = str(int(duration)) if isinstance(duration, int | float) else "-1"

            lines.append(f"#EXTINF:{duration_value},{label}")
            lines.append(media_path)
            playable_count += 1
            continue

        source = _text_value(track.get("source")) if isinstance(track, dict) else ""
        missing_key = (label, source)
        if missing_key in seen_missing:
            continue

        seen_missing.add(missing_key)
        lines.append(f"# MISSING: {label}")
        if source:
            lines.append(f"# SOURCE: {source}")
        missing_count += 1

    if len(lines) == 1:
        return {
            "configured": True,
            "written": False,
            "playlist_name": playlist_name,
            "target_path": str(target_path),
            "filename": target_path.name,
            "entry_count": 0,
            "playable_count": 0,
            "missing_count": 0,
            "is_recurring": is_recurring,
            "reason": "No tracks were available for playlist export.",
            "removed_files": removed_files,
        }

    existed_before = target_path.exists()
    target_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    return {
        "configured": True,
        "written": True,
        "playlist_name": playlist_name,
        "target_path": str(target_path),
        "filename": target_path.name,
        "entry_count": playable_count + missing_count,
        "playable_count": playable_count,
        "missing_count": missing_count,
        "is_recurring": is_recurring,
        "overwritten": existed_before or bool(removed_files),
        "removed_files": removed_files,
        "reason": (
            f"{missing_count} track(s) are still pending Octo-Fiesta download."
            if missing_count
            else ""
        ),
    }


def _extract_media_path(item: dict[str, Any]) -> str:
    for candidate in (
        item.get("resolved_match", {}).get("path"),
        item.get("match", {}).get("path"),
        item.get("track", {}).get("source"),
    ):
        normalized = _normalize_media_path(candidate)
        if normalized:
            return normalized

    return ""


def _normalize_media_path(value: Any) -> str:
    text = _text_value(value).replace("\\", "/").strip()
    if not text:
        return ""

    parsed = urlparse(text)
    if parsed.scheme in {"http", "https"}:
        return ""

    return text


def _build_playlist_stem(playlist_name: str) -> tuple[str, bool]:
    cleaned_name = _BRACKET_PREFIX_RE.sub("", playlist_name).strip() or "playlist"
    lowered = cleaned_name.lower()
    is_recurring = any(marker in lowered for marker in ("daily", "weekly", "day of", "week of"))

    if is_recurring:
        cleaned_name = _RECURRING_DATE_RE.sub("", cleaned_name).strip(" ,-_")
        cleaned_name = _RECURRING_FOR_RE.sub("", cleaned_name).strip(" ,-_")

    stem = secure_filename(cleaned_name).replace("_", "-").strip(".-").lower() or "playlist"
    return stem, is_recurring


def _remove_recurring_variants(
    folder: Path,
    stable_stem: str,
    target_path: Path,
) -> list[str]:
    removed: list[str] = []

    for candidate in folder.glob("*.m3u"):
        if candidate == target_path:
            continue

        normalized_stem = candidate.stem.replace("_", "-").strip(".-").lower()
        if normalized_stem == stable_stem or normalized_stem.startswith(f"{stable_stem}-"):
            candidate.unlink(missing_ok=True)
            removed.append(str(candidate))

    return removed


def _text_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, int | float):
        return str(value)
    return ""
