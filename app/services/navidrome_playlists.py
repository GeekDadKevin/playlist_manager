from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from flask import current_app, has_app_context
from werkzeug.utils import secure_filename

from app.matching.normalize import normalize_text

_BRACKET_PREFIX_RE = re.compile(r"^\s*\[[^\]]+\]\s*")
_RECURRING_DATE_RE = re.compile(
    r",?\s*(?:week|day)\s+of\s+\d{4}-\d{2}-\d{2}(?:\s+[A-Za-z]{3})?",
    re.IGNORECASE,
)
_RECURRING_FOR_RE = re.compile(r"\s+for\s+[^,]+$", re.IGNORECASE)
_MEDIA_EXTENSIONS = {
    ".aac",
    ".aif",
    ".aiff",
    ".alac",
    ".ape",
    ".flac",
    ".m4a",
    ".mp3",
    ".ogg",
    ".oga",
    ".opus",
    ".wav",
    ".wma",
}


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
    media_roots, media_path_prefix = _load_media_path_settings()

    lines = ["#EXTM3U"]
    seen_paths: set[str] = set()
    seen_missing: set[tuple[str, str]] = set()
    playable_count = 0
    missing_count = 0

    for item in sync_results:
        track = item.get("track") if isinstance(item, dict) else {}
        artist = _text_value(track.get("artist")) if isinstance(track, dict) else ""
        title = _text_value(track.get("title")) if isinstance(track, dict) else ""
        media_path = _extract_media_path(
            item,
            playlist_dir=folder,
            media_roots=media_roots,
            media_path_prefix=media_path_prefix,
        )
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
        "reason": f"{missing_count} track(s) are still pending download." if missing_count else "",
    }


def _extract_media_path(
    item: dict[str, Any],
    *,
    playlist_dir: Path,
    media_roots: list[str],
    media_path_prefix: str,
) -> str:
    for source_name, candidate in (
        ("resolved_match", item.get("resolved_match", {}).get("path")),
        ("match", item.get("match", {}).get("path")),
        ("track", item.get("track", {}).get("source")),
    ):
        normalized = _normalize_media_path(
            candidate,
            playlist_dir=playlist_dir,
            media_roots=media_roots,
            media_path_prefix=media_path_prefix,
        )
        if not normalized:
            continue

        exists = _path_exists_when_resolved(
            candidate,
            normalized,
            playlist_dir=playlist_dir,
            media_roots=media_roots,
            prefer_playlist_relative=source_name == "track",
        )
        if source_name == "track":
            if exists is not True:
                continue
        elif exists is False:
            continue

        return normalized
    discovered = _discover_media_path_from_library(
        item,
        playlist_dir=playlist_dir,
        media_roots=media_roots,
        media_path_prefix=media_path_prefix,
    )
    if discovered:
        return discovered

    return ""


def _normalize_media_path(
    value: Any,
    *,
    playlist_dir: Path,
    media_roots: list[str],
    media_path_prefix: str,
) -> str:
    text = _text_value(value).replace("\\", "/").strip()
    if not text:
        return ""

    parsed = urlparse(text)
    if parsed.scheme in {"http", "https", "memory"}:
        return ""

    if text.startswith(("../", "./")):
        return text

    rewritten = _rewrite_from_media_roots(text, media_roots, media_path_prefix)
    if rewritten:
        return rewritten

    if _looks_like_absolute_path(text):
        try:
            relative = os.path.relpath(text, start=str(playlist_dir))
        except ValueError:
            return text
        return relative.replace("\\", "/")

    return _join_media_prefix(media_path_prefix, text.lstrip("/"))


def _discover_media_path_from_library(
    item: dict[str, Any],
    *,
    playlist_dir: Path,
    media_roots: list[str],
    media_path_prefix: str,
) -> str:
    track = item.get("track") if isinstance(item, dict) else {}
    if not isinstance(track, dict):
        return ""

    candidate = _find_existing_media_file(
        title=_text_value(track.get("title")),
        artist=_text_value(track.get("artist")),
        album=_text_value(track.get("album")),
        source=_text_value(track.get("source")),
        media_roots=media_roots,
    )
    if candidate is None:
        return ""

    return _normalize_media_path(
        str(candidate),
        playlist_dir=playlist_dir,
        media_roots=media_roots,
        media_path_prefix=media_path_prefix,
    )


def _find_existing_media_file(
    *,
    title: str,
    artist: str,
    album: str,
    source: str,
    media_roots: list[str],
) -> Path | None:
    title_key = normalize_text(title)
    if not title_key:
        return None

    best_path: Path | None = None
    best_score = -1

    for directory in _candidate_library_directories(
        artist=artist,
        album=album,
        source=source,
        media_roots=media_roots,
    ):
        try:
            files = directory.rglob("*")
        except OSError:
            continue

        for candidate in files:
            if not candidate.is_file() or candidate.suffix.casefold() not in _MEDIA_EXTENSIONS:
                continue

            score = _score_media_candidate(
                candidate,
                title=title,
                artist=artist,
                album=album,
            )
            if score > best_score:
                best_score = score
                best_path = candidate

    return best_path if best_score >= 100 else None


def _candidate_library_directories(
    *,
    artist: str,
    album: str,
    source: str,
    media_roots: list[str],
) -> list[Path]:
    directories: list[Path] = []
    seen: set[str] = set()

    def add_directory(path_value: Path) -> None:
        candidate = path_value.absolute() if not path_value.is_absolute() else path_value
        key = str(candidate).casefold()
        if key in seen or not candidate.exists() or not candidate.is_dir():
            return
        seen.add(key)
        directories.append(candidate)

    for root in media_roots:
        if not _looks_like_absolute_path(root):
            continue
        root_path = Path(root)
        if not root_path.exists() or not root_path.is_dir():
            continue

        hinted_parent = _source_parent_under_root(source, root_path)
        if hinted_parent is not None:
            add_directory(hinted_parent)

        artist_dir = _find_matching_child_directory(root_path, artist)
        if artist_dir is not None:
            if album:
                album_dir = _find_matching_child_directory(artist_dir, album)
                if album_dir is not None:
                    add_directory(album_dir)
            add_directory(artist_dir)

    return directories


def _source_parent_under_root(source: str, root_path: Path) -> Path | None:
    cleaned_source = source.replace("\\", "/").strip()
    if not cleaned_source or cleaned_source.lower().startswith(("http://", "https://", "memory://")):
        return None

    relative_parts = [
        part
        for part in Path(cleaned_source).parts
        if part not in {"..", ".", "/", "\\"}
    ]
    if not relative_parts:
        return None

    relative_path = Path(*relative_parts)
    hinted_path = root_path / relative_path
    if hinted_path.exists() and hinted_path.is_dir():
        return hinted_path
    if hinted_path.parent.exists() and hinted_path.parent.is_dir():
        return hinted_path.parent
    return None


def _find_matching_child_directory(parent: Path, target_name: str) -> Path | None:
    normalized_target = normalize_text(target_name)
    if not normalized_target or not parent.exists() or not parent.is_dir():
        return None

    direct = parent / target_name
    if direct.exists() and direct.is_dir():
        return direct

    for child in parent.iterdir():
        if not child.is_dir():
            continue
        child_name = normalize_text(child.name)
        if child_name == normalized_target or normalized_target in child_name:
            return child

    return None


def _score_media_candidate(
    candidate: Path,
    *,
    title: str,
    artist: str,
    album: str,
) -> int:
    title_key = normalize_text(title)
    artist_key = normalize_text(artist)
    album_key = normalize_text(album)
    stem_key = normalize_text(candidate.stem)
    path_key = normalize_text(" ".join(candidate.parts[-4:]))

    score = 0
    if title_key and title_key in stem_key:
        score += 120
    elif title_key and all(part in stem_key for part in title_key.split()):
        score += 80

    if artist_key and artist_key in path_key:
        score += 25
    if album_key and album_key in path_key:
        score += 15
    if title_key and stem_key == title_key:
        score += 15

    return score


def _path_exists_when_resolved(
    original_value: Any,
    normalized_path: str,
    *,
    playlist_dir: Path,
    media_roots: list[str],
    prefer_playlist_relative: bool,
) -> bool | None:
    original_text = _text_value(original_value).replace("\\", "/").strip()
    candidates: list[Path] = []
    seen: set[str] = set()

    def add_candidate(path_value: str | Path) -> None:
        candidate = Path(path_value)
        key = str(candidate).casefold()
        if key not in seen:
            seen.add(key)
            candidates.append(candidate)

    if original_text and _looks_like_absolute_path(original_text):
        add_candidate(original_text)

    if normalized_path:
        if _looks_like_absolute_path(normalized_path):
            add_candidate(normalized_path)
        elif normalized_path.startswith(("../", "./")):
            can_check_relative = prefer_playlist_relative or any(
                _path_is_within_root(playlist_dir, root)
                for root in media_roots
                if _looks_like_absolute_path(root)
            )
            if can_check_relative:
                add_candidate((playlist_dir / normalized_path).resolve())

    if original_text and not _looks_like_absolute_path(original_text):
        cleaned_original = original_text.lstrip("/")
        for root in media_roots:
            if not _looks_like_absolute_path(root):
                continue
            root_path = Path(root)
            if not root_path.exists() or not root_path.is_dir():
                continue
            add_candidate(root_path / cleaned_original)

    if not candidates:
        return None

    has_accessible_root = False
    for candidate in candidates:
        if candidate.exists():
            return True

        anchor = candidate.anchor or str(candidate.parent)
        if anchor and Path(anchor).exists():
            has_accessible_root = True

    return False if has_accessible_root else None


def _path_is_within_root(path_value: Path, root_text: str) -> bool:
    try:
        root_path = Path(root_text)
        path_value.resolve().relative_to(root_path.resolve())
    except (ValueError, OSError):
        return False
    return True


def _load_media_path_settings() -> tuple[list[str], str]:
    if has_app_context():
        roots = [
            str(current_app.config.get("NAVIDROME_MUSIC_ROOT", "")),
            str(current_app.config.get("DEEZER_DOWNLOAD_DIR", "/app/downloads")),
        ]
        prefix = str(current_app.config.get("NAVIDROME_M3U_PATH_PREFIX", ".."))
    else:
        roots = [
            os.getenv("NAVIDROME_MUSIC_ROOT", ""),
            os.getenv("DEEZER_DOWNLOAD_DIR", "/app/downloads"),
        ]
        prefix = os.getenv("NAVIDROME_M3U_PATH_PREFIX", "..")

    cleaned_roots: list[str] = []
    seen: set[str] = set()
    for root in roots:
        normalized = root.replace("\\", "/").strip().rstrip("/")
        key = normalized.casefold()
        if normalized and key not in seen:
            seen.add(key)
            cleaned_roots.append(normalized)

    return cleaned_roots, prefix.replace("\\", "/").strip() or ".."


def _rewrite_from_media_roots(
    path_text: str,
    media_roots: list[str],
    media_path_prefix: str,
) -> str:
    normalized_path = path_text.replace("\\", "/").strip()
    lowered_path = normalized_path.casefold()

    for root in media_roots:
        normalized_root = root.replace("\\", "/").strip().rstrip("/")
        lowered_root = normalized_root.casefold()
        if not normalized_root:
            continue
        if lowered_path == lowered_root or lowered_path.startswith(f"{lowered_root}/"):
            suffix = normalized_path[len(normalized_root) :].lstrip("/")
            return _join_media_prefix(media_path_prefix, suffix)

    return ""


def _join_media_prefix(prefix: str, relative_path: str) -> str:
    cleaned_relative = relative_path.replace("\\", "/").lstrip("/")
    cleaned_prefix = prefix.replace("\\", "/").strip().rstrip("/")
    if not cleaned_prefix or cleaned_prefix == ".":
        return cleaned_relative
    if not cleaned_relative:
        return cleaned_prefix
    return f"{cleaned_prefix}/{cleaned_relative}"


def _looks_like_absolute_path(value: str) -> bool:
    normalized = value.replace("\\", "/").strip()
    return normalized.startswith("/") or bool(re.match(r"^[a-zA-Z]:/", normalized))


def _build_playlist_stem(playlist_name: str) -> tuple[str, bool]:
    cleaned_name = _BRACKET_PREFIX_RE.sub("", playlist_name).strip() or "playlist"
    lowered = cleaned_name.lower()
    is_recurring = any(marker in lowered for marker in ("daily", "weekly", "day of", "week of"))

    if is_recurring:
        cleaned_name = _RECURRING_DATE_RE.sub("", cleaned_name).strip(" ,-_")
        cleaned_name = _RECURRING_FOR_RE.sub("", cleaned_name).strip(" ,-_")
        stem = _safe_recurring_filename(cleaned_name)
    else:
        stem = secure_filename(cleaned_name).replace("_", "-").strip(".-").lower() or "playlist"

    return stem, is_recurring


def _safe_recurring_filename(value: str) -> str:
    sanitized = re.sub(r'[<>:"/\\|?*]+', ' ', value)
    sanitized = re.sub(r'\s+', ' ', sanitized).strip(' .-_')
    if sanitized and sanitized == sanitized.lower():
        sanitized = sanitized.title()
    return sanitized or "Playlist"


def _stable_playlist_key(value: str) -> str:
    return re.sub(r'[\s_-]+', '', value).strip('.-').casefold()


def _remove_recurring_variants(
    folder: Path,
    stable_stem: str,
    target_path: Path,
) -> list[str]:
    removed: list[str] = []
    stable_key = _stable_playlist_key(stable_stem)

    for candidate in folder.glob("*.m3u"):
        if candidate == target_path:
            continue

        normalized_key = _stable_playlist_key(candidate.stem)
        if normalized_key == stable_key or normalized_key.startswith(stable_key):
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
