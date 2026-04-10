from __future__ import annotations

import logging
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app.matching import rank_candidates
from app.matching.normalize import build_search_queries, normalize_text
from app.models import PlaylistTrack
from app.services.song_metadata import write_song_metadata_xml

try:  # pragma: no cover - exercised indirectly in runtime environments
    from yt_dlp import YoutubeDL
except ImportError:  # pragma: no cover - handled by is_configured
    YoutubeDL = None  # type: ignore[assignment]

log = logging.getLogger(__name__)
_SAFE_NAME_RE = re.compile(r'[<>:"/\\|?*\x00-\x1f]')
_AUDIO_EXTENSIONS = {".aac", ".flac", ".m4a", ".mp3", ".ogg", ".opus", ".wav"}


class SoundCloudDownloadService:
    """Searches and downloads tracks from SoundCloud using yt-dlp."""

    def __init__(
        self,
        download_dir: str = "/navidrome/root",
        navidrome_music_root: str = "",
        match_threshold: float = 72.0,
        enabled: bool = True,
        extractor_factory: Any | None = None,
    ) -> None:
        self.download_dir = download_dir
        self.navidrome_music_root = navidrome_music_root or download_dir
        self.match_threshold = match_threshold
        self.enabled = enabled
        self._extractor_factory = extractor_factory or _default_extractor_factory

    @classmethod
    def from_config(cls, config: Any) -> SoundCloudDownloadService:
        music_root = str(config.get("NAVIDROME_MUSIC_ROOT", "/navidrome/root"))
        raw_enabled = str(config.get("SOUNDCLOUD_FALLBACK_ENABLED", "1")).strip().lower()
        enabled = raw_enabled not in {"0", "false", "no", "off"}
        threshold = float(
            config.get(
                "SOUNDCLOUD_MATCH_THRESHOLD",
                config.get("DEEZER_MATCH_THRESHOLD", 72.0),
            )
        )
        return cls(
            download_dir=music_root,
            navidrome_music_root=music_root,
            match_threshold=threshold,
            enabled=enabled,
        )

    def is_configured(self) -> bool:
        if not self.enabled or not self.download_dir or self._extractor_factory is None:
            return False
        if self._extractor_factory is _default_extractor_factory and YoutubeDL is None:
            return False
        return True

    def search_track(self, track: PlaylistTrack, limit: int = 10) -> list[dict[str, Any]]:
        if not self.is_configured():
            return []

        candidates: list[dict[str, Any]] = []
        seen: set[str] = set()
        for query in build_search_queries(track):
            payload = self._extract_info(f"scsearch{limit}:{query}", download=False)
            entries = payload.get("entries", []) if isinstance(payload, dict) else []
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                candidate = self._candidate_from_info(entry)
                dedupe_key = str(
                    candidate.get("soundcloud_id") or candidate.get("link") or candidate.get("id")
                ).strip()
                if not dedupe_key or dedupe_key in seen:
                    continue
                seen.add(dedupe_key)
                candidates.append(candidate)

        return rank_candidates(track, candidates, threshold=self.match_threshold)

    def resolve_track_selection(
        self,
        track: PlaylistTrack,
        match: dict[str, Any],
    ) -> dict[str, Any]:
        if not self.is_configured():
            raise ValueError(
                "SoundCloud fallback is unavailable. Install `yt-dlp` and keep "
                "`SOUNDCLOUD_FALLBACK_ENABLED=1` to use it."
            )

        result: dict[str, Any] = {
            "track": track.to_dict(),
            "queries": build_search_queries(track),
            "status": "not_found",
            "message": "No SoundCloud match found.",
            "match": {**match, "provider": "soundcloud", "provider_label": "SoundCloud"},
            "candidates": [{**match, "provider": "soundcloud", "provider_label": "SoundCloud"}],
        }

        in_library = self._find_in_library(match, track)
        if in_library is not None:
            result["status"] = "already_available"
            result["message"] = "Track is already in the Navidrome library."
            result["match"] = {**result["match"], "path": str(in_library)}
            return result

        existing = self._find_existing(match, track)
        if existing is not None:
            result["status"] = "already_available"
            result["message"] = "Track is already in the download directory."
            result["match"] = {**result["match"], "path": str(existing)}
            return result

        file_path, metadata_path = self._download_track(track, result["match"])
        result["status"] = "downloaded"
        result["message"] = "Track downloaded from SoundCloud."
        result["match"] = {**result["match"], "path": str(file_path)}
        result["download"] = {
            "provider": "soundcloud",
            "path": str(file_path),
            "metadata_path": str(metadata_path),
            "completed_at": _utc_timestamp(),
        }
        return result

    def _candidate_from_info(self, info: dict[str, Any]) -> dict[str, Any]:
        duration = info.get("duration")
        if isinstance(duration, (int, float)) and duration > 10_000:
            duration = int(duration / 1000)

        link = str(info.get("webpage_url") or info.get("original_url") or info.get("url") or "")
        soundcloud_id = str(info.get("id") or "").strip()
        return {
            "id": f"soundcloud:{soundcloud_id or link}",
            "provider": "soundcloud",
            "provider_label": "SoundCloud",
            "soundcloud_id": soundcloud_id,
            "title": str(info.get("track") or info.get("title") or "").strip(),
            "artist": str(
                info.get("artist") or info.get("creator") or info.get("uploader") or ""
            ).strip(),
            "album": str(info.get("album") or info.get("playlist_title") or "").strip(),
            "duration_seconds": int(duration) if isinstance(duration, (int, float)) else None,
            "link": link,
            "source_kind": "external",
        }

    def _extract_info(
        self, url: str, *, download: bool, outtmpl: str | None = None
    ) -> dict[str, Any]:
        if self._extractor_factory is None:
            raise RuntimeError("yt-dlp is not available in this environment.")

        options: dict[str, Any] = {
            "quiet": True,
            "no_warnings": True,
            "noplaylist": True,
            "skip_download": not download,
        }
        if download and outtmpl:
            options.update(
                {
                    "format": "bestaudio/best",
                    "outtmpl": {"default": outtmpl},
                    "overwrites": False,
                }
            )

        with self._extractor_factory(options) as ydl:
            info = ydl.extract_info(url, download=download)
            if isinstance(info, dict):
                return info
            raise ValueError("SoundCloud extractor returned an unexpected payload.")

    def _download_track(
        self,
        track: PlaylistTrack,
        match: dict[str, Any],
    ) -> tuple[Path, Path]:
        link = str(match.get("link") or "").strip()
        if not link:
            raise ValueError("The selected SoundCloud match does not include a playable URL.")

        stem_path = self._build_stem_path(match, track)
        stem_path.parent.mkdir(parents=True, exist_ok=True)
        info = self._extract_info(
            link,
            download=True,
            outtmpl=str(stem_path.parent / f"{stem_path.name}.%(ext)s"),
        )
        output_path = self._resolve_downloaded_path(info, stem_path)
        if not output_path.exists():
            raise ValueError("SoundCloud download finished, but no audio file was written.")

        metadata_path = write_song_metadata_xml(
            output_path,
            title=str(match.get("title") or track.title or output_path.stem),
            artist=str(match.get("artist") or track.artist or ""),
            album=str(match.get("album") or track.album or "SoundCloud"),
            duration_seconds=match.get("duration_seconds") or track.duration_seconds,
            provider="soundcloud",
            quality=str(info.get("audio_ext") or info.get("ext") or ""),
            source=link,
            annotation=str(info.get("description") or ""),
            timestamp=_utc_timestamp(),
        )
        return output_path, metadata_path

    def _resolve_downloaded_path(self, info: dict[str, Any], stem_path: Path) -> Path:
        requested = info.get("requested_downloads")
        if isinstance(requested, list):
            for item in requested:
                if isinstance(item, dict) and item.get("filepath"):
                    return Path(str(item["filepath"]))

        for key in ("filepath", "_filename"):
            value = info.get(key)
            if value:
                return Path(str(value))

        matches = sorted(stem_path.parent.glob(f"{stem_path.name}.*"))
        for candidate in matches:
            if candidate.suffix.lower() in _AUDIO_EXTENSIONS:
                return candidate
        raise ValueError("Could not determine the saved SoundCloud file path.")

    def _build_stem_path(self, match: dict[str, Any], track: PlaylistTrack) -> Path:
        artist = _safe_name(str(match.get("artist") or track.artist or "Unknown Artist"))
        album = _safe_name(str(match.get("album") or track.album or "SoundCloud"))
        title = _safe_name(str(match.get("title") or track.title or "Unknown Track"))
        return Path(self.download_dir) / artist / album / title

    def _find_in_library(self, match: dict[str, Any], track: PlaylistTrack) -> Path | None:
        if not self.navidrome_music_root:
            return None

        root = Path(self.navidrome_music_root)
        if not root.is_dir():
            return None

        artist = _safe_name(str(match.get("artist") or track.artist or ""))
        album = _safe_name(str(match.get("album") or track.album or ""))
        title_key = normalize_text(str(match.get("title") or track.title or ""))
        if not artist or not title_key:
            return None

        album_dir = root / artist / album
        if not album_dir.is_dir():
            return None

        try:
            for candidate in album_dir.iterdir():
                if not candidate.is_file() or candidate.suffix.lower() not in _AUDIO_EXTENSIONS:
                    continue
                stem_key = normalize_text(candidate.stem)
                if title_key in stem_key or stem_key == title_key:
                    return candidate
        except OSError:
            return None
        return None

    def _find_existing(self, match: dict[str, Any], track: PlaylistTrack) -> Path | None:
        stem_path = self._build_stem_path(match, track)
        for ext in _AUDIO_EXTENSIONS:
            candidate = stem_path.with_suffix(ext)
            if candidate.exists():
                return candidate
        return None


def _default_extractor_factory(options: dict[str, Any]) -> Any:
    if YoutubeDL is None:
        raise RuntimeError("yt-dlp is not installed. Run `uv sync --dev` to add it.")
    return YoutubeDL(options)


def _safe_name(value: str) -> str:
    cleaned = _SAFE_NAME_RE.sub("_", value).strip(". ")
    return cleaned[:100] or "Unknown"


def _utc_timestamp() -> str:
    return datetime.now(UTC).isoformat()
