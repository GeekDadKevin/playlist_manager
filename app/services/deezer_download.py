from __future__ import annotations

import hashlib
import logging
import re
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
from Crypto.Cipher import Blowfish

from app.matching import rank_candidates
from app.matching.normalize import build_search_queries, normalize_text
from app.models import PlaylistTrack
from app.services.cover_art import ensure_cover_art
from app.services.listenbrainz import ListenBrainzService
from app.services.musicbrainz import MusicBrainzService
from app.services.path_template import build_download_path
from app.services.song_metadata import (
    extract_musicbrainz_track_id,
    write_flac_tags,
    write_song_metadata_xml,
)
from app.services.soundcloud_download import SoundCloudDownloadService
from app.services.youtube_download import YouTubeDownloadService

log = logging.getLogger(__name__)

_DEEZER_GW = "https://www.deezer.com/ajax/gw-light.php"
_DEEZER_MEDIA_URL = "https://media.deezer.com/v1/get_url"
_DEEZER_SEARCH_URL = "https://api.deezer.com/search"
_BF_SECRET = "g4el58wc0zvf9na1"
_CHUNK_SIZE = 2048
_SAFE_NAME_RE = re.compile(r'[<>:"/\\|?*\x00-\x1f]')


class DeezerDownloadService:
    """Downloads tracks from Deezer and can offer fallback providers during review."""

    def __init__(
        self,
        arl: str = "",
        download_dir: str = "/navidrome/root",
        navidrome_music_root: str = "",
        download_path_template: str = "{artist}/{album}/{artist} - {track} - {title}",
        quality: str = "FLAC",
        match_threshold: float = 72.0,
        transport: httpx.BaseTransport | None = None,
        soundcloud_service: SoundCloudDownloadService | None = None,
        youtube_service: YouTubeDownloadService | None = None,
        musicbrainz_service: MusicBrainzService | None = None,
        listenbrainz_service: ListenBrainzService | None = None,
        download_threads: int = 1,
    ) -> None:
        self.arl = arl.strip()
        self.download_dir = download_dir
        self.navidrome_music_root = navidrome_music_root
        self.download_path_template = download_path_template
        self.quality = quality.upper()
        self.match_threshold = match_threshold
        self.transport = transport
        self.soundcloud_service = soundcloud_service
        self.youtube_service = youtube_service
        self.musicbrainz_service = musicbrainz_service
        self.listenbrainz_service = listenbrainz_service
        self.download_threads = max(int(download_threads), 1)
        self._api_token = ""
        self._license_token = ""
        self._gw_cookies: dict[str, str] = {}

    @classmethod
    def from_config(cls, config: Any) -> DeezerDownloadService:
        music_root = str(config.get("NAVIDROME_MUSIC_ROOT", "/navidrome/root"))
        soundcloud_service = SoundCloudDownloadService.from_config(config)
        youtube_service = YouTubeDownloadService.from_config(config)
        musicbrainz_service = MusicBrainzService.from_config(config)
        listenbrainz_service = ListenBrainzService.from_config(config)
        return cls(
            arl=str(config.get("DEEZER_ARL", "")),
            download_dir=music_root,
            navidrome_music_root=music_root,
            download_path_template=str(
                config.get(
                    "DOWNLOAD_PATH_TEMPLATE",
                    "{artist}/{album}/{artist} - {track} - {title}",
                )
            ),
            quality=str(config.get("DEEZER_QUALITY", "FLAC")),
            match_threshold=float(config.get("DEEZER_MATCH_THRESHOLD", 72.0)),
            soundcloud_service=soundcloud_service,
            youtube_service=youtube_service,
            musicbrainz_service=musicbrainz_service,
            listenbrainz_service=listenbrainz_service,
            download_threads=int(config.get("DOWNLOAD_THREADS", 1) or 1),
        )

    def is_configured(self) -> bool:
        return bool(self.arl and self.download_dir)

    def search_track(
        self,
        track: PlaylistTrack,
        limit: int = 10,
        *,
        include_soundcloud: bool = False,
        include_youtube: bool = False,
    ) -> list[dict[str, Any]]:
        return self._search_track(
            track,
            limit=limit,
            include_soundcloud=include_soundcloud,
            include_youtube=include_youtube,
        )

    def resolve_track_selection(
        self,
        track: PlaylistTrack,
        match: dict[str, Any],
        *,
        download_progress: Callable[[PlaylistTrack, dict[str, Any], int, int], None] | None = None,
    ) -> dict[str, Any]:
        provider = str(match.get("provider", "deezer")).strip().lower()
        if provider == "soundcloud":
            if not self.soundcloud_service or not self.soundcloud_service.is_configured():
                raise ValueError("SoundCloud fallback is not configured for this sync job.")
            return self.soundcloud_service.resolve_track_selection(track, match)
        if provider == "youtube":
            if not self.youtube_service or not self.youtube_service.is_configured():
                raise ValueError("YouTube fallback is not configured for this sync job.")
            return self.youtube_service.resolve_track_selection(track, match)

        self._validate_configuration()
        if not self.arl:
            raise ValueError("Set `DEEZER_ARL` to download tracks from Deezer.")
        self._ensure_authenticated()

        result = self._build_result(track)
        result["match"] = {
            **match,
            "accepted": True,
            "provider": "deezer",
            "provider_label": "Deezer",
        }
        result["candidates"] = [result["match"]]
        return self._finalize_selected_match(
            track,
            result,
            result["match"],
            download_progress=download_progress,
        )

    def sync_tracks(
        self,
        tracks: list[PlaylistTrack],
        max_tracks: int | None = None,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        self._validate_configuration()

        started_at = _utc_timestamp()
        selected = tracks[:max_tracks] if max_tracks else tracks
        provider_name = self._provider_name()
        download_progress: dict[str, Any] = {
            "status": "idle",
            "current_track": "",
            "percent": 0,
            "bytes_downloaded": 0,
            "bytes_total": 0,
            "updated_at": "",
            "items": [],
        }
        progress_items: dict[str, dict[str, Any]] = {}
        from threading import Lock

        progress_lock = Lock()
        completed_ttl_seconds = 5
        log.info(
            "Starting sync: %d track(s), providers=%s, quality=%s, threshold=%.0f",
            len(selected),
            provider_name,
            self.quality,
            self.match_threshold,
        )
        results: list[dict[str, Any]] = []
        summary = {
            "requested": len(selected),
            "processed": 0,
            "preview": 0,
            "downloaded": 0,
            "already_available": 0,
            "low_confidence": 0,
            "not_found": 0,
            "failed": 0,
        }

        def report_download_progress(
            track: PlaylistTrack,
            match: dict[str, Any],
            bytes_downloaded: int,
            bytes_total: int,
        ) -> None:
            percent = 0
            if bytes_total > 0:
                percent = min(100, int((bytes_downloaded / bytes_total) * 100))
            label = self._format_track_label(track, match)
            item_key = str(
                match.get("deezer_id")
                or match.get("id")
                or match.get("link")
                or f"{track.artist}-{track.title}"
            )
            with progress_lock:
                now_ts = time.time()
                existing = progress_items.get(item_key, {})
                if percent >= 100:
                    progress_items[item_key] = {
                        "track": label,
                        "percent": 100,
                        "bytes_downloaded": bytes_downloaded,
                        "bytes_total": bytes_total,
                        "updated_at": _utc_timestamp(),
                        "completed_at_ts": existing.get("completed_at_ts", now_ts),
                    }
                else:
                    progress_items[item_key] = {
                        "track": label,
                        "percent": percent,
                        "bytes_downloaded": bytes_downloaded,
                        "bytes_total": bytes_total,
                        "updated_at": _utc_timestamp(),
                        "completed_at_ts": 0,
                    }
                expired_keys = [
                    key
                    for key, item in progress_items.items()
                    if item.get("completed_at_ts", 0)
                    and now_ts - float(item.get("completed_at_ts", 0)) >= completed_ttl_seconds
                ]
                for key in expired_keys:
                    progress_items.pop(key, None)
                download_progress.update(
                    {
                        "status": "downloading",
                        "current_track": label,
                        "percent": percent,
                        "bytes_downloaded": bytes_downloaded,
                        "bytes_total": bytes_total,
                        "updated_at": _utc_timestamp(),
                        "items": list(progress_items.values()),
                    }
                )

            if progress_callback is not None:
                processing_mode = "parallel" if download_threads > 1 else "sequential"
                snapshot_results = [item for item in results if item]
                progress_callback(
                    {
                        "mode": "download",
                        "provider": provider_name,
                        "threshold": self.match_threshold,
                        "processing_mode": processing_mode,
                        "started_at": started_at,
                        "completed_at": "",
                        "summary": dict(summary),
                        "results": snapshot_results,
                        "download_progress": dict(download_progress),
                    }
                )

        download_threads = max(int(self.download_threads), 1)
        if download_threads == 1:
            for index, track in enumerate(selected, start=1):
                result = self._sync_single_track(
                    track,
                    download_progress=report_download_progress,
                )
                result["index"] = index
                result["completed_at"] = _utc_timestamp()
                results.append(result)
                summary["processed"] = index
                status = result["status"]
                if status in summary:
                    summary[status] += 1

                if progress_callback is not None:
                    progress_callback(
                        {
                            "mode": "download",
                            "provider": provider_name,
                            "threshold": self.match_threshold,
                            "processing_mode": "sequential",
                            "started_at": started_at,
                            "completed_at": "",
                            "summary": dict(summary),
                            "results": list(results),
                            "download_progress": dict(download_progress),
                        }
                    )
        else:
            from concurrent.futures import ThreadPoolExecutor, as_completed

            results = [None] * len(selected)
            completed = 0

            def run_item(index: int, track: PlaylistTrack) -> dict[str, Any]:
                worker = self._clone_for_worker()
                result = worker._sync_single_track(
                    track,
                    download_progress=report_download_progress,
                )
                result["index"] = index
                result["completed_at"] = _utc_timestamp()
                return result

            with ThreadPoolExecutor(
                max_workers=download_threads,
                thread_name_prefix="deezer-download",
            ) as pool:
                future_map = {
                    pool.submit(run_item, index, track): index
                    for index, track in enumerate(selected, start=1)
                }
                for future in as_completed(future_map):
                    result = future.result()
                    index = int(result.get("index", future_map[future]))
                    results[index - 1] = result
                    completed += 1
                    summary["processed"] = completed
                    status = result.get("status")
                    if status in summary:
                        summary[status] += 1

                    if progress_callback is not None:
                        snapshot_results = [item for item in results if item]
                        progress_callback(
                            {
                                "mode": "download",
                                "provider": provider_name,
                                "threshold": self.match_threshold,
                                "processing_mode": "parallel",
                                "started_at": started_at,
                                "completed_at": "",
                                "summary": dict(summary),
                                "results": snapshot_results,
                                "download_progress": dict(download_progress),
                            }
                        )

        final_results = [item for item in results if item]
        final: dict[str, Any] = {
            "mode": "download",
            "provider": provider_name,
            "threshold": self.match_threshold,
            "processing_mode": "sequential" if download_threads == 1 else "parallel",
            "started_at": started_at,
            "completed_at": _utc_timestamp(),
            "summary": summary,
            "results": final_results,
            "download_progress": dict(download_progress),
        }
        if progress_callback is not None:
            progress_callback(final)
        return final

    # ------------------------------------------------------------------
    # Per-track logic
    # ------------------------------------------------------------------

    def _sync_single_track(
        self,
        track: PlaylistTrack,
        *,
        download_progress: Callable[[PlaylistTrack, dict[str, Any], int, int], None] | None = None,
    ) -> dict[str, Any]:
        log.info("Processing: %r by %r", track.title, track.artist)
        result = self._build_result(track)

        try:
            ranked = self._search_track(track, include_soundcloud=False)
        except Exception as exc:
            log.error("Search failed for %r: %s", track.title, exc)
            result["status"] = "failed"
            result["message"] = f"Search error: {exc}"
            return result

        if not ranked:
            log.warning("No Deezer results for %r by %r", track.title, track.artist)
            return result

        top = next(
            (
                candidate
                for candidate in ranked
                if candidate.get("provider") == "deezer" and candidate.get("accepted")
            ),
            ranked[0],
        )
        result["match"] = top
        result["candidates"] = ranked[:8]
        log.info(
            "Best match: %r by %r (provider=%s, score=%.1f, accepted=%s)",
            top.get("title"),
            top.get("artist"),
            top.get("provider", "deezer"),
            top.get("score", 0),
            top.get("accepted"),
        )

        if not top.get("accepted", False):
            log.warning(
                "Low confidence for %r — score %.1f < threshold %.0f",
                track.title,
                top.get("score", 0),
                self.match_threshold,
            )
            result["status"] = "low_confidence"
            result["message"] = "Best Deezer match was below the configured confidence threshold."
            return result

        try:
            resolved = self.resolve_track_selection(
                track,
                top,
                download_progress=download_progress,
            )
        except Exception as exc:
            log.error(
                "Selection failed for %r using %s: %s",
                track.title,
                top.get("provider", "deezer"),
                exc,
            )
            result["status"] = "failed"
            result["message"] = str(exc)
            return result

        resolved["candidates"] = ranked[:8]
        return resolved

    def _build_result(self, track: PlaylistTrack) -> dict[str, Any]:
        return {
            "track": track.to_dict(),
            "queries": build_search_queries(track),
            "status": "not_found",
            "message": "No Deezer match found.",
            "match": {},
            "candidates": [],
        }

    def _finalize_selected_match(
        self,
        track: PlaylistTrack,
        result: dict[str, Any],
        match: dict[str, Any],
        *,
        download_progress: Callable[[PlaylistTrack, dict[str, Any], int, int], None] | None = None,
    ) -> dict[str, Any]:
        resolved_recording_mbid = self._ensure_musicbrainz_recording_id(track, match)
        updated_track_number = self._ensure_track_number(track, match)
        if resolved_recording_mbid or updated_track_number:
            result["track"] = track.to_dict()
        deezer_id = match.get("deezer_id")
        if not deezer_id:
            log.error("Match for %r has no deezer_id — cannot download", track.title)
            return result

        in_library = self._find_in_library(match, track)
        if in_library is not None:
            log.info("Already in library: %s", in_library)
            result["status"] = "already_available"
            result["message"] = "Track is already in the Navidrome library."
            result["match"] = {**match, "path": str(in_library)}
            return result

        existing = self._find_existing(match, track)
        if existing is not None:
            log.info("Already downloaded: %s", existing)
            result["status"] = "already_available"
            result["message"] = "Track is already in the download directory."
            result["match"] = {**match, "path": str(existing)}
            return result

        try:
            file_path, metadata_path = self._download_track(
                deezer_id,
                match,
                track,
                download_progress=download_progress,
            )
        except Exception as exc:
            log.error("Download failed for %r (id=%s): %s", track.title, deezer_id, exc)
            result["status"] = "failed"
            result["message"] = str(exc)
            return result

        log.info("Downloaded: %s", file_path)
        result["status"] = "downloaded"
        result["message"] = "Track downloaded from Deezer."
        result["match"] = {**match, "path": str(file_path)}
        result["download"] = {
            "provider": "deezer",
            "deezer_id": deezer_id,
            "path": str(file_path),
            "extension": file_path.suffix,
            "metadata_path": str(metadata_path),
            "completed_at": _utc_timestamp(),
        }
        return result

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    def _search_track(
        self,
        track: PlaylistTrack,
        limit: int = 10,
        *,
        include_soundcloud: bool = False,
        include_youtube: bool = False,
    ) -> list[dict[str, Any]]:
        deezer_ranked = self._search_deezer_candidates(track, limit=limit)
        if not include_soundcloud and not include_youtube:
            return deezer_ranked

        soundcloud_ranked: list[dict[str, Any]] = []
        if self.soundcloud_service and self.soundcloud_service.is_configured():
            try:
                soundcloud_ranked = self.soundcloud_service.search_track(track, limit=limit)
            except Exception as exc:
                log.warning("SoundCloud search failed for %r: %s", track.title, exc)
        merged = self._merge_candidates(deezer_ranked, soundcloud_ranked)

        youtube_ranked: list[dict[str, Any]] = []
        if include_youtube and self.youtube_service and self.youtube_service.is_configured():
            try:
                youtube_ranked = self.youtube_service.search_track(track, limit=limit)
            except Exception as exc:
                log.warning("YouTube search failed for %r: %s", track.title, exc)

        if youtube_ranked:
            merged = self._merge_candidates(merged, youtube_ranked)
        return merged

    def _clone_for_worker(self) -> DeezerDownloadService:
        return DeezerDownloadService(
            arl=self.arl,
            download_dir=self.download_dir,
            navidrome_music_root=self.navidrome_music_root,
            download_path_template=self.download_path_template,
            quality=self.quality,
            match_threshold=self.match_threshold,
            transport=self.transport,
            soundcloud_service=self.soundcloud_service,
            youtube_service=self.youtube_service,
            musicbrainz_service=self.musicbrainz_service,
            listenbrainz_service=self.listenbrainz_service,
            download_threads=1,
        )

    def _search_deezer_candidates(
        self,
        track: PlaylistTrack,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        """Try each Deezer query variant and return the best-ranked results found."""
        if not self.arl:
            return []

        best: list[dict[str, Any]] = []
        queries = build_search_queries(track)
        log.debug("Search queries for %r: %s", track.title, queries)
        for query in queries:
            candidates = self._deezer_search(query, limit)
            log.debug("Query %r → %d Deezer candidate(s)", query, len(candidates))
            if not candidates:
                continue
            ranked = rank_candidates(track, candidates, threshold=self.match_threshold)
            if ranked and (not best or ranked[0]["score"] > best[0]["score"]):
                best = ranked
            if ranked and ranked[0]["accepted"]:
                log.debug("Accepted Deezer match found on query %r", query)
                break
        return best

    def _deezer_search(self, query: str, limit: int) -> list[dict[str, Any]]:
        with self._client(timeout=10.0) as client:
            response = client.get(
                _DEEZER_SEARCH_URL,
                params={"q": query, "limit": limit},
            )
            response.raise_for_status()
            payload = response.json()

        return [
            {
                "id": str(item.get("id", "")),
                "provider": "deezer",
                "provider_label": "Deezer",
                "deezer_id": item.get("id"),
                "link": item.get("link", ""),
                "title": item.get("title", ""),
                "artist": item.get("artist", {}).get("name", ""),
                "artist_id": item.get("artist", {}).get("id"),
                "album": item.get("album", {}).get("title", ""),
                "album_id": item.get("album", {}).get("id"),
                "album_cover": (
                    item.get("album", {}).get("cover_xl")
                    or item.get("album", {}).get("cover_big")
                    or item.get("album", {}).get("cover_medium")
                    or item.get("album", {}).get("cover")
                    or ""
                ),
                "track_number": item.get("track_position") or item.get("track_number") or 0,
                "duration_seconds": item.get("duration"),
                "source_kind": "external",
            }
            for item in payload.get("data", [])
        ]

    # ------------------------------------------------------------------
    # Authentication
    # ------------------------------------------------------------------

    def _ensure_authenticated(self) -> None:
        if self._api_token and self._license_token:
            return
        log.info("Authenticating with Deezer (ARL: ...%s)", self.arl[-8:] if self.arl else "?")
        with self._client(timeout=15.0) as client:
            client.cookies.update({"arl": self.arl})
            response = client.post(
                _DEEZER_GW,
                params={
                    "method": "deezer.getUserData",
                    "api_version": "1.0",
                    "api_token": "null",
                    "input": "3",
                },
            )
            response.raise_for_status()
            # Capture ALL cookies Deezer set — subsequent GW calls need the full session
            self._gw_cookies = {"arl": self.arl, **dict(response.cookies)}
            log.debug("Session cookies captured: %s", list(self._gw_cookies.keys()))
            payload = response.json()
            _check_gw_error(payload)
            data = payload.get("results", {})

        self._api_token = str(data.get("checkForm", ""))
        self._license_token = str(data.get("USER", {}).get("OPTIONS", {}).get("license_token", ""))
        if not self._api_token or not self._license_token:
            raise RuntimeError(
                "Deezer authentication failed — checkForm or license_token missing. "
                "Verify your DEEZER_ARL token."
            )
        log.info("Deezer authentication OK — api_token: ...%s", self._api_token[-8:])

    # ------------------------------------------------------------------
    # Download
    # ------------------------------------------------------------------

    def _get_track_token(self, deezer_id: int | str) -> str:
        log.debug("Fetching track token for Deezer ID %s", deezer_id)
        with self._client(timeout=15.0) as client:
            client.cookies.update(self._gw_cookies)
            response = client.post(
                _DEEZER_GW,
                params={
                    "method": "song.getData",
                    "api_version": "1.0",
                    "api_token": self._api_token,
                    "input": "3",
                },
                json={"sng_id": str(deezer_id)},
            )
            response.raise_for_status()
            payload = response.json()
            _check_gw_error(payload)
            token = str(payload.get("results", {}).get("TRACK_TOKEN", ""))
            if token:
                log.debug("Got track token for ID %s: ...%s", deezer_id, token[-12:])
            else:
                log.warning(
                    "song.getData returned no TRACK_TOKEN for ID %s — full results keys: %s",
                    deezer_id,
                    list(payload.get("results", {}).keys()),
                )
            return token

    def _get_download_url(self, track_token: str) -> tuple[str, str]:
        """Return (url, format_name). Format name determines the file extension."""
        formats = self._quality_formats()
        log.debug("Requesting download URL — format preference: %s", formats)
        with self._client(timeout=15.0) as client:
            response = client.post(
                _DEEZER_MEDIA_URL,
                json={
                    "license_token": self._license_token,
                    "media": [
                        {
                            "type": "FULL",
                            "formats": [
                                {"cipher": "BF_CBC_STRIPE", "format": fmt} for fmt in formats
                            ],
                        }
                    ],
                    "track_tokens": [track_token],
                },
            )
            response.raise_for_status()
            payload = response.json()

        log.debug("Media API response: %s", payload)
        media_list = payload.get("data", [{}])[0].get("media", [])
        if not media_list:
            raise RuntimeError("Deezer returned no download media for this track.")
        media = media_list[0]
        sources = media.get("sources", [])
        if not sources:
            raise RuntimeError("Deezer returned no download sources for this track.")
        fmt = media.get("format", self.quality)
        log.info("Download URL obtained — format=%s", fmt)
        return sources[0]["url"], fmt

    def _download_track(
        self,
        deezer_id: int | str,
        match: dict[str, Any],
        track: PlaylistTrack,
        *,
        download_progress: Callable[[PlaylistTrack, dict[str, Any], int, int], None] | None = None,
    ) -> tuple[Path, Path]:
        track_token = self._get_track_token(deezer_id)
        if not track_token:
            raise RuntimeError(f"No track token returned for Deezer ID {deezer_id}.")

        url, fmt = self._get_download_url(track_token)
        ext = ".flac" if "FLAC" in fmt else ".mp3"
        output_path = self._build_path(match, track, ext)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        log.info("Downloading to: %s", output_path)
        bf_key = _blowfish_key(str(int(deezer_id)))
        with self._client(timeout=None) as client:
            with client.stream("GET", url) as response:
                response.raise_for_status()
                bytes_total = int(response.headers.get("Content-Length") or 0)
                bytes_downloaded = 0

                def handle_progress(chunk_size: int) -> None:
                    nonlocal bytes_downloaded
                    bytes_downloaded += chunk_size
                    if download_progress is not None:
                        download_progress(track, match, bytes_downloaded, bytes_total)

                with open(output_path, "wb") as fh:
                    _decrypt_stream(
                        response.iter_bytes(_CHUNK_SIZE),
                        bf_key,
                        fh,
                        progress_callback=handle_progress,
                    )

        metadata_path = self._write_metadata_xml(
            output_path,
            match,
            track,
            deezer_id=deezer_id,
            fmt=fmt,
        )

        # Enforce strict tag completeness and safety after download
        tag_ok, tag_errors = self._validate_tag_completeness(output_path, match, track)
        if not tag_ok:
            log.error("Tag completeness validation failed for %s: %s", output_path, tag_errors)
            # Optionally, remove or quarantine the file here
            raise RuntimeError(f"Downloaded file failed tag completeness: {tag_errors}")
    def _validate_tag_completeness(self, audio_path: Path, match: dict[str, Any], track: PlaylistTrack) -> tuple[bool, list[str]]:
        """Check that all required tags (MusicBrainz, Deezer, core fields) are present and file is safe."""
        from app.services.song_metadata import load_embedded_audio_metadata
        tags = load_embedded_audio_metadata(audio_path)
        errors = []
        required = ["title", "artist", "album", "track_number", "musicbrainz_track_id", "deezer_id"]
        for key in required:
            value = str(tags.get(key, "")).strip()
            if not value or value.lower() in {"", "unknown", "0"}:
                errors.append(f"Missing or invalid tag: {key}")
        # Optionally, check for corruption or unsafe file here
        if audio_path.stat().st_size < 1024 * 100:
            errors.append("File size too small, possible corruption")
        return (len(errors) == 0, errors)

        try:
            ensure_cover_art(
                output_path.parent,
                cover_url=str(match.get("album_cover") or ""),
                fallback_title=str(match.get("title") or track.title or ""),
                fallback_artist=str(match.get("artist") or track.artist or ""),
                fallback_album=str(match.get("album") or track.album or ""),
            )
        except Exception as exc:
            log.warning("Cover art update failed for %s: %s", output_path.parent, exc)

        size_kb = output_path.stat().st_size // 1024
        log.info("Download complete: %s (%d KB)", output_path, size_kb)
        log.info("Metadata sidecar written: %s", metadata_path)
        return output_path, metadata_path

    # ------------------------------------------------------------------
    # Path helpers
    # ------------------------------------------------------------------

    def _build_path(
        self,
        match: dict[str, Any],
        track: PlaylistTrack,
        ext: str,
    ) -> Path:
        artist = _safe_name(match.get("artist", "") or track.artist or "Unknown Artist")
        album = _safe_name(match.get("album", "") or track.album or "Unknown Album")
        title = _safe_name(match.get("title", "") or track.title or "Unknown Track")
        track_number = _coerce_track_number(match.get("track_number") or track.track_number)
        return build_download_path(
            self.download_dir,
            self.download_path_template,
            artist=artist,
            album=album,
            title=title,
            track_number=track_number,
            ext=ext,
        )

    def _write_metadata_xml(
        self,
        audio_path: Path,
        match: dict[str, Any],
        track: PlaylistTrack,
        *,
        deezer_id: int | str,
        fmt: str,
    ) -> Path:
        annotation = str(track.extra.get("annotation", "")) if isinstance(track.extra, dict) else ""
        recording_mbid, _release_mbid = self._musicbrainz_ids_from_track(track)
        return write_song_metadata_xml(
            audio_path,
            title=str(match.get("title", "") or track.title or audio_path.stem),
            artist=str(match.get("artist", "") or track.artist or ""),
            album=str(match.get("album", "") or track.album or ""),
            track_number=_coerce_track_number(match.get("track_number") or track.track_number),
            duration_seconds=match.get("duration_seconds") or track.duration_seconds,
            provider="deezer",
            deezer_id=deezer_id,
            deezer_artist_id=match.get("artist_id"),
            deezer_album_id=match.get("album_id"),
            deezer_link=str(match.get("link", "")),
            quality=str(fmt or self.quality),
            source=str(track.source or ""),
            downloaded_from="deezer",
            musicbrainz_track_id=recording_mbid,
            annotation=annotation,
            timestamp=_utc_timestamp(),
        )

    def _write_flac_tags(
        self,
        audio_path: Path,
        match: dict[str, Any],
        track: PlaylistTrack,
        *,
        deezer_id: int | str,
        fmt: str,
    ) -> Path:
        annotation = str(track.extra.get("annotation", "")) if isinstance(track.extra, dict) else ""
        recording_mbid, _release_mbid = self._musicbrainz_ids_from_track(track)
        return write_flac_tags(
            audio_path,
            title=str(match.get("title", "") or track.title or audio_path.stem),
            artist=str(match.get("artist", "") or track.artist or ""),
            album=str(match.get("album", "") or track.album or ""),
            track_number=_coerce_track_number(match.get("track_number") or track.track_number),
            duration_seconds=match.get("duration_seconds") or track.duration_seconds,
            provider="deezer",
            deezer_id=deezer_id,
            deezer_artist_id=match.get("artist_id"),
            deezer_album_id=match.get("album_id"),
            deezer_link=str(match.get("link", "")),
            source=str(track.source or ""),
            musicbrainz_track_id=recording_mbid,
            annotation=annotation,
            quality=str(fmt or self.quality),
        )

    def _find_in_library(
        self,
        match: dict[str, Any],
        track: PlaylistTrack,
    ) -> Path | None:
        """Check NAVIDROME_MUSIC_ROOT/{artist}/{album}/ for any file whose stem
        contains the track title (case-insensitive, normalised).  This handles
        files named 'Title.flac', '01 - Title.flac', 'Artist Album Title.flac', etc.
        """
        if not self.navidrome_music_root:
            log.debug("Library check skipped — NAVIDROME_MUSIC_ROOT not set")
            return None

        root = Path(self.navidrome_music_root)
        if not root.is_dir():
            log.warning("Library check skipped — NAVIDROME_MUSIC_ROOT does not exist: %s", root)
            return None

        artist = _safe_name(match.get("artist", "") or track.artist or "")
        album = _safe_name(match.get("album", "") or track.album or "")
        title_key = normalize_text(match.get("title", "") or track.title or "")

        if not artist or not title_key:
            log.debug("Library check skipped — could not determine artist/title")
            return None

        album_dir = root / artist / album
        log.debug("Library check: scanning %s for title %r", album_dir, title_key)

        if not album_dir.is_dir():
            log.debug("Library check: album dir not found — %s", album_dir)
            return None

        _AUDIO = {".flac", ".mp3", ".m4a", ".aac", ".ogg", ".opus", ".wav"}
        try:
            for candidate in album_dir.iterdir():
                if not candidate.is_file() or candidate.suffix.lower() not in _AUDIO:
                    continue
                stem_key = normalize_text(candidate.stem)
                if title_key in stem_key or stem_key == title_key:
                    log.info("Found in library: %s", candidate)
                    return candidate
        except OSError as exc:
            log.warning("Library check error scanning %s: %s", album_dir, exc)
            return None

        log.debug("Library check: no match for %r in %s", title_key, album_dir)
        return None

    def _find_existing(
        self,
        match: dict[str, Any],
        track: PlaylistTrack,
    ) -> Path | None:
        """Return the path of a previously-downloaded file, or None."""
        for ext in (".flac", ".mp3"):
            candidate = self._build_path(match, track, ext)
            log.debug("Download dir check: trying %s", candidate)
            if candidate.exists():
                return candidate
        return None

    # ------------------------------------------------------------------
    # Misc helpers
    # ------------------------------------------------------------------

    def _provider_name(self) -> str:
        return "deezer"

    def _ensure_track_number(self, track: PlaylistTrack, match: dict[str, Any]) -> int | None:
        track_number = _coerce_track_number(track.track_number)
        if track_number:
            return track_number

        service = self.musicbrainz_service
        if service is None:
            return None

        recording_mbid, release_mbid = self._musicbrainz_ids_from_track(track)
        album_name = str(match.get("album") or track.album or "").strip()
        artist_name = str(match.get("artist") or track.artist or "").strip()

        resolved = service.lookup_track_number(
            recording_mbid=recording_mbid,
            release_mbid=release_mbid,
            album_name=album_name,
            artist_name=artist_name,
        )
        if resolved:
            track.track_number = resolved
        return resolved

    def _ensure_musicbrainz_recording_id(
        self,
        track: PlaylistTrack,
        match: dict[str, Any],
    ) -> str:
        recording_mbid, release_mbid = self._musicbrainz_ids_from_track(track)
        if recording_mbid:
            return recording_mbid

        title = str(match.get("title") or track.title or "").strip()
        artist_name = str(match.get("artist") or track.artist or "").strip()
        album_name = str(match.get("album") or track.album or "").strip()
        label = self._format_track_label(track, match)

        extra = track.extra if isinstance(track.extra, dict) else {}
        if not isinstance(track.extra, dict):
            track.extra = extra

        listenbrainz_service = self.listenbrainz_service
        if listenbrainz_service is not None:
            try:
                listenbrainz_metadata = listenbrainz_service.lookup_recording_metadata(
                    artist_name=artist_name,
                    recording_name=title,
                    release_name=album_name,
                )
            except Exception as exc:
                log.warning("ListenBrainz recording lookup failed for %s: %s", label, exc)
            else:
                recording_mbid = str(listenbrainz_metadata.get("recording_mbid") or "").strip()
                if recording_mbid:
                    extra["musicbrainz_recording_id"] = recording_mbid
                    if not release_mbid:
                        release_mbid = str(listenbrainz_metadata.get("release_mbid") or "").strip()
                        if release_mbid:
                            extra["musicbrainz_release_id"] = release_mbid
                    log.info(
                        "Resolved MusicBrainz recording ID via ListenBrainz for %s: %s",
                        label,
                        recording_mbid,
                    )
                    return recording_mbid

        musicbrainz_service = self.musicbrainz_service
        if musicbrainz_service is not None:
            try:
                recording_mbid = musicbrainz_service.lookup_recording_mbid(
                    title=title,
                    artist_name=artist_name,
                    album_name=album_name,
                )
            except Exception as exc:
                log.warning("MusicBrainz recording lookup failed for %s: %s", label, exc)
            else:
                if recording_mbid:
                    extra["musicbrainz_recording_id"] = recording_mbid
                    log.info(
                        "Resolved MusicBrainz recording ID via MusicBrainz for %s: %s",
                        label,
                        recording_mbid,
                    )
                    return recording_mbid

        log.warning(
            "No MusicBrainz recording ID found for %s after Deezer, "
            "ListenBrainz, and MusicBrainz lookup.",
            label,
        )
        return ""

    def _musicbrainz_ids_from_track(self, track: PlaylistTrack) -> tuple[str, str]:
        extra = track.extra if isinstance(track.extra, dict) else {}
        recording_mbid = str(extra.get("musicbrainz_recording_id") or "").strip()
        if not recording_mbid:
            recording_mbid = extract_musicbrainz_track_id(str(track.source or ""))
            if recording_mbid:
                extra["musicbrainz_recording_id"] = recording_mbid
                if not isinstance(track.extra, dict):
                    track.extra = extra
        release_mbid = str(extra.get("musicbrainz_release_id") or "").strip()
        return recording_mbid, release_mbid

    def _format_track_label(self, track: PlaylistTrack, match: dict[str, Any]) -> str:
        artist = str(match.get("artist") or track.artist or "").strip()
        album = str(match.get("album") or track.album or "").strip()
        track_number = match.get("track_number") or track.track_number
        title = str(match.get("title") or track.title or "").strip()
        parts = [artist, album]
        if track_number:
            parts.append(str(track_number))
        if title:
            parts.append(title)
        label = " - ".join([part for part in parts if part])
        return label or "Unknown track"

    def _merge_candidates(self, *groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
        merged: dict[tuple[str, str], dict[str, Any]] = {}
        for group in groups:
            for candidate in group:
                provider = str(candidate.get("provider", "deezer"))
                candidate_id = str(
                    candidate.get("id")
                    or candidate.get("deezer_id")
                    or candidate.get("soundcloud_id")
                    or candidate.get("youtube_id")
                    or candidate.get("link")
                    or ""
                )
                key = (provider, candidate_id)
                existing = merged.get(key)
                if existing is None or float(candidate.get("score", 0)) > float(
                    existing.get("score", 0)
                ):
                    merged[key] = candidate
        return sorted(
            merged.values(),
            key=lambda item: (
                float(item.get("score", 0)),
                1 if item.get("provider") == "deezer" else 0,
            ),
            reverse=True,
        )

    def _quality_formats(self) -> list[str]:
        order = ["FLAC", "MP3_320", "MP3_128"]
        if self.quality in order:
            return order[order.index(self.quality) :]
        return order

    def _client(self, timeout: float | None) -> httpx.Client:
        return httpx.Client(
            follow_redirects=True,
            timeout=timeout,
            transport=self.transport,
        )

    def _validate_configuration(self) -> None:
        if not self.is_configured():
            raise ValueError(
                "Set `DEEZER_ARL` and `NAVIDROME_MUSIC_ROOT` to enable live downloads. "
                "SoundCloud is only offered during low-confidence manual review."
            )


# ------------------------------------------------------------------
# Crypto helpers
# ------------------------------------------------------------------


def _check_gw_error(payload: dict[str, Any]) -> None:
    """Raise if the Deezer GW response contains an error."""
    error = payload.get("error")
    if error:
        code = error.get("code", "")
        message = error.get("message", str(error))
        raise RuntimeError(f"Deezer GW error {code}: {message}")


def _blowfish_key(track_id: str) -> bytes:
    """Derive the per-track Blowfish decryption key from the track ID."""
    h = hashlib.md5(track_id.encode()).hexdigest()
    return bytes(ord(h[i]) ^ ord(h[i + 16]) ^ ord(_BF_SECRET[i]) for i in range(16))


def _decrypt_stream(
    chunks: Any,
    key: bytes,
    output: Any,
    *,
    progress_callback: Callable[[int], None] | None = None,
) -> None:
    """Write decrypted audio to *output*.

    Deezer encrypts every 3rd 2048-byte chunk (index % 3 == 0) with
    Blowfish CBC; the rest is written through unchanged.
    """
    iv = bytes(range(8))
    for index, chunk in enumerate(chunks):
        if index % 3 == 0 and len(chunk) == _CHUNK_SIZE:
            cipher = Blowfish.new(key, Blowfish.MODE_CBC, iv)
            chunk = cipher.decrypt(chunk)
        output.write(chunk)
        if progress_callback is not None:
            progress_callback(len(chunk))


def _safe_name(value: str) -> str:
    """Strip characters that are invalid in directory/file names."""
    cleaned = _SAFE_NAME_RE.sub("_", value).strip(". ")
    return cleaned[:100] or "Unknown"


def _coerce_track_number(value: Any) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return 0
    return number if number > 0 else 0


def _utc_timestamp() -> str:
    return datetime.now(UTC).isoformat()
