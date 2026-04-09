from __future__ import annotations

import hashlib
import logging
import re
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
from Crypto.Cipher import Blowfish

from app.matching import rank_candidates
from app.matching.normalize import build_search_queries, normalize_text
from app.models import PlaylistTrack
from app.services.song_metadata import write_flac_tags, write_song_metadata_xml

log = logging.getLogger(__name__)

_DEEZER_GW = "https://www.deezer.com/ajax/gw-light.php"
_DEEZER_MEDIA_URL = "https://media.deezer.com/v1/get_url"
_DEEZER_SEARCH_URL = "https://api.deezer.com/search"
_BF_SECRET = "g4el58wc0zvf9na1"
_CHUNK_SIZE = 2048
_SAFE_NAME_RE = re.compile(r'[<>:"/\\|?*\x00-\x1f]')


class DeezerDownloadService:
    """Downloads tracks directly from Deezer using ARL authentication."""

    def __init__(
        self,
        arl: str = "",
        download_dir: str = "/navidrome/root",
        navidrome_music_root: str = "",
        quality: str = "FLAC",
        match_threshold: float = 72.0,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.arl = arl.strip()
        self.download_dir = download_dir
        self.navidrome_music_root = navidrome_music_root
        self.quality = quality.upper()
        self.match_threshold = match_threshold
        self.transport = transport
        self._api_token = ""
        self._license_token = ""
        self._gw_cookies: dict[str, str] = {}

    @classmethod
    def from_config(cls, config: Any) -> DeezerDownloadService:
        music_root = str(config.get("NAVIDROME_MUSIC_ROOT", "/navidrome/root"))
        return cls(
            arl=str(config.get("DEEZER_ARL", "")),
            download_dir=music_root,
            navidrome_music_root=music_root,
            quality=str(config.get("DEEZER_QUALITY", "FLAC")),
            match_threshold=float(config.get("DEEZER_MATCH_THRESHOLD", 72.0)),
        )

    def is_configured(self) -> bool:
        return bool(self.arl and self.download_dir)

    def search_track(self, track: PlaylistTrack, limit: int = 10) -> list[dict[str, Any]]:
        return self._search_track(track, limit=limit)

    def sync_tracks(
        self,
        tracks: list[PlaylistTrack],
        max_tracks: int | None = None,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        self._validate_configuration()
        self._ensure_authenticated()

        started_at = _utc_timestamp()
        selected = tracks[:max_tracks] if max_tracks else tracks
        log.info("Starting Deezer sync: %d track(s), quality=%s, threshold=%.0f",
                 len(selected), self.quality, self.match_threshold)
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

        for index, track in enumerate(selected, start=1):
            result = self._sync_single_track(track)
            result["index"] = index
            result["completed_at"] = _utc_timestamp()
            results.append(result)
            summary["processed"] = index
            status = result["status"]
            if status in summary:
                summary[status] += 1

            if progress_callback is not None:
                progress_callback({
                    "mode": "download",
                    "provider": "deezer",
                    "threshold": self.match_threshold,
                    "processing_mode": "sequential",
                    "started_at": started_at,
                    "completed_at": "",
                    "summary": dict(summary),
                    "results": list(results),
                })

        final: dict[str, Any] = {
            "mode": "download",
            "provider": "deezer",
            "threshold": self.match_threshold,
            "processing_mode": "sequential",
            "started_at": started_at,
            "completed_at": _utc_timestamp(),
            "summary": summary,
            "results": results,
        }
        if progress_callback is not None:
            progress_callback(final)
        return final

    # ------------------------------------------------------------------
    # Per-track logic
    # ------------------------------------------------------------------

    def _sync_single_track(self, track: PlaylistTrack) -> dict[str, Any]:
        log.info("Processing: %r by %r", track.title, track.artist)
        result: dict[str, Any] = {
            "track": track.to_dict(),
            "queries": build_search_queries(track),
            "status": "not_found",
            "message": "No Deezer match found.",
            "match": {},
        }

        try:
            ranked = self._search_track(track)
        except Exception as exc:
            log.error("Search failed for %r: %s", track.title, exc)
            result["status"] = "failed"
            result["message"] = f"Deezer search error: {exc}"
            return result

        if not ranked:
            log.warning("No Deezer results for %r by %r", track.title, track.artist)
            return result

        top = ranked[0]
        result["match"] = top
        log.info(
            "Best match: %r by %r (score=%.1f, accepted=%s, deezer_id=%s)",
            top.get("title"), top.get("artist"),
            top.get("score", 0), top.get("accepted"), top.get("deezer_id"),
        )

        if not top.get("accepted", False):
            log.warning(
                "Low confidence for %r — score %.1f < threshold %.0f",
                track.title, top.get("score", 0), self.match_threshold,
            )
            result["status"] = "low_confidence"
            result["message"] = "Best match was below the configured confidence threshold."
            return result

        deezer_id = top.get("deezer_id")
        if not deezer_id:
            log.error("Match for %r has no deezer_id — cannot download", track.title)
            return result

        in_library = self._find_in_library(top, track)
        if in_library is not None:
            log.info("Already in library: %s", in_library)
            result["status"] = "already_available"
            result["message"] = "Track is already in the Navidrome library."
            result["match"] = {**top, "path": str(in_library)}
            return result

        existing = self._find_existing(top, track)
        if existing is not None:
            log.info("Already downloaded: %s", existing)
            result["status"] = "already_available"
            result["message"] = "Track is already in the download directory."
            result["match"] = {**top, "path": str(existing)}
            return result

        try:
            file_path, metadata_path = self._download_track(deezer_id, top, track)
        except Exception as exc:
            log.error("Download failed for %r (id=%s): %s", track.title, deezer_id, exc)
            result["status"] = "failed"
            result["message"] = str(exc)
            return result

        log.info("Downloaded: %s", file_path)
        result["status"] = "downloaded"
        result["message"] = "Track downloaded from Deezer."
        result["match"] = {**top, "path": str(file_path)}
        result["download"] = {
            "deezer_id": deezer_id,
            "path": str(file_path),
            "metadata_path": str(metadata_path),
            "completed_at": _utc_timestamp(),
        }
        return result

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    def _search_track(self, track: PlaylistTrack, limit: int = 10) -> list[dict[str, Any]]:
        """Try each query variant, return the best-ranked results found."""
        best: list[dict[str, Any]] = []
        queries = build_search_queries(track)
        log.debug("Search queries for %r: %s", track.title, queries)
        for query in queries:
            candidates = self._deezer_search(query, limit)
            log.debug("Query %r → %d candidate(s)", query, len(candidates))
            if not candidates:
                continue
            ranked = rank_candidates(track, candidates, threshold=self.match_threshold)
            if ranked and (not best or ranked[0]["score"] > best[0]["score"]):
                best = ranked
            if ranked and ranked[0]["accepted"]:
                log.debug("Accepted match found on query %r", query)
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
                "deezer_id": item.get("id"),
                "link": item.get("link", ""),
                "title": item.get("title", ""),
                "artist": item.get("artist", {}).get("name", ""),
                "artist_id": item.get("artist", {}).get("id"),
                "album": item.get("album", {}).get("title", ""),
                "album_id": item.get("album", {}).get("id"),
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
        self._license_token = str(
            data.get("USER", {}).get("OPTIONS", {}).get("license_token", "")
        )
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
                                {"cipher": "BF_CBC_STRIPE", "format": fmt}
                                for fmt in formats
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
                with open(output_path, "wb") as fh:
                    _decrypt_stream(response.iter_bytes(_CHUNK_SIZE), bf_key, fh)

        metadata_path = self._write_metadata_xml(
            output_path,
            match,
            track,
            deezer_id=deezer_id,
            fmt=fmt,
        )
        if output_path.suffix.lower() == ".flac":
            try:
                self._write_flac_tags(
                    output_path,
                    match,
                    track,
                    deezer_id=deezer_id,
                    fmt=fmt,
                )
            except Exception as exc:
                log.warning("Could not write FLAC tags for %s: %s", output_path, exc)

        size_kb = output_path.stat().st_size // 1024
        log.info("Download complete: %s (%d KB)", output_path.name, size_kb)
        log.info("Metadata sidecar written: %s", metadata_path.name)
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
        return Path(self.download_dir) / artist / album / f"{title}{ext}"

    def _write_metadata_xml(
        self,
        audio_path: Path,
        match: dict[str, Any],
        track: PlaylistTrack,
        *,
        deezer_id: int | str,
        fmt: str,
    ) -> Path:
        annotation = (
            str(track.extra.get("annotation", ""))
            if isinstance(track.extra, dict)
            else ""
        )
        return write_song_metadata_xml(
            audio_path,
            title=str(match.get("title", "") or track.title or audio_path.stem),
            artist=str(match.get("artist", "") or track.artist or ""),
            album=str(match.get("album", "") or track.album or ""),
            duration_seconds=match.get("duration_seconds") or track.duration_seconds,
            provider="deezer",
            deezer_id=deezer_id,
            deezer_artist_id=match.get("artist_id"),
            deezer_album_id=match.get("album_id"),
            deezer_link=str(match.get("link", "")),
            quality=str(fmt or self.quality),
            source=str(track.source or ""),
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
        annotation = (
            str(track.extra.get("annotation", ""))
            if isinstance(track.extra, dict)
            else ""
        )
        return write_flac_tags(
            audio_path,
            title=str(match.get("title", "") or track.title or audio_path.stem),
            artist=str(match.get("artist", "") or track.artist or ""),
            album=str(match.get("album", "") or track.album or ""),
            duration_seconds=match.get("duration_seconds") or track.duration_seconds,
            provider="deezer",
            deezer_id=deezer_id,
            deezer_artist_id=match.get("artist_id"),
            deezer_album_id=match.get("album_id"),
            deezer_link=str(match.get("link", "")),
            source=str(track.source or ""),
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

    def _quality_formats(self) -> list[str]:
        order = ["FLAC", "MP3_320", "MP3_128"]
        if self.quality in order:
            return order[order.index(self.quality):]
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
                "Set `DEEZER_ARL` and `NAVIDROME_MUSIC_ROOT` to enable direct Deezer downloads."
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


def _safe_name(value: str) -> str:
    """Strip characters that are invalid in directory/file names."""
    cleaned = _SAFE_NAME_RE.sub("_", value).strip(". ")
    return cleaned[:100] or "Unknown"


def _utc_timestamp() -> str:
    return datetime.now(UTC).isoformat()
