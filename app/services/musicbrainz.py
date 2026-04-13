from __future__ import annotations

import time
from threading import Lock
from typing import Any

import httpx


class MusicBrainzService:
    """Resolve track numbers from MusicBrainz recording/release metadata."""

    def __init__(
        self,
        base_url: str = "https://musicbrainz.org",
        user_agent: str = "playlist_manager/1.0 (local)",
        rate_limit_seconds: float = 1.0,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/") or "https://musicbrainz.org"
        self.user_agent = user_agent.strip() or "playlist_manager/1.0 (local)"
        self.rate_limit_seconds = max(float(rate_limit_seconds or 1.0), 0.1)
        self.transport = transport
        self._last_request_ts = 0.0
        self._cache: dict[tuple[str, str, str, str], int | None] = {}
        self._lock = Lock()

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> MusicBrainzService:
        return cls(
            base_url=str(config.get("MUSICBRAINZ_API_BASE_URL", "https://musicbrainz.org")),
            user_agent=str(
                config.get("MUSICBRAINZ_USER_AGENT", "playlist_manager/1.0 (local)")
            ),
            rate_limit_seconds=float(config.get("MUSICBRAINZ_RATE_LIMIT_SECONDS", 1.0) or 1.0),
        )

    def lookup_track_number(
        self,
        recording_mbid: str = "",
        release_mbid: str = "",
        album_name: str = "",
        artist_name: str = "",
    ) -> int | None:
        recording_mbid = str(recording_mbid or "").strip()
        release_mbid = str(release_mbid or "").strip()
        album_name = str(album_name or "").strip()
        artist_name = str(artist_name or "").strip()
        if not recording_mbid and not release_mbid:
            return None

        cache_key = (
            recording_mbid,
            release_mbid,
            album_name.lower(),
            artist_name.lower(),
        )
        with self._lock:
            if cache_key in self._cache:
                return self._cache[cache_key]

        track_number: int | None = None
        if release_mbid:
            track_number = self._track_number_from_release(release_mbid, recording_mbid)

        if track_number is None and recording_mbid:
            release_id = self._select_release_for_recording(
                recording_mbid,
                album_name=album_name,
                artist_name=artist_name,
            )
            if release_id:
                track_number = self._track_number_from_release(release_id, recording_mbid)

        with self._lock:
            self._cache[cache_key] = track_number
        return track_number

    def _track_number_from_release(self, release_mbid: str, recording_mbid: str) -> int | None:
        payload = self._get_json(
            f"/ws/2/release/{release_mbid}",
            {"inc": "recordings+media"},
        )
        media = payload.get("media", []) if isinstance(payload, dict) else []
        for medium in media:
            tracks = medium.get("tracks", []) if isinstance(medium, dict) else []
            for track in tracks:
                recording = track.get("recording", {}) if isinstance(track, dict) else {}
                if recording_mbid and recording.get("id") != recording_mbid:
                    continue
                number = track.get("number") or track.get("position")
                return _coerce_track_number(number)
        return None

    def _select_release_for_recording(
        self,
        recording_mbid: str,
        *,
        album_name: str,
        artist_name: str,
    ) -> str:
        payload = self._get_json(
            f"/ws/2/recording/{recording_mbid}",
            {"inc": "releases+artist-credits"},
        )
        releases = payload.get("releases", []) if isinstance(payload, dict) else []
        if not releases:
            return ""

        album_norm = _normalize(album_name)
        artist_norm = _normalize(artist_name)
        best_score = -1
        best_release = ""
        for release in releases[:25]:
            if not isinstance(release, dict):
                continue
            release_id = str(release.get("id") or "").strip()
            if not release_id:
                continue
            title = _normalize(str(release.get("title") or ""))
            score = 0
            if album_norm and title == album_norm:
                score += 3
            elif album_norm and album_norm in title:
                score += 2
            elif album_norm:
                score -= 1

            if artist_norm:
                credit = release.get("artist-credit", [])
                credit_names = _normalize(" ".join(_artist_credit_names(credit)))
                if credit_names and artist_norm in credit_names:
                    score += 2

            if score > best_score:
                best_score = score
                best_release = release_id

        if best_release:
            return best_release
        return str(releases[0].get("id") or "").strip()

    def _get_json(self, path: str, params: dict[str, str]) -> dict[str, Any]:
        with self._lock:
            now = time.time()
            delta = now - self._last_request_ts
            if delta < self.rate_limit_seconds:
                time.sleep(self.rate_limit_seconds - delta)
            self._last_request_ts = time.time()

        with httpx.Client(
            base_url=self.base_url,
            follow_redirects=True,
            timeout=15.0,
            transport=self.transport,
            headers={"User-Agent": self.user_agent},
        ) as client:
            response = client.get(path, params={"fmt": "json", **params})
            response.raise_for_status()
            return response.json()


def _artist_credit_names(credit: list[Any]) -> list[str]:
    names: list[str] = []
    for item in credit:
        if isinstance(item, dict):
            name = str(item.get("name") or item.get("artist", {}).get("name") or "")
            if name:
                names.append(name)
        elif isinstance(item, str):
            names.append(item)
    return names


def _normalize(value: str) -> str:
    return " ".join(value.lower().strip().split())


def _coerce_track_number(value: Any) -> int | None:
    try:
        number = int(str(value).strip())
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None
