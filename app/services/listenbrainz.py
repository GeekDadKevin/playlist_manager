from __future__ import annotations

import json
import re
from collections.abc import Mapping
from typing import Any
from urllib.parse import urlparse

import httpx

_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)
_PLAYLIST_PATH_RE = re.compile(
    r"/playlist/([0-9a-fA-F-]{36})(?:/export/jspf)?/?$",
    re.IGNORECASE,
)


def normalize_listenbrainz_url(url: str) -> str:
    normalized = url.strip()
    if not normalized:
        raise ValueError("Provide a ListenBrainz playlist URL, playlist ID, or JSPF export URL.")

    if _UUID_RE.fullmatch(normalized):
        return f"https://listenbrainz.org/playlist/{normalized}/export/jspf"

    if normalized.startswith("listenbrainz.org/"):
        normalized = f"https://{normalized}"

    parsed = urlparse(normalized)
    match = _PLAYLIST_PATH_RE.search(parsed.path)
    if match:
        playlist_id = match.group(1)
        return f"https://listenbrainz.org/playlist/{playlist_id}/export/jspf"

    return normalized


def extract_listenbrainz_playlist_id(value: str) -> str:
    normalized = value.strip()
    if not normalized:
        return ""
    if _UUID_RE.fullmatch(normalized):
        return normalized

    if normalized.startswith("listenbrainz.org/"):
        normalized = f"https://{normalized}"

    parsed = urlparse(normalized)
    match = _PLAYLIST_PATH_RE.search(parsed.path)
    return match.group(1) if match else ""


def build_listenbrainz_api_url(
    value: str,
    base_url: str = "https://api.listenbrainz.org",
) -> str:
    playlist_id = extract_listenbrainz_playlist_id(value)
    if not playlist_id:
        return ""
    return f"{base_url.rstrip('/')}/1/playlist/{playlist_id}"


class ListenBrainzService:
    """Helper for resolving and fetching ListenBrainz playlist exports."""

    def __init__(
        self,
        base_url: str = "https://api.listenbrainz.org",
        username: str = "",
        auth_token: str = "",
        playlist_type: str = "createdfor",
        playlist_id: str = "",
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.username = username.strip()
        self.auth_token = auth_token.strip()
        self.playlist_type = playlist_type.strip().lower() or "createdfor"
        self.playlist_id = playlist_id.strip()
        self.transport = transport

    @classmethod
    def from_config(cls, config: Mapping[str, Any]) -> ListenBrainzService:
        return cls(
            base_url=str(config.get("LISTENBRAINZ_API_BASE_URL", "https://api.listenbrainz.org")),
            username=str(config.get("LISTENBRAINZ_USERNAME", "")),
            auth_token=str(config.get("LISTENBRAINZ_AUTH_TOKEN", "")),
        )

    def is_configured(self) -> bool:
        return bool(self.username)

    def list_playlists(
        self,
        count: int = 50,
        exclude_playlist_ids: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        if not self.username:
            return []

        excluded_ids = {
            playlist_id.lower() for playlist_id in (exclude_playlist_ids or set()) if playlist_id
        }
        playlists: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        errors: list[str] = []

        for source, source_label in (
            ("createdfor", "Created For You"),
            ("user", "Your Playlists"),
        ):
            try:
                fetched = self._fetch_playlists(
                    self._playlist_endpoint_for_type(source),
                    count=count,
                )
            except Exception as exc:
                errors.append(str(exc))
                continue

            for playlist in fetched:
                choice = self._build_playlist_choice(playlist, source, source_label)
                if choice is None:
                    continue
                if choice["playlist_id"].lower() in excluded_ids:
                    continue
                playlist_key = choice["playlist_id"] or choice["jspf_url"]
                if playlist_key in seen_ids:
                    continue
                seen_ids.add(playlist_key)
                playlists.append(choice)

        if not playlists and errors:
            raise ValueError(errors[0])

        return playlists

    def resolve_playlist_url(self, url: str = "") -> str:
        if url.strip():
            return normalize_listenbrainz_url(url)
        if self.playlist_id:
            return normalize_listenbrainz_url(self.playlist_id)
        if not self.username:
            raise ValueError(
                "Provide a ListenBrainz playlist URL/ID, or set `LISTENBRAINZ_USERNAME`."
            )

        playlist = self._fetch_first_playlist()
        identifier = self._playlist_identifier(playlist)
        if not identifier:
            raise ValueError("ListenBrainz returned a playlist without a usable identifier.")
        return normalize_listenbrainz_url(identifier)

    def fetch_jspf_document(self, url: str = "") -> dict:
        resolved_url = self.resolve_fetch_url(url)
        with httpx.Client(
            follow_redirects=True,
            timeout=15.0,
            transport=self.transport,
        ) as client:
            response = client.get(resolved_url, headers=self._auth_headers())
            response.raise_for_status()

        content_type = response.headers.get("content-type", "")
        if "json" in content_type.lower():
            return response.json()

        try:
            return json.loads(response.text)
        except json.JSONDecodeError as exc:
            raise ValueError(
                "ListenBrainz returned HTML instead of playlist JSON. "
                "Check the playlist selection or URL."
            ) from exc

    def lookup_recording_metadata(
        self,
        *,
        artist_name: str,
        recording_name: str,
        release_name: str = "",
    ) -> dict[str, Any]:
        if not self.auth_token:
            return {}

        artist_name = str(artist_name or "").strip()
        recording_name = str(recording_name or "").strip()
        release_name = str(release_name or "").strip()
        if not artist_name or not recording_name:
            return {}

        params = {
            "artist_name": artist_name,
            "recording_name": recording_name,
        }
        if release_name:
            params["release_name"] = release_name

        with httpx.Client(
            base_url=self.base_url,
            follow_redirects=True,
            timeout=15.0,
            transport=self.transport,
        ) as client:
            response = client.get(
                "/1/metadata/lookup/",
                params=params,
                headers=self._auth_headers(),
            )
            response.raise_for_status()

        payload = response.json()
        return {
            "recording_mbid": _first_nested_text(
                payload,
                "recording_mbid",
                "musicbrainz_recording_id",
                "recording_id",
            ),
            "release_mbid": _first_nested_text(
                payload,
                "release_mbid",
                "musicbrainz_release_id",
                "release_id",
            ),
        }

    def lookup_recording_mbid(
        self,
        *,
        artist_name: str,
        recording_name: str,
        release_name: str = "",
    ) -> str:
        metadata = self.lookup_recording_metadata(
            artist_name=artist_name,
            recording_name=recording_name,
            release_name=release_name,
        )
        return str(metadata.get("recording_mbid") or "").strip()

    def resolve_fetch_url(self, url: str = "") -> str:
        direct_value = url.strip() or self.playlist_id
        api_url = build_listenbrainz_api_url(direct_value, base_url=self.base_url)
        if api_url:
            return api_url

        if url.strip():
            return url.strip()

        if not self.username:
            raise ValueError(
                "Provide a ListenBrainz playlist URL/ID, or set `LISTENBRAINZ_USERNAME`."
            )

        playlist = self._fetch_first_playlist()
        identifier = self._playlist_identifier(playlist)
        if not identifier:
            raise ValueError("ListenBrainz returned a playlist without a usable identifier.")

        api_url = build_listenbrainz_api_url(identifier, base_url=self.base_url)
        return api_url or normalize_listenbrainz_url(identifier)

    def _fetch_first_playlist(self) -> dict[str, Any]:
        playlists = self._fetch_playlists(
            self._playlist_endpoint_for_type(self.playlist_type),
            count=1,
        )
        if not playlists:
            raise ValueError(
                "No ListenBrainz playlists were found for "
                f"`{self.username}` using `{self.playlist_type}`."
            )
        return playlists[0]

    def _fetch_playlists(self, endpoint: str, count: int = 50) -> list[dict[str, Any]]:
        with httpx.Client(
            base_url=self.base_url,
            follow_redirects=True,
            timeout=15.0,
            transport=self.transport,
        ) as client:
            response = client.get(
                endpoint,
                params={"count": count, "offset": 0},
                headers=self._auth_headers(),
            )
            response.raise_for_status()

        payload = response.json()
        payload_data = payload.get("payload", payload)
        playlists = payload_data.get("playlists") or payload_data.get("playlist") or []
        if isinstance(playlists, dict):
            playlists = [playlists]

        normalized_playlists: list[dict[str, Any]] = []
        for item in playlists:
            if not isinstance(item, dict):
                continue
            nested = item.get("playlist")
            if isinstance(nested, dict):
                normalized_playlists.append(nested)
            else:
                normalized_playlists.append(item)

        return normalized_playlists

    def _playlist_endpoint_for_type(self, playlist_type: str) -> str:
        normalized = playlist_type.strip().lower()
        if normalized in {"createdfor", "created-for-you", "created_for_you"}:
            return f"/1/user/{self.username}/playlists/createdfor"
        return f"/1/user/{self.username}/playlists"

    def _build_playlist_choice(
        self,
        playlist: Mapping[str, Any],
        source: str,
        source_label: str,
    ) -> dict[str, Any] | None:
        identifier = self._playlist_identifier(playlist)
        if not identifier:
            return None

        playlist_id = extract_listenbrainz_playlist_id(identifier)
        title = str(playlist.get("title") or playlist.get("name") or "Untitled Playlist").strip()
        description = str(playlist.get("description") or "").strip()

        return {
            "title": title,
            "description": description,
            "source": source,
            "source_label": source_label,
            "playlist_id": playlist_id,
            "jspf_url": normalize_listenbrainz_url(identifier),
            "selected": bool(playlist_id and playlist_id == self.playlist_id),
        }

    @staticmethod
    def _playlist_identifier(playlist: Mapping[str, Any]) -> str:
        return str(
            playlist.get("identifier") or playlist.get("playlist_mbid") or playlist.get("id") or ""
        ).strip()

    def _auth_headers(self) -> dict[str, str]:
        if not self.auth_token:
            return {}
        return {"Authorization": f"Token {self.auth_token}"}


def _first_nested_text(payload: Any, *keys: str) -> str:
    wanted = {key.strip().lower() for key in keys if key.strip()}
    if not wanted:
        return ""

    def walk(value: Any) -> str:
        if isinstance(value, dict):
            for key, nested in value.items():
                if str(key).strip().lower() in wanted:
                    text = str(nested or "").strip()
                    if text:
                        return text
                found = walk(nested)
                if found:
                    return found
        elif isinstance(value, list):
            for item in value:
                found = walk(item)
                if found:
                    return found
        return ""

    return walk(payload)
