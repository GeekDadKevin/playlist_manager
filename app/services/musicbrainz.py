from __future__ import annotations

import time
from threading import Lock
from typing import Any

import httpx

_TRANSIENT_STATUS_CODES = {429, 500, 502, 503, 504}
_REQUEST_ATTEMPTS = 3
_RETRY_BASE_SECONDS = 1.0


class MusicBrainzService:
    """Resolve recording IDs and track numbers from MusicBrainz metadata."""

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
        self._recording_cache: dict[tuple[str, str, str], str] = {}
        self._lock = Lock()

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> MusicBrainzService:
        return cls(
            base_url=str(
                config.get("MUSICBRAINZ_API_BASE_URL", "https://musicbrainz.org")
            ),
            user_agent=str(
                config.get("MUSICBRAINZ_USER_AGENT", "playlist_manager/1.0 (local)")
            ),
            rate_limit_seconds=float(
                config.get("MUSICBRAINZ_RATE_LIMIT_SECONDS", 1.0) or 1.0
            ),
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
                track_number = self._track_number_from_release(
                    release_id, recording_mbid
                )

        with self._lock:
            self._cache[cache_key] = track_number
        return track_number

    def lookup_recording_mbid(
        self,
        *,
        title: str,
        artist_name: str = "",
        album_name: str = "",
    ) -> str:
        title = str(title or "").strip()
        artist_name = str(artist_name or "").strip()
        album_name = str(album_name or "").strip()
        if not title:
            return ""

        cache_key = (title.lower(), artist_name.lower(), album_name.lower())
        with self._lock:
            if cache_key in self._recording_cache:
                return self._recording_cache[cache_key]

        recording_mbid = ""
        for query in self._build_recording_queries(
            title=title,
            artist_name=artist_name,
            album_name=album_name,
        ):
            payload = self._get_json(
                "/ws/2/recording",
                {"query": query, "limit": "10"},
            )
            recordings = (
                payload.get("recordings", []) if isinstance(payload, dict) else []
            )
            recording_mbid = self._select_recording_mbid(
                recordings,
                title=title,
                artist_name=artist_name,
                album_name=album_name,
            )
            if recording_mbid:
                break

        with self._lock:
            self._recording_cache[cache_key] = recording_mbid
        return recording_mbid

    def lookup_recording_details(
        self,
        *,
        title: str,
        artist_name: str = "",
        album_name: str = "",
        recording_mbid: str = "",
        release_mbid: str = "",
    ) -> dict[str, Any]:
        recording_mbid = str(recording_mbid or "").strip()
        release_mbid = str(release_mbid or "").strip()
        title = str(title or "").strip()
        artist_name = str(artist_name or "").strip()
        album_name = str(album_name or "").strip()

        recording_payload: dict[str, Any] = {}
        selected_recording: dict[str, Any] | None = None
        if recording_mbid:
            recording_payload = self._get_json(
                f"/ws/2/recording/{recording_mbid}",
                {"inc": "releases+artist-credits+isrcs+genres"},
            )
            if isinstance(recording_payload, dict):
                selected_recording = recording_payload
        else:
            for query in self._build_recording_queries(
                title=title,
                artist_name=artist_name,
                album_name=album_name,
            ):
                payload = self._get_json(
                    "/ws/2/recording",
                    {
                        "query": query,
                        "limit": "10",
                    },
                )
                recordings = (
                    payload.get("recordings", []) if isinstance(payload, dict) else []
                )
                selected_recording = self._select_recording(
                    recordings,
                    title=title,
                    artist_name=artist_name,
                    album_name=album_name,
                )
                if selected_recording is not None:
                    break
            if selected_recording is None:
                return {}
            recording_mbid = str(selected_recording.get("id") or "").strip()
            recording_payload = selected_recording

        if not recording_mbid:
            return {}

        if not release_mbid:
            release_mbid = self._select_release_for_recording(
                recording_mbid,
                album_name=album_name,
                artist_name=artist_name,
            )

        release_payload: dict[str, Any] = {}
        if release_mbid:
            release_payload = self._get_json(
                f"/ws/2/release/{release_mbid}",
                {"inc": "recordings+media+artist-credits+labels+release-groups+genres"},
            )

        release_title = str(release_payload.get("title") or "").strip() or album_name
        artist_credit = recording_payload.get("artist-credit", [])
        artist_value = (
            " ".join(_artist_credit_names(artist_credit)).strip() or artist_name
        )
        artist_mbid = _first_artist_credit_id(artist_credit)
        artist_sort = (
            " ".join(_artist_credit_sort_names(artist_credit)).strip() or artist_value
        )
        album_artist_credit = release_payload.get("artist-credit", []) or artist_credit
        album_artist_value = (
            " ".join(_artist_credit_names(album_artist_credit)).strip() or artist_value
        )
        album_artist_mbid = _first_artist_credit_id(album_artist_credit) or artist_mbid
        album_artist_sort = (
            " ".join(_artist_credit_sort_names(album_artist_credit)).strip()
            or album_artist_value
        )
        track_context = (
            _release_track_context(release_payload, recording_mbid)
            if release_payload
            else {
                "track_number": None,
                "track_total": None,
                "disc_number": None,
                "disc_total": None,
                "media_format": "",
            }
        )
        release_group = release_payload.get("release-group", {})
        text_representation = release_payload.get("text-representation", {})
        label_info = _first_label_info(release_payload.get("label-info"))
        genres = _collect_genres(recording_payload, release_payload, release_group)
        secondary_types = _coerce_string_list(release_group.get("secondary-types"))

        return {
            "recording_mbid": recording_mbid,
            "release_mbid": release_mbid,
            "release_group_mbid": str(release_group.get("id") or "").strip(),
            "title": str(recording_payload.get("title") or title).strip(),
            "artist": artist_value,
            "artist_sort": artist_sort,
            "album": release_title,
            "albumartist": album_artist_value,
            "albumartist_sort": album_artist_sort,
            "artist_mbid": artist_mbid,
            "albumartist_mbid": album_artist_mbid,
            "track_number": track_context["track_number"],
            "track_total": track_context["track_total"],
            "disc_number": track_context["disc_number"],
            "disc_total": track_context["disc_total"],
            "date": str(release_payload.get("date") or "").strip(),
            "original_date": str(release_group.get("first-release-date") or "").strip(),
            "genre": "; ".join(genres),
            "isrc": "; ".join(_coerce_string_list(recording_payload.get("isrcs"))),
            "barcode": str(release_payload.get("barcode") or "").strip(),
            "label": label_info["label"],
            "catalog_number": label_info["catalog_number"],
            "media_format": str(track_context["media_format"] or "").strip(),
            "release_country": str(release_payload.get("country") or "").strip(),
            "release_status": str(release_payload.get("status") or "").strip(),
            "release_type": str(release_group.get("primary-type") or "").strip(),
            "release_secondary_types": "; ".join(secondary_types),
            "language": str(text_representation.get("language") or "").strip(),
            "script": str(text_representation.get("script") or "").strip(),
            "recording_disambiguation": str(
                recording_payload.get("disambiguation") or ""
            ).strip(),
            "album_disambiguation": str(
                release_payload.get("disambiguation") or ""
            ).strip(),
        }

    def _track_number_from_release(
        self, release_mbid: str, recording_mbid: str
    ) -> int | None:
        payload = self._get_json(
            f"/ws/2/release/{release_mbid}",
            {"inc": "recordings+media"},
        )
        media = payload.get("media", []) if isinstance(payload, dict) else []
        for medium in media:
            tracks = medium.get("tracks", []) if isinstance(medium, dict) else []
            for track in tracks:
                recording = (
                    track.get("recording", {}) if isinstance(track, dict) else {}
                )
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
        last_error: Exception | None = None
        for attempt in range(1, _REQUEST_ATTEMPTS + 1):
            with self._lock:
                now = time.time()
                delta = now - self._last_request_ts
                if delta < self.rate_limit_seconds:
                    time.sleep(self.rate_limit_seconds - delta)
                self._last_request_ts = time.time()

            try:
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
            except httpx.HTTPStatusError as exc:
                last_error = exc
                status_code = (
                    exc.response.status_code if exc.response is not None else 0
                )
                if (
                    status_code not in _TRANSIENT_STATUS_CODES
                    or attempt >= _REQUEST_ATTEMPTS
                ):
                    raise
            except httpx.RequestError as exc:
                last_error = exc
                if attempt >= _REQUEST_ATTEMPTS:
                    raise

            time.sleep(_RETRY_BASE_SECONDS * attempt)

        if last_error is not None:
            raise last_error
        raise RuntimeError("MusicBrainz request failed unexpectedly.")

    def _select_recording_mbid(
        self,
        recordings: list[Any],
        *,
        title: str,
        artist_name: str,
        album_name: str,
    ) -> str:
        selected = self._select_recording(
            recordings,
            title=title,
            artist_name=artist_name,
            album_name=album_name,
        )
        if selected is None:
            return ""
        return str(selected.get("id") or "").strip()

    def _select_recording(
        self,
        recordings: list[Any],
        *,
        title: str,
        artist_name: str,
        album_name: str,
    ) -> dict[str, Any] | None:
        wanted_title = _normalize(title)
        wanted_artist = _normalize(artist_name)
        wanted_album = _normalize(album_name)
        best_score = -10_000
        best_item: dict[str, Any] | None = None

        for item in recordings[:10]:
            if not isinstance(item, dict):
                continue
            recording_id = str(item.get("id") or "").strip()
            if not recording_id:
                continue

            score = int(str(item.get("score") or "0").strip() or 0)
            title_value = _normalize(str(item.get("title") or ""))
            if wanted_title and title_value == wanted_title:
                score += 40
            elif wanted_title and (
                wanted_title in title_value or title_value in wanted_title
            ):
                score += 20
            elif wanted_title:
                score -= 30

            credit_names = _normalize(
                " ".join(_artist_credit_names(item.get("artist-credit", [])))
            )
            if wanted_artist and credit_names == wanted_artist:
                score += 30
            elif wanted_artist and wanted_artist in credit_names:
                score += 15
            elif wanted_artist:
                score -= 15

            releases = (
                item.get("releases", [])
                if isinstance(item.get("releases"), list)
                else []
            )
            release_titles = [
                _normalize(str(release.get("title") or ""))
                for release in releases
                if isinstance(release, dict)
            ]
            if wanted_album and wanted_album in release_titles:
                score += 20
            elif wanted_album and any(
                wanted_album in release_title for release_title in release_titles
            ):
                score += 8

            if score > best_score:
                best_score = score
                best_item = item

        return best_item if best_score > 0 else None

    def _build_recording_query(
        self,
        *,
        title: str,
        artist_name: str,
        album_name: str,
    ) -> str:
        query_parts = [f'recording:"{_escape_query_value(title)}"']
        if artist_name:
            query_parts.append(f'artist:"{_escape_query_value(artist_name)}"')
        if album_name:
            query_parts.append(f'release:"{_escape_query_value(album_name)}"')
        return " AND ".join(query_parts)

    def _build_recording_queries(
        self,
        *,
        title: str,
        artist_name: str,
        album_name: str,
    ) -> list[str]:
        variants = [
            self._build_recording_query(
                title=title,
                artist_name=artist_name,
                album_name=album_name,
            )
        ]
        if title and artist_name and album_name:
            variants.append(
                self._build_recording_query(
                    title=title,
                    artist_name=artist_name,
                    album_name="",
                )
            )
        if title and album_name and not artist_name:
            variants.append(
                self._build_recording_query(
                    title=title,
                    artist_name="",
                    album_name=album_name,
                )
            )
        if title:
            variants.append(
                self._build_recording_query(
                    title=title,
                    artist_name="",
                    album_name="",
                )
            )

        unique: list[str] = []
        seen: set[str] = set()
        for query in variants:
            normalized = str(query or "").strip()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            unique.append(normalized)
        return unique


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


def _artist_credit_sort_names(credit: list[Any]) -> list[str]:
    names: list[str] = []
    for item in credit:
        if isinstance(item, dict):
            artist = (
                item.get("artist", {}) if isinstance(item.get("artist"), dict) else {}
            )
            name = str(
                artist.get("sort-name") or item.get("name") or artist.get("name") or ""
            )
            if name:
                names.append(name)
        elif isinstance(item, str):
            names.append(item)
    return names


def _release_track_context(
    release_payload: dict[str, Any], recording_mbid: str
) -> dict[str, Any]:
    media = (
        release_payload.get("media", []) if isinstance(release_payload, dict) else []
    )
    disc_total = len([medium for medium in media if isinstance(medium, dict)]) or None
    for medium_index, medium in enumerate(media, start=1):
        if not isinstance(medium, dict):
            continue
        tracks = (
            medium.get("tracks", []) if isinstance(medium.get("tracks"), list) else []
        )
        track_total = _coerce_track_number(medium.get("track-count")) or len(
            [track for track in tracks if isinstance(track, dict)]
        )
        for track in tracks:
            if not isinstance(track, dict):
                continue
            recording_value = track.get("recording")
            recording = recording_value if isinstance(recording_value, dict) else {}
            if (
                recording_mbid
                and str(recording.get("id") or "").strip() != recording_mbid
            ):
                continue
            return {
                "track_number": _coerce_track_number(
                    track.get("number") or track.get("position")
                ),
                "track_total": track_total or None,
                "disc_number": _coerce_track_number(medium.get("position"))
                or medium_index,
                "disc_total": disc_total,
                "media_format": str(medium.get("format") or "").strip(),
            }

    return {
        "track_number": None,
        "track_total": None,
        "disc_number": None,
        "disc_total": disc_total,
        "media_format": "",
    }


def _first_label_info(value: Any) -> dict[str, str]:
    if not isinstance(value, list):
        return {"label": "", "catalog_number": ""}
    for item in value:
        if not isinstance(item, dict):
            continue
        label = item.get("label", {}) if isinstance(item.get("label"), dict) else {}
        label_name = str(label.get("name") or item.get("name") or "").strip()
        catalog_number = str(item.get("catalog-number") or "").strip()
        if label_name or catalog_number:
            return {
                "label": label_name,
                "catalog_number": catalog_number,
            }
    return {"label": "", "catalog_number": ""}


def _collect_genres(*sources: Any) -> list[str]:
    names: list[str] = []
    seen: set[str] = set()
    for source in sources:
        genres = source.get("genres", []) if isinstance(source, dict) else []
        for genre in genres:
            if not isinstance(genre, dict):
                continue
            name = str(genre.get("name") or "").strip()
            if not name:
                continue
            key = name.casefold()
            if key in seen:
                continue
            seen.add(key)
            names.append(name)
    return names


def _coerce_string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    items: list[str] = []
    for item in value:
        text = str(item or "").strip()
        if text:
            items.append(text)
    return items


def _first_artist_credit_id(credit: list[Any]) -> str:
    for item in credit:
        if not isinstance(item, dict):
            continue
        artist = item.get("artist")
        if not isinstance(artist, dict):
            continue
        artist_id = str(artist.get("id") or "").strip()
        if artist_id:
            return artist_id
    return ""


def _normalize(value: str) -> str:
    return " ".join(value.lower().strip().split())


def _escape_query_value(value: str) -> str:
    return str(value).replace('"', '\\"').strip()


def _coerce_track_number(value: Any) -> int | None:
    try:
        number = int(str(value).strip())
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None
