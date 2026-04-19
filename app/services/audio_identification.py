from __future__ import annotations

import json
import shutil
import subprocess
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import httpx

from app.services.musicbrainz import MusicBrainzService
from app.services.song_metadata import (
    guess_preliminary_metadata,
    load_embedded_audio_metadata,
    load_song_metadata_xml,
)

_ACOUSTID_LOOKUP_META = "recordings releasegroups releases"


def lookup_musicbrainz_metadata_match(
    audio_path: str | Path,
    *,
    musicbrainz_service: MusicBrainzService,
    root: str | Path | None = None,
) -> dict[str, Any]:
    path = Path(audio_path).expanduser().resolve()
    embedded = load_embedded_audio_metadata(path)
    xml_data = load_song_metadata_xml(path.with_suffix(".xml"))
    guessed = guess_preliminary_metadata(path, root=root)

    title = str(
        embedded.get("title")
        or xml_data.get("title")
        or guessed.get("title")
        or path.stem
    ).strip()
    artist = str(
        embedded.get("artist")
        or embedded.get("albumartist")
        or xml_data.get("performingartist")
        or xml_data.get("artist")
        or guessed.get("artist")
    ).strip()
    album = str(
        embedded.get("album")
        or xml_data.get("albumtitle")
        or xml_data.get("album")
        or guessed.get("album")
    ).strip()

    variants = _metadata_search_variants(title=title, artist=artist, album=album)
    for variant in variants:
        try:
            details = musicbrainz_service.lookup_recording_details(
                title=variant["title"],
                artist_name=variant["artist"],
                album_name=variant["album"],
            )
        except Exception:
            continue
        if details.get("recording_mbid"):
            details["match_source"] = "musicbrainz_metadata"
            return details
    return {}


def _metadata_search_variants(
    *, title: str, artist: str, album: str
) -> list[dict[str, str]]:
    candidates = [
        {"title": title, "artist": artist, "album": album},
        {"title": title, "artist": artist, "album": ""},
        {"title": title, "artist": "", "album": album},
        {"title": title, "artist": "", "album": ""},
    ]
    variants: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for candidate in candidates:
        normalized = (
            str(candidate.get("title") or "").strip(),
            str(candidate.get("artist") or "").strip(),
            str(candidate.get("album") or "").strip(),
        )
        if not normalized[0] or normalized in seen:
            continue
        seen.add(normalized)
        variants.append(
            {
                "title": normalized[0],
                "artist": normalized[1],
                "album": normalized[2],
            }
        )
    return variants


def find_fpcalc_executable(config: Mapping[str, object] | None = None) -> str | None:
    values = {} if config is None else dict(config)
    configured = str(values.get("FPCALC_BIN", "") or "").strip()
    if configured:
        resolved = shutil.which(configured)
        if resolved:
            return resolved
        candidate = Path(configured).expanduser()
        if candidate.exists() and candidate.is_file():
            return str(candidate)
    return shutil.which("fpcalc")


class AcoustIdService:
    def __init__(
        self,
        api_key: str = "",
        *,
        base_url: str = "https://api.acoustid.org",
        fpcalc_path: str = "",
        request_timeout: float = 20.0,
        score_threshold: float = 0.9,
        fingerprint_length: int = 120,
        transport: httpx.BaseTransport | None = None,
        command_runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
    ) -> None:
        self.api_key = str(api_key or "").strip()
        self.base_url = str(base_url or "https://api.acoustid.org").rstrip("/")
        self.fpcalc_path = str(fpcalc_path or find_fpcalc_executable() or "").strip()
        self.request_timeout = max(float(request_timeout or 20.0), 1.0)
        self.score_threshold = min(max(float(score_threshold or 0.9), 0.0), 1.0)
        self.fingerprint_length = max(int(fingerprint_length or 120), 15)
        self.transport = transport
        self.command_runner = command_runner or subprocess.run

    @classmethod
    def from_config(cls, config: Mapping[str, object]) -> AcoustIdService:
        return cls(
            api_key=str(config.get("ACOUSTID_API_KEY", "")),
            base_url=str(
                config.get("ACOUSTID_API_BASE_URL", "https://api.acoustid.org")
            ),
            fpcalc_path=str(config.get("FPCALC_BIN", "")),
            request_timeout=float(config.get("ACOUSTID_LOOKUP_TIMEOUT", 20.0) or 20.0),
            score_threshold=float(config.get("ACOUSTID_SCORE_THRESHOLD", 0.9) or 0.9),
            fingerprint_length=int(
                config.get("ACOUSTID_FINGERPRINT_LENGTH", 120) or 120
            ),
        )

    def is_configured(self) -> bool:
        return bool(self.api_key and self.fpcalc_path)

    def identify_track(
        self,
        audio_path: str | Path,
        *,
        musicbrainz_service: MusicBrainzService | None = None,
        max_candidates: int = 5,
    ) -> dict[str, Any]:
        if not self.is_configured():
            raise ValueError(
                "Configure ACOUSTID_API_KEY and ensure fpcalc is installed."
            )

        fingerprint = self.fingerprint_audio(audio_path)
        candidates = self.lookup_candidates(
            fingerprint["duration"],
            fingerprint["fingerprint"],
            max_candidates=max_candidates,
        )
        if not candidates:
            return {
                "duration": fingerprint["duration"],
                "candidates": [],
                "match": {},
                "accepted": False,
            }

        best = dict(candidates[0])
        details: dict[str, Any] = {}
        if best.get("recording_mbid"):
            service = musicbrainz_service or MusicBrainzService.from_config({})
            details = service.lookup_recording_details(
                title=str(best.get("title") or ""),
                artist_name=str(best.get("artist") or ""),
                album_name=str(best.get("album") or ""),
                recording_mbid=str(best.get("recording_mbid") or ""),
                release_mbid=str(best.get("release_mbid") or ""),
            )

        if not details:
            details = {
                "recording_mbid": str(best.get("recording_mbid") or ""),
                "release_mbid": str(best.get("release_mbid") or ""),
                "title": str(best.get("title") or "").strip(),
                "artist": str(best.get("artist") or "").strip(),
                "album": str(best.get("album") or "").strip(),
                "albumartist": str(best.get("artist") or "").strip(),
                "artist_mbid": str(best.get("artist_mbid") or "").strip(),
                "albumartist_mbid": str(best.get("artist_mbid") or "").strip(),
                "track_number": _coerce_track_number(best.get("track_number")),
            }

        details["acoustid_id"] = str(best.get("acoustid_id") or "")
        details["acoustid_score"] = float(best.get("score") or 0.0)
        details["fingerprint_duration"] = fingerprint["duration"]

        return {
            "duration": fingerprint["duration"],
            "candidates": candidates,
            "match": details,
            "accepted": float(best.get("score") or 0.0) >= self.score_threshold,
        }

    def fingerprint_audio(self, audio_path: str | Path) -> dict[str, Any]:
        path = Path(audio_path).expanduser().resolve()
        if not path.exists() or not path.is_file():
            raise ValueError(f"Audio file does not exist: {path}")
        if not self.fpcalc_path:
            raise ValueError("fpcalc is not installed or not configured.")

        command = [
            self.fpcalc_path,
            "-json",
            "-length",
            str(self.fingerprint_length),
            str(path),
        ]
        try:
            result = self.command_runner(
                command,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=max(self.request_timeout * 2, 30.0),
                check=False,
            )
        except OSError as exc:
            raise ValueError(f"Could not start fpcalc: {exc}") from exc
        except subprocess.TimeoutExpired as exc:
            raise ValueError(
                f"fpcalc timed out while fingerprinting {path.name}."
            ) from exc

        stdout = str(result.stdout or "").strip()
        stderr = str(result.stderr or "").strip()
        if result.returncode != 0:
            raise ValueError(
                stderr or stdout or f"fpcalc exited with code {result.returncode}."
            )

        try:
            payload = json.loads(stdout)
        except json.JSONDecodeError as exc:
            raise ValueError("fpcalc did not return valid JSON output.") from exc

        duration = float(payload.get("duration") or 0.0)
        fingerprint = str(payload.get("fingerprint") or "").strip()
        if duration <= 0 or not fingerprint:
            raise ValueError("fpcalc did not return a usable duration and fingerprint.")

        return {
            "duration": duration,
            "fingerprint": fingerprint,
        }

    def lookup_candidates(
        self,
        duration: float,
        fingerprint: str,
        *,
        max_candidates: int = 5,
    ) -> list[dict[str, Any]]:
        if not self.api_key:
            raise ValueError("ACOUSTID_API_KEY is not configured.")

        with httpx.Client(
            base_url=self.base_url,
            follow_redirects=True,
            timeout=self.request_timeout,
            transport=self.transport,
        ) as client:
            response = client.post(
                "/v2/lookup",
                data={
                    "client": self.api_key,
                    "duration": int(round(duration)),
                    "fingerprint": fingerprint,
                    "meta": _ACOUSTID_LOOKUP_META,
                },
            )
            response.raise_for_status()

        payload = response.json()
        if str(payload.get("status") or "").strip().lower() != "ok":
            message = str(
                payload.get("error", {}).get("message") or "AcoustID lookup failed."
            )
            raise ValueError(message)

        candidates: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        for result in payload.get("results", []):
            if not isinstance(result, dict):
                continue
            score = float(result.get("score") or 0.0)
            acoustid_id = str(result.get("id") or "").strip()
            recordings_value = result.get("recordings")
            recordings = recordings_value if isinstance(recordings_value, list) else []
            for recording in recordings:
                if not isinstance(recording, dict):
                    continue
                candidate = _candidate_from_recording(
                    recording,
                    acoustid_id=acoustid_id,
                    score=score,
                )
                dedupe_key = (candidate["recording_mbid"], candidate["release_mbid"])
                if dedupe_key in seen:
                    continue
                seen.add(dedupe_key)
                candidates.append(candidate)

        candidates.sort(key=lambda item: float(item.get("score") or 0.0), reverse=True)
        return candidates[: max(max_candidates, 1)]


def _candidate_from_recording(
    recording: dict[str, Any],
    *,
    acoustid_id: str,
    score: float,
) -> dict[str, Any]:
    artist_name, artist_mbid = _artist_credit(recording.get("artists"))
    releases = (
        recording.get("releases", [])
        if isinstance(recording.get("releases"), list)
        else []
    )
    release_title = ""
    release_mbid = ""
    track_number: int | None = None
    for release in releases:
        if not isinstance(release, dict):
            continue
        release_mbid = str(release.get("id") or "").strip()
        release_title = str(release.get("title") or "").strip()
        mediums = (
            release.get("mediums", [])
            if isinstance(release.get("mediums"), list)
            else []
        )
        for medium in mediums:
            if not isinstance(medium, dict):
                continue
            tracks = (
                medium.get("tracks", [])
                if isinstance(medium.get("tracks"), list)
                else []
            )
            for track in tracks:
                if not isinstance(track, dict):
                    continue
                if (
                    str(track.get("id") or "").strip()
                    == str(recording.get("id") or "").strip()
                ):
                    track_number = _coerce_track_number(
                        track.get("position") or track.get("number")
                    )
                    break
            if track_number is not None:
                break
        if release_mbid or release_title:
            break

    return {
        "acoustid_id": acoustid_id,
        "score": score,
        "recording_mbid": str(recording.get("id") or "").strip(),
        "release_mbid": release_mbid,
        "title": str(recording.get("title") or "").strip(),
        "artist": artist_name,
        "artist_mbid": artist_mbid,
        "album": release_title,
        "track_number": track_number,
    }


def _artist_credit(value: Any) -> tuple[str, str]:
    if not isinstance(value, list):
        return "", ""
    names: list[str] = []
    artist_mbid = ""
    for item in value:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        if name:
            names.append(name)
        if not artist_mbid:
            artist_mbid = str(item.get("id") or "").strip()
    return " ".join(names).strip(), artist_mbid


def _coerce_track_number(value: Any) -> int | None:
    try:
        parsed = int(str(value or "").strip())
    except ValueError:
        return None
    return parsed if parsed > 0 else None
