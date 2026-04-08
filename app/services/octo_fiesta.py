from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from typing import Any

import httpx

from app.matching import build_search_queries, rank_candidates
from app.models import PlaylistTrack


class OctoFiestaService:
    def __init__(
        self,
        base_url: str = "",
        mode: str = "preview",
        username: str = "",
        password: str = "",
        token: str = "",
        salt: str = "",
        client_name: str = "jspf-converter",
        api_version: str = "1.16.1",
        provider: str = "deezer",
        match_threshold: float = 72.0,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.mode = mode
        self.username = username
        self.password = password
        self.token = token
        self.salt = salt
        self.client_name = client_name
        self.api_version = api_version
        self.provider = provider.lower()
        self.match_threshold = match_threshold
        self.transport = transport

    @classmethod
    def from_config(cls, config: Mapping[str, Any]) -> OctoFiestaService:
        return cls(
            base_url=str(config.get("OCTO_FIESTA_BASE_URL", "")),
            mode=str(config.get("OCTO_FIESTA_HANDOFF_MODE", "preview")),
            username=str(config.get("OCTO_FIESTA_USERNAME", "")),
            password=str(config.get("OCTO_FIESTA_PASSWORD", "")),
            token=str(config.get("OCTO_FIESTA_TOKEN", "")),
            salt=str(config.get("OCTO_FIESTA_SALT", "")),
            client_name=str(config.get("OCTO_FIESTA_CLIENT_NAME", "jspf-converter")),
            api_version=str(config.get("OCTO_FIESTA_API_VERSION", "1.16.1")),
            provider=str(config.get("OCTO_FIESTA_PROVIDER", "deezer")),
            match_threshold=float(config.get("OCTO_FIESTA_MATCH_THRESHOLD", 72.0)),
        )

    def is_configured(self) -> bool:
        has_auth = bool(self.username and (self.password or (self.token and self.salt)))
        return bool(self.base_url and has_auth)

    def build_handoff_payload(
        self,
        track: PlaylistTrack,
        deezer_match: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        match_id = str((deezer_match or {}).get("id", ""))
        return {
            "mode": self.mode,
            "provider": self.provider,
            "track": track.to_dict(),
            "deezer_match": deezer_match or {},
            "octo_fiesta_base_url": self.base_url,
            "search_endpoint": f"{self.base_url}/rest/search3" if self.base_url else "",
            "stream_endpoint": f"{self.base_url}/rest/stream" if self.base_url else "",
            "downloadable_external_id": match_id if match_id.startswith("ext-") else "",
            "configured": self.is_configured(),
            "note": (
                "Octo-Fiesta integration uses the documented Subsonic proxy endpoints: "
                "`/rest/search3` to find local/external matches and `/rest/stream` to trigger "
                "downloads for `ext-*` song IDs."
            ),
        }

    def search_track(self, track: PlaylistTrack, limit: int = 10) -> list[dict[str, Any]]:
        self._validate_configuration()

        best_ranked: list[dict[str, Any]] = []
        for query in build_search_queries(track):
            payload = self._request_json(
                "/rest/search3",
                {
                    "query": query,
                    "artistCount": 0,
                    "albumCount": 0,
                    "songCount": limit,
                },
            )
            candidates = self._extract_song_candidates(payload)
            ranked = rank_candidates(track, candidates, threshold=self.match_threshold)
            if ranked and (not best_ranked or ranked[0]["score"] > best_ranked[0]["score"]):
                best_ranked = ranked
            if ranked and ranked[0]["accepted"]:
                break

        return best_ranked

    def sync_tracks(
        self,
        tracks: list[PlaylistTrack],
        max_tracks: int | None = None,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        self._validate_configuration()

        started_at = _utc_timestamp()
        selected_tracks = tracks[:max_tracks] if max_tracks else tracks
        results: list[dict[str, Any]] = []
        summary = {
            "requested": len(selected_tracks),
            "processed": 0,
            "preview": 0,
            "downloaded": 0,
            "already_available": 0,
            "low_confidence": 0,
            "not_found": 0,
            "failed": 0,
        }

        for index, track in enumerate(selected_tracks, start=1):
            result = self._sync_single_track(track)
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
                        "mode": self.mode,
                        "provider": self.provider,
                        "threshold": self.match_threshold,
                        "processing_mode": "sequential",
                        "started_at": started_at,
                        "completed_at": "",
                        "summary": dict(summary),
                        "results": list(results),
                    }
                )

        final_result = {
            "mode": self.mode,
            "provider": self.provider,
            "threshold": self.match_threshold,
            "processing_mode": "sequential",
            "started_at": started_at,
            "completed_at": _utc_timestamp(),
            "summary": summary,
            "results": results,
        }
        if progress_callback is not None:
            progress_callback(final_result)

        return final_result

    def submit(self, payload: dict[str, Any]) -> dict[str, Any]:
        match = payload.get("deezer_match") or {}
        external_id = str(match.get("id", ""))

        if self.mode == "preview":
            return {"status": "preview", "payload": payload}
        if not external_id.startswith("ext-"):
            return {"status": "skipped", "reason": "No external song ID was provided."}

        return self.trigger_download(external_id)

    def trigger_download(self, external_id: str) -> dict[str, Any]:
        self._validate_configuration()

        if not external_id.startswith("ext-"):
            raise ValueError("Octo-Fiesta downloads require an external `ext-*` song ID.")

        with self._client(timeout=None) as client:
            with client.stream(
                "GET",
                "/rest/stream",
                params=self._auth_params({"id": external_id}, include_format=False),
            ) as response:
                response.raise_for_status()

                content_type = response.headers.get("content-type", "")
                if "audio" not in content_type and "octet-stream" not in content_type:
                    body = response.read().decode("utf-8", errors="ignore")
                    raise RuntimeError(f"Unexpected Octo-Fiesta stream response: {body[:300]}")

                bytes_streamed = 0
                for chunk in response.iter_bytes():
                    bytes_streamed += len(chunk)

        return {
            "status": "downloaded",
            "external_id": external_id,
            "bytes_streamed": bytes_streamed,
            "stream_endpoint": f"{self.base_url}/rest/stream",
            "completed_at": _utc_timestamp(),
        }

    def _sync_single_track(self, track: PlaylistTrack) -> dict[str, Any]:
        result = {
            "track": track.to_dict(),
            "queries": build_search_queries(track),
            "status": "not_found",
            "message": "No Octo-Fiesta match found.",
            "match": {},
        }

        ranked = self.search_track(track)
        if not ranked:
            return result

        top_match = ranked[0]
        result["match"] = top_match

        if not top_match.get("accepted", False):
            result["status"] = "low_confidence"
            result["message"] = "Best match was below the configured confidence threshold."
            return result

        if top_match.get("source_kind") == "local":
            result["status"] = "already_available"
            result["message"] = "Track is already available through Navidrome/Octo-Fiesta."
            return result

        if self.mode == "preview":
            result["status"] = "preview"
            result["message"] = "Preview mode is enabled, so no Octo-Fiesta download was triggered."
            return result

        try:
            download_result = self.trigger_download(str(top_match.get("id", "")))
        except Exception as exc:  # pragma: no cover - network/runtime dependent
            result["status"] = "failed"
            result["message"] = str(exc)
            return result

        resolved_match = self._find_local_match(track)
        if resolved_match is not None:
            result["resolved_match"] = resolved_match

        result["status"] = "downloaded"
        result["message"] = "Download triggered successfully through `/rest/stream`."
        result["download"] = download_result
        return result

    def _request_json(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        with self._client(timeout=20.0) as client:
            response = client.get(path, params=self._auth_params(params))
            response.raise_for_status()

        payload = response.json().get("subsonic-response", {})
        if payload.get("status") == "failed":
            error = payload.get("error", {})
            raise RuntimeError(str(error.get("message", "Octo-Fiesta request failed.")))

        return payload

    def _extract_song_candidates(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        search_result = payload.get("searchResult3", {})
        raw_songs = search_result.get("song") or []
        songs = raw_songs if isinstance(raw_songs, list) else [raw_songs]

        candidates: list[dict[str, Any]] = []
        for item in songs:
            item_id = str(item.get("id", ""))
            provider = self._provider_from_id(item_id)
            source_kind = "external" if item_id.startswith("ext-") else "local"

            if source_kind == "external" and provider and provider != self.provider:
                continue

            candidates.append(
                {
                    "id": item_id,
                    "title": item.get("title", ""),
                    "artist": item.get("artist", ""),
                    "album": item.get("album", ""),
                    "duration_seconds": item.get("duration"),
                    "path": item.get("path", ""),
                    "source_kind": source_kind,
                    "provider": provider or ("navidrome" if source_kind == "local" else "external"),
                }
            )

        return candidates

    def _auth_params(
        self,
        extra: dict[str, Any] | None = None,
        include_format: bool = True,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "u": self.username,
            "v": self.api_version,
            "c": self.client_name,
        }
        if include_format:
            params["f"] = "json"
        if self.password:
            params["p"] = self.password
        elif self.token and self.salt:
            params["t"] = self.token
            params["s"] = self.salt

        if extra:
            params.update(extra)
        return params

    def _client(self, timeout: float | None) -> httpx.Client:
        return httpx.Client(
            base_url=self.base_url,
            follow_redirects=True,
            timeout=timeout,
            transport=self.transport,
        )

    def _validate_configuration(self) -> None:
        if not self.is_configured():
            raise ValueError(
                "Set `OCTO_FIESTA_BASE_URL`, `OCTO_FIESTA_USERNAME`, and either "
                "`OCTO_FIESTA_PASSWORD` or `OCTO_FIESTA_TOKEN` + `OCTO_FIESTA_SALT`."
            )

    def _find_local_match(self, track: PlaylistTrack) -> dict[str, Any] | None:
        try:
            ranked = self.search_track(track)
        except Exception:  # pragma: no cover - best-effort enrichment only
            return None

        for candidate in ranked:
            if candidate.get("source_kind") == "local" and candidate.get("path"):
                return candidate
        return None

    @staticmethod
    def _provider_from_id(item_id: str) -> str:
        if not item_id.startswith("ext-"):
            return ""

        parts = item_id.split("-")
        return parts[1] if len(parts) >= 4 else ""


def _utc_timestamp() -> str:
    return datetime.now(UTC).isoformat()
