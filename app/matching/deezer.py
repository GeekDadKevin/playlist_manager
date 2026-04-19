from __future__ import annotations

from typing import Any

import httpx
from rapidfuzz import fuzz

from app.matching.normalize import build_search_queries, normalize_text
from app.models import PlaylistTrack


def rank_candidates(
    track: PlaylistTrack,
    candidates: list[dict[str, Any]],
    threshold: float = 72.0,
) -> list[dict[str, Any]]:
    ranked: list[dict[str, Any]] = []

    for candidate in candidates:
        artist_score = _fuzzy_score(track.artist, candidate.get("artist", ""))
        title_score = _fuzzy_score(track.title, candidate.get("title", ""))
        album_score = (
            _fuzzy_score(track.album, candidate.get("album", ""))
            if track.album
            else 0.0
        )
        duration_score = _duration_score(
            track.duration_seconds, candidate.get("duration_seconds")
        )

        total = round(
            (title_score * 0.5)
            + (artist_score * 0.3)
            + (album_score * 0.1)
            + (duration_score * 0.1),
            2,
        )
        ranked.append(
            {
                **candidate,
                "score": total,
                "accepted": total >= threshold,
                "queries": build_search_queries(track),
            }
        )

    return sorted(ranked, key=lambda item: item["score"], reverse=True)


class DeezerSearchService:
    base_url = "https://api.deezer.com/search"

    def search(self, track: PlaylistTrack, limit: int = 5) -> list[dict[str, Any]]:
        queries = build_search_queries(track)
        if not queries:
            return []

        with httpx.Client(timeout=10.0) as client:
            response = client.get(
                self.base_url, params={"q": queries[0], "limit": limit}
            )
            response.raise_for_status()

        payload = response.json()
        candidates = [
            {
                "title": item.get("title", ""),
                "artist": item.get("artist", {}).get("name", ""),
                "album": item.get("album", {}).get("title", ""),
                "duration_seconds": item.get("duration"),
                "deezer_id": item.get("id"),
                "link": item.get("link", ""),
            }
            for item in payload.get("data", [])
        ]
        return rank_candidates(track, candidates)


def _fuzzy_score(left: str, right: str) -> float:
    if not left or not right:
        return 0.0
    return float(fuzz.token_set_ratio(normalize_text(left), normalize_text(right)))


def _duration_score(expected: int | None, actual: Any) -> float:
    if expected in (None, 0) or actual in (None, 0, ""):
        return 70.0

    try:
        delta = abs(int(expected) - int(actual))
    except (TypeError, ValueError):
        return 0.0

    return max(0.0, 100.0 - (delta * 2.5))
