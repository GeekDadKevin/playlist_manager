from __future__ import annotations

from app.matching.deezer import rank_candidates
from app.matching.normalize import build_search_queries, normalize_text
from app.models import PlaylistTrack


def test_normalize_text_handles_accents_and_feature_aliases() -> None:
    value = "Beyoncé feat. JAY-Z"

    assert normalize_text(value) == "beyonce feat jay z"


def test_build_search_queries_prefers_artist_and_title() -> None:
    track = PlaylistTrack(title="Teardrop", artist="Massive Attack", album="Mezzanine")

    assert build_search_queries(track)[0] == "massive attack teardrop"


def test_rank_candidates_prefers_best_match() -> None:
    track = PlaylistTrack(title="Teardrop", artist="Massive Attack", duration_seconds=245)
    candidates = [
        {"title": "Teardrop", "artist": "Massive Attack", "duration_seconds": 245},
        {"title": "Tear Drop", "artist": "Attack Massive", "duration_seconds": 250},
        {"title": "Completely Different", "artist": "Someone Else", "duration_seconds": 180},
    ]

    ranked = rank_candidates(track, candidates)

    assert ranked[0]["title"] == "Teardrop"
    assert ranked[0]["accepted"] is True
    assert ranked[-1]["accepted"] is False
