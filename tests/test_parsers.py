from __future__ import annotations

from app.parsers import parse_jspf, parse_m3u


def test_parse_m3u_extracts_artist_title_and_duration() -> None:
    content = """#EXTM3U
#EXTINF:245,Massive Attack - Teardrop
/music/massive-attack/teardrop.flac
"""

    tracks = parse_m3u(content)

    assert len(tracks) == 1
    assert tracks[0].artist == "Massive Attack"
    assert tracks[0].title == "Teardrop"
    assert tracks[0].duration_seconds == 245


def test_parse_jspf_extracts_core_fields() -> None:
    payload = {
        "playlist": {
            "track": [
                {
                    "title": "Windowlicker",
                    "creator": "Aphex Twin",
                    "album": "Windowlicker",
                    "duration": 366000,
                    "identifier": "https://listenbrainz.org/track/123",
                }
            ]
        }
    }

    tracks = parse_jspf(payload)

    assert len(tracks) == 1
    assert tracks[0].artist == "Aphex Twin"
    assert tracks[0].title == "Windowlicker"
    assert tracks[0].album == "Windowlicker"
    assert tracks[0].duration_seconds == 366
