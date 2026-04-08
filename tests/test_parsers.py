from __future__ import annotations

from app.parsers import parse_jspf, parse_m3u
from app.services.ingest import parse_uploaded_playlist
from app.services.listenbrainz import normalize_listenbrainz_url


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


def test_parse_jspf_accepts_identifier_lists_from_listenbrainz() -> None:
    payload = {
        "playlist": {
            "track": [
                {
                    "title": "Shake It Off",
                    "creator": "Taylor Swift",
                    "album": "1989",
                    "identifier": [
                        "https://musicbrainz.org/recording/59fc5ddf-a1d9-4746-902d-71fb5a9a78c2"
                    ],
                }
            ]
        }
    }

    tracks = parse_jspf(payload)

    assert len(tracks) == 1
    assert tracks[0].title == "Shake It Off"
    assert tracks[0].artist == "Taylor Swift"
    assert (
        tracks[0].source == "https://musicbrainz.org/recording/59fc5ddf-a1d9-4746-902d-71fb5a9a78c2"
    )


def test_parse_navidrome_missing_csv_extracts_metadata() -> None:
    csv_content = (
        "Theory of a Deadman/Scars & Souvenirs (Special Edition)/"
        "05 - Crutch (1).mp3\n"
        "Nine Inch Nails/Pretty Hate Machine/"
        "Nine Inch Nails - Pretty Hate Machine - Terrible Lie.ogg\n"
    )

    tracks = parse_uploaded_playlist("navidrome_missing.csv", csv_content.encode("utf-8"))

    assert len(tracks) == 2
    assert tracks[0].artist == "Theory of a Deadman"
    assert tracks[0].album == "Scars & Souvenirs (Special Edition)"
    assert tracks[0].title == "Crutch"
    assert tracks[1].artist == "Nine Inch Nails"
    assert tracks[1].album == "Pretty Hate Machine"
    assert tracks[1].title == "Terrible Lie"


def test_normalize_listenbrainz_url_accepts_playlist_page_url() -> None:
    url = "https://listenbrainz.org/playlist/12345678-1234-1234-1234-123456789abc/"

    assert normalize_listenbrainz_url(url) == f"{url}export/jspf"
