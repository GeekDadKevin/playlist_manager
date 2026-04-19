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
        tracks[0].source
        == "https://musicbrainz.org/recording/59fc5ddf-a1d9-4746-902d-71fb5a9a78c2"
    )


def test_parse_uploaded_downloaded_jspf_with_utf8_bom() -> None:
    content = (
        "\ufeff{"
        '"playlist": {'
        '"title": "Downloaded Mix",'
        '"track": ['
        "{"
        '"title": "Windowlicker",'
        '"creator": "Aphex Twin",'
        '"album": "Windowlicker",'
        '"identifier": "https://musicbrainz.org/recording/demo"'
        "}"
        "]"
        "}"
        "}"
    )

    tracks = parse_uploaded_playlist("downloaded.jspf", content.encode("utf-8"))

    assert len(tracks) == 1
    assert tracks[0].artist == "Aphex Twin"
    assert tracks[0].title == "Windowlicker"


def test_parse_jspf_accepts_single_track_dict_payload() -> None:
    payload = {
        "playlist": {
            "track": {
                "title": "Only Shallow",
                "creator": "My Bloody Valentine",
                "album": "Loveless",
                "identifier": "https://musicbrainz.org/recording/only-shallow",
            }
        }
    }

    tracks = parse_jspf(payload)

    assert len(tracks) == 1
    assert tracks[0].artist == "My Bloody Valentine"
    assert tracks[0].title == "Only Shallow"
    assert tracks[0].album == "Loveless"


def test_parse_jspf_accepts_top_level_playlist_object_without_wrapper() -> None:
    payload = {
        "title": "LB Radio for tag punk on easy mode",
        "creator": "ListenBrainz Troi",
        "track": [
            {
                "title": "Dayo",
                "creator": "The Business",
                "album": "Suburban Rebels",
                "duration": 109000,
                "identifier": [
                    "https://musicbrainz.org/recording/5c3e2b29-7aec-48fe-92a7-fa76a2903273"
                ],
            }
        ],
    }

    tracks = parse_jspf(payload)

    assert len(tracks) == 1
    assert tracks[0].title == "Dayo"
    assert tracks[0].artist == "The Business"
    assert tracks[0].album == "Suburban Rebels"
    assert tracks[0].duration_seconds == 109


def test_parse_jspf_accepts_tracks_key_and_root_list() -> None:
    payload = {
        "title": "Alt playlist",
        "tracks": [
            {
                "name": "Generator",
                "artist": "Bad Religion",
                "album": "Generator",
                "identifier": "https://musicbrainz.org/recording/generator",
            }
        ],
    }

    tracks = parse_jspf(payload)

    assert len(tracks) == 1
    assert tracks[0].title == "Generator"
    assert tracks[0].artist == "Bad Religion"
    assert tracks[0].album == "Generator"

    list_payload = [
        {
            "title": "Hybrid Moments",
            "creator": "Misfits",
            "album": "Static Age",
            "identifier": "https://musicbrainz.org/recording/hybrid-moments",
        }
    ]

    list_tracks = parse_jspf(list_payload)

    assert len(list_tracks) == 1
    assert list_tracks[0].title == "Hybrid Moments"
    assert list_tracks[0].artist == "Misfits"


def test_parse_navidrome_missing_csv_extracts_metadata() -> None:
    csv_content = (
        "Theory of a Deadman/Scars & Souvenirs (Special Edition)/"
        "05 - Crutch (1).mp3\n"
        "Nine Inch Nails/Pretty Hate Machine/"
        "Nine Inch Nails - Pretty Hate Machine - Terrible Lie.ogg\n"
    )

    tracks = parse_uploaded_playlist(
        "navidrome_missing.csv", csv_content.encode("utf-8")
    )

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
