from __future__ import annotations

import httpx
from app.services.musicbrainz import MusicBrainzService


def test_get_json_retries_transient_503(monkeypatch) -> None:
    calls = {"count": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["count"] += 1
        if calls["count"] < 3:
            return httpx.Response(503, json={"error": "busy"})
        return httpx.Response(200, json={"status": "ok", "title": "done"})

    monkeypatch.setattr("app.services.musicbrainz.time.sleep", lambda _seconds: None)
    service = MusicBrainzService(transport=httpx.MockTransport(handler))

    payload = service._get_json("/ws/2/release/demo", {"inc": "recordings+media"})

    assert calls["count"] == 3
    assert payload["title"] == "done"


def test_lookup_recording_details_returns_richer_musicbrainz_fields(monkeypatch) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/ws/2/recording/recording-123":
            return httpx.Response(
                200,
                json={
                    "id": "recording-123",
                    "title": "Teardrop",
                    "disambiguation": "album version",
                    "artist-credit": [
                        {
                            "name": "Massive Attack",
                            "artist": {
                                "id": "artist-1",
                                "name": "Massive Attack",
                                "sort-name": "Massive Attack",
                            },
                        }
                    ],
                    "isrcs": ["GBBKS9801234"],
                    "genres": [{"name": "Trip Hop"}],
                },
            )

        if request.url.path == "/ws/2/release/release-456":
            return httpx.Response(
                200,
                json={
                    "id": "release-456",
                    "title": "Mezzanine",
                    "date": "1998-04-20",
                    "country": "GB",
                    "status": "Official",
                    "barcode": "724384559524",
                    "disambiguation": "2019 remaster",
                    "text-representation": {"language": "eng", "script": "Latn"},
                    "artist-credit": [
                        {
                            "name": "Massive Attack",
                            "artist": {
                                "id": "artist-1",
                                "name": "Massive Attack",
                                "sort-name": "Massive Attack",
                            },
                        }
                    ],
                    "label-info": [
                        {
                            "catalog-number": "WBRCD2",
                            "label": {"name": "Virgin"},
                        }
                    ],
                    "genres": [{"name": "Electronic"}],
                    "release-group": {
                        "id": "group-789",
                        "primary-type": "Album",
                        "secondary-types": ["Compilation"],
                        "first-release-date": "1998-04-20",
                    },
                    "media": [
                        {
                            "position": "1",
                            "format": "CD",
                            "track-count": 11,
                            "tracks": [
                                {
                                    "number": "10",
                                    "recording": {"id": "recording-123"},
                                }
                            ],
                        }
                    ],
                },
            )

        raise AssertionError(f"Unexpected request: {request.url}")

    service = MusicBrainzService(transport=httpx.MockTransport(handler))

    details = service.lookup_recording_details(
        title="Teardrop",
        artist_name="Massive Attack",
        album_name="Mezzanine",
        recording_mbid="recording-123",
        release_mbid="release-456",
    )

    assert details["recording_mbid"] == "recording-123"
    assert details["release_mbid"] == "release-456"
    assert details["release_group_mbid"] == "group-789"
    assert details["artist_sort"] == "Massive Attack"
    assert details["albumartist_sort"] == "Massive Attack"
    assert details["track_number"] == 10
    assert details["track_total"] == 11
    assert details["disc_number"] == 1
    assert details["disc_total"] == 1
    assert details["date"] == "1998-04-20"
    assert details["original_date"] == "1998-04-20"
    assert details["genre"] == "Trip Hop; Electronic"
    assert details["isrc"] == "GBBKS9801234"
    assert details["barcode"] == "724384559524"
    assert details["label"] == "Virgin"
    assert details["catalog_number"] == "WBRCD2"
    assert details["media_format"] == "CD"
    assert details["release_country"] == "GB"
    assert details["release_status"] == "Official"
    assert details["release_type"] == "Album"
    assert details["release_secondary_types"] == "Compilation"
    assert details["language"] == "eng"
    assert details["script"] == "Latn"
    assert details["recording_disambiguation"] == "album version"
    assert details["album_disambiguation"] == "2019 remaster"


def test_lookup_recording_details_falls_back_when_release_query_is_wrong() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/ws/2/recording":
            query = request.url.params.get("query", "")
            if 'release:"Wrong Album"' in query:
                return httpx.Response(200, json={"recordings": []})
            if 'artist:"Massive Attack"' in query:
                return httpx.Response(
                    200,
                    json={
                        "recordings": [
                            {
                                "id": "recording-123",
                                "title": "Teardrop",
                                "score": "100",
                                "artist-credit": [
                                    {
                                        "name": "Massive Attack",
                                        "artist": {
                                            "id": "artist-1",
                                            "name": "Massive Attack",
                                            "sort-name": "Massive Attack",
                                        },
                                    }
                                ],
                                "releases": [{"id": "release-456", "title": "Mezzanine"}],
                            }
                        ]
                    },
                )
        if request.url.path == "/ws/2/recording/recording-123":
            return httpx.Response(
                200,
                json={
                    "id": "recording-123",
                    "title": "Teardrop",
                    "artist-credit": [
                        {
                            "name": "Massive Attack",
                            "artist": {
                                "id": "artist-1",
                                "name": "Massive Attack",
                                "sort-name": "Massive Attack",
                            },
                        }
                    ],
                    "releases": [{"id": "release-456", "title": "Mezzanine"}],
                },
            )
        if request.url.path == "/ws/2/release/release-456":
            return httpx.Response(
                200,
                json={
                    "id": "release-456",
                    "title": "Mezzanine",
                    "artist-credit": [
                        {
                            "name": "Massive Attack",
                            "artist": {
                                "id": "artist-1",
                                "name": "Massive Attack",
                                "sort-name": "Massive Attack",
                            },
                        }
                    ],
                    "release-group": {"id": "group-789"},
                    "media": [],
                },
            )
        raise AssertionError(f"Unexpected request: {request.url}")

    service = MusicBrainzService(transport=httpx.MockTransport(handler))

    details = service.lookup_recording_details(
        title="Teardrop",
        artist_name="Massive Attack",
        album_name="Wrong Album",
    )

    assert details["recording_mbid"] == "recording-123"
    assert details["release_mbid"] == "release-456"
    assert details["album"] == "Mezzanine"
