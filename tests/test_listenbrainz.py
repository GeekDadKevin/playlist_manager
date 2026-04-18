from __future__ import annotations

import httpx
from app.services.ingest import fetch_remote_jspf
from app.services.listenbrainz import ListenBrainzService


def test_listenbrainz_service_resolves_createdfor_playlist_url() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/1/user/demo/playlists/createdfor"
        return httpx.Response(
            200,
            json={
                "payload": {
                    "playlists": [
                        {
                            "identifier": (
                                "https://listenbrainz.org/playlist/"
                                "12345678-1234-1234-1234-123456789abc"
                            )
                        }
                    ]
                }
            },
        )

    service = ListenBrainzService(
        username="demo",
        transport=httpx.MockTransport(handler),
    )

    assert (
        service.resolve_playlist_url()
        == "https://listenbrainz.org/playlist/12345678-1234-1234-1234-123456789abc/export/jspf"
    )


def test_listenbrainz_service_prefers_configured_playlist_id() -> None:
    service = ListenBrainzService(
        username="demo",
        playlist_id="abcdefab-1234-5678-90ab-abcdefabcdef",
    )

    assert (
        service.resolve_playlist_url()
        == "https://listenbrainz.org/playlist/abcdefab-1234-5678-90ab-abcdefabcdef/export/jspf"
    )


def test_listenbrainz_service_lists_createdfor_and_user_playlists() -> None:
    responses = {
        "/1/user/demo/playlists/createdfor": {
            "payload": {
                "playlists": [
                    {
                        "title": "Created For You Mix",
                        "identifier": "https://listenbrainz.org/playlist/11111111-1111-1111-1111-111111111111",
                    }
                ]
            }
        },
        "/1/user/demo/playlists": {
            "payload": {
                "playlists": [
                    {
                        "title": "My Favorites",
                        "identifier": "https://listenbrainz.org/playlist/22222222-2222-2222-2222-222222222222",
                    }
                ]
            }
        },
    }

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=responses[request.url.path])

    service = ListenBrainzService(
        username="demo",
        transport=httpx.MockTransport(handler),
    )

    playlists = service.list_playlists()

    assert [item["title"] for item in playlists] == ["Created For You Mix", "My Favorites"]
    assert playlists[0]["source"] == "createdfor"
    assert playlists[1]["source"] == "user"
    assert playlists[0]["jspf_url"].endswith("/export/jspf")


def test_listenbrainz_service_hides_previously_imported_playlists() -> None:
    responses = {
        "/1/user/demo/playlists/createdfor": {
            "payload": {
                "playlists": [
                    {
                        "title": "Created For You Mix",
                        "identifier": "https://listenbrainz.org/playlist/11111111-1111-1111-1111-111111111111",
                    }
                ]
            }
        },
        "/1/user/demo/playlists": {
            "payload": {
                "playlists": [
                    {
                        "title": "My Favorites",
                        "identifier": "https://listenbrainz.org/playlist/22222222-2222-2222-2222-222222222222",
                    }
                ]
            }
        },
    }

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=responses[request.url.path])

    service = ListenBrainzService(
        username="demo",
        transport=httpx.MockTransport(handler),
    )

    playlists = service.list_playlists(
        exclude_playlist_ids={"11111111-1111-1111-1111-111111111111"}
    )

    assert [item["title"] for item in playlists] == ["My Favorites"]


def test_listenbrainz_service_fetches_playlist_json_for_selected_playlist() -> None:
    playlist_id = "12345678-1234-1234-1234-123456789abc"

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == f"/1/playlist/{playlist_id}"
        assert request.headers.get("Authorization") == "Token secret-token"
        return httpx.Response(
            200,
            json={
                "playlist": {
                    "title": "Weekly Exploration",
                    "track": [
                        {
                            "title": "Windowlicker",
                            "creator": "Aphex Twin",
                            "identifier": "https://listenbrainz.org/recording/1",
                        }
                    ],
                }
            },
        )

    service = ListenBrainzService(
        username="demo",
        auth_token="secret-token",
        transport=httpx.MockTransport(handler),
    )

    payload = service.fetch_jspf_document(playlist_id)

    assert payload["playlist"]["title"] == "Weekly Exploration"


def test_fetch_remote_jspf_uses_configured_service_when_url_blank(tmp_path) -> None:
    playlist_id = "12345678-1234-1234-1234-123456789abc"

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == f"/1/playlist/{playlist_id}"
        return httpx.Response(
            200,
            json={
                "playlist": {
                    "title": "Auto Mix",
                    "track": [
                        {
                            "title": "Teardrop",
                            "creator": "Massive Attack",
                            "identifier": "https://listenbrainz.org/recording/teardrop",
                        }
                    ],
                }
            },
        )

    service = ListenBrainzService(
        username="demo",
        playlist_id=playlist_id,
        transport=httpx.MockTransport(handler),
    )

    upload = fetch_remote_jspf(tmp_path, "", listenbrainz=service)

    assert upload.playlist_name == "Auto Mix"
    assert upload.remote_url.endswith(f"/1/playlist/{playlist_id}")
    assert upload.count == 1


def test_listenbrainz_lookup_recording_metadata_returns_mbids() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/1/metadata/lookup/"
        assert request.headers.get("Authorization") == "Token secret-token"
        assert request.url.params.get("artist_name") == "Massive Attack"
        assert request.url.params.get("recording_name") == "Teardrop"
        return httpx.Response(
            200,
            json={
                "payload": {
                    "recording_mbid": "abcd1234-0000-1111-2222-abcdefabcdef",
                    "release_mbid": "fedcba98-7654-3210-ffff-eeeeeeeeeeee",
                }
            },
        )

    service = ListenBrainzService(
        username="demo",
        auth_token="secret-token",
        transport=httpx.MockTransport(handler),
    )

    metadata = service.lookup_recording_metadata(
        artist_name="Massive Attack",
        recording_name="Teardrop",
        release_name="Mezzanine",
    )

    assert metadata["recording_mbid"] == "abcd1234-0000-1111-2222-abcdefabcdef"
    assert metadata["release_mbid"] == "fedcba98-7654-3210-ffff-eeeeeeeeeeee"
