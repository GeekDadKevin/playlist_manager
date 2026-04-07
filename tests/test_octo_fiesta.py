from __future__ import annotations

import httpx
from app.models import PlaylistTrack
from app.services.octo_fiesta import OctoFiestaService


def test_octo_fiesta_sync_downloads_external_track() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/rest/search3":
            return httpx.Response(
                200,
                json={
                    "subsonic-response": {
                        "status": "ok",
                        "searchResult3": {
                            "song": [
                                {
                                    "id": "ext-deezer-song-12345",
                                    "title": "Teardrop",
                                    "artist": "Massive Attack",
                                    "album": "Mezzanine",
                                    "duration": 245,
                                }
                            ]
                        },
                    }
                },
            )

        if request.url.path == "/rest/stream":
            assert request.url.params["id"] == "ext-deezer-song-12345"
            return httpx.Response(200, headers={"content-type": "audio/flac"}, content=b"abc123")

        raise AssertionError(f"Unexpected path: {request.url.path}")

    service = OctoFiestaService(
        base_url="http://octo-fiesta:5274",
        mode="download",
        username="demo",
        password="demo",
        transport=httpx.MockTransport(handler),
    )

    result = service.sync_tracks([PlaylistTrack(title="Teardrop", artist="Massive Attack")])

    assert result["processing_mode"] == "sequential"
    assert result["summary"]["downloaded"] == 1
    assert result["results"][0]["index"] == 1
    assert result["results"][0]["download"]["external_id"] == "ext-deezer-song-12345"
    assert result["results"][0]["download"]["bytes_streamed"] == 6


def test_octo_fiesta_sync_skips_local_track() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "subsonic-response": {
                    "status": "ok",
                    "searchResult3": {
                        "song": [
                            {
                                "id": "local-song-1",
                                "title": "Teardrop",
                                "artist": "Massive Attack",
                                "album": "Mezzanine",
                                "duration": 245,
                            }
                        ]
                    },
                }
            },
        )

    service = OctoFiestaService(
        base_url="http://octo-fiesta:5274",
        mode="download",
        username="demo",
        password="demo",
        transport=httpx.MockTransport(handler),
    )

    result = service.sync_tracks([PlaylistTrack(title="Teardrop", artist="Massive Attack")])

    assert result["summary"]["already_available"] == 1
    assert result["results"][0]["status"] == "already_available"
