from __future__ import annotations

from pathlib import Path

import httpx
from app.models import PlaylistTrack
from app.services.deezer_download import DeezerDownloadService


def test_deezer_sync_downloads_track(tmp_path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "www.deezer.com" and request.url.path == "/ajax/gw-light.php":
            method = request.url.params.get("method")
            if method == "deezer.getUserData":
                return httpx.Response(
                    200,
                    json={
                        "results": {
                            "checkForm": "api-token",
                            "USER": {"OPTIONS": {"license_token": "license-token"}},
                        }
                    },
                    headers={"set-cookie": "sid=session-cookie; Path=/; HttpOnly"},
                )
            if method == "song.getData":
                return httpx.Response(200, json={"results": {"TRACK_TOKEN": "track-token"}})

        if request.url.host == "api.deezer.com" and request.url.path == "/search":
            return httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "id": 12345,
                            "title": "Teardrop",
                            "artist": {"name": "Massive Attack"},
                            "album": {"title": "Mezzanine"},
                            "duration": 245,
                        }
                    ]
                },
            )

        if request.url.host == "media.deezer.com" and request.url.path == "/v1/get_url":
            return httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "media": [
                                {
                                    "format": "FLAC",
                                    "sources": [{"url": "https://cdn.example.test/track.flac"}],
                                }
                            ]
                        }
                    ]
                },
            )

        if request.url.host == "cdn.example.test":
            return httpx.Response(200, content=(b"0" * 2048) + b"abc")

        raise AssertionError(f"Unexpected request: {request.method} {request.url}")

    service = DeezerDownloadService(
        arl="demo-cookie",
        download_dir=str(tmp_path),
        quality="FLAC",
        transport=httpx.MockTransport(handler),
    )

    result = service.sync_tracks([PlaylistTrack(title="Teardrop", artist="Massive Attack")])

    assert result["processing_mode"] == "sequential"
    assert result["summary"]["downloaded"] == 1
    assert result["results"][0]["index"] == 1

    output_path = Path(result["results"][0]["download"]["path"])
    assert output_path.exists()
    assert output_path.name == "Teardrop.flac"

    metadata_path = output_path.with_suffix(".xml")
    assert metadata_path.exists()

    metadata_xml = metadata_path.read_text(encoding="utf-8")
    assert "<song>" in metadata_xml
    assert "<title>Teardrop</title>" in metadata_xml
    assert "<performingartist>Massive Attack</performingartist>" in metadata_xml
    assert "<albumtitle>Mezzanine</albumtitle>" in metadata_xml
    assert "<deezerid>12345</deezerid>" in metadata_xml


def test_deezer_search_track_returns_ranked_match() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "api.deezer.com" and request.url.path == "/search":
            return httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "id": 9876,
                            "title": "Teardrop",
                            "artist": {"name": "Massive Attack"},
                            "album": {"title": "Mezzanine"},
                            "duration": 245,
                        }
                    ]
                },
            )

        raise AssertionError(f"Unexpected request: {request.method} {request.url}")

    service = DeezerDownloadService(
        arl="demo-cookie",
        download_dir="/tmp/downloads",
        transport=httpx.MockTransport(handler),
    )

    ranked = service.search_track(PlaylistTrack(title="Teardrop", artist="Massive Attack"))

    assert ranked[0]["title"] == "Teardrop"
    assert ranked[0]["accepted"] is True
