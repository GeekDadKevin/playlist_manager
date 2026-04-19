from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import httpx
from app.models import PlaylistTrack
from app.services.deezer_download import DeezerDownloadService
from app.services.listenbrainz import ListenBrainzService
from app.services.musicbrainz import MusicBrainzService


def test_deezer_sync_downloads_track(tmp_path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if (
            request.url.host == "www.deezer.com"
            and request.url.path == "/ajax/gw-light.php"
        ):
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
                return httpx.Response(
                    200, json={"results": {"TRACK_TOKEN": "track-token"}}
                )

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
                                    "sources": [
                                        {"url": "https://cdn.example.test/track.flac"}
                                    ],
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

    result = service.sync_tracks(
        [PlaylistTrack(title="Teardrop", artist="Massive Attack")]
    )

    assert result["processing_mode"] == "sequential"
    assert result["summary"]["downloaded"] == 1
    assert result["results"][0]["index"] == 1

    output_path = Path(result["results"][0]["download"]["path"])
    assert output_path.exists()
    assert output_path.name.endswith("Teardrop.flac")
    assert result["results"][0]["download"]["extension"] == ".flac"

    metadata_path = output_path.with_suffix(".xml")
    assert metadata_path.exists()

    metadata_xml = metadata_path.read_text(encoding="utf-8")
    assert "<song>" in metadata_xml
    assert "<title>Teardrop</title>" in metadata_xml
    assert "<performingartist>Massive Attack</performingartist>" in metadata_xml
    assert "<albumtitle>Mezzanine</albumtitle>" in metadata_xml
    assert "<deezerid>12345</deezerid>" in metadata_xml
    assert "<audioextension>.flac</audioextension>" in metadata_xml


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

    ranked = service.search_track(
        PlaylistTrack(title="Teardrop", artist="Massive Attack")
    )

    assert ranked[0]["title"] == "Teardrop"
    assert ranked[0]["accepted"] is True


def test_deezer_sync_keeps_soundcloud_for_manual_review_only(tmp_path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if (
            request.url.host == "www.deezer.com"
            and request.url.path == "/ajax/gw-light.php"
        ):
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
                )
        if request.url.host == "api.deezer.com" and request.url.path == "/search":
            return httpx.Response(200, json={"data": []})

        raise AssertionError(f"Unexpected request: {request.method} {request.url}")

    class StubSoundCloud:
        def __init__(self) -> None:
            self.search_calls = 0

        def is_configured(self) -> bool:
            return True

        def search_track(
            self, track: PlaylistTrack, limit: int = 10
        ) -> list[dict[str, object]]:
            self.search_calls += 1
            return [
                {
                    "id": "soundcloud:123",
                    "provider": "soundcloud",
                    "provider_label": "SoundCloud",
                    "soundcloud_id": "123",
                    "title": track.title,
                    "artist": track.artist,
                    "album": track.album,
                    "duration_seconds": 245,
                    "link": "https://soundcloud.com/demo/teardrop",
                    "score": 91.0,
                    "accepted": True,
                }
            ]

    soundcloud = StubSoundCloud()
    service = DeezerDownloadService(
        arl="demo-cookie",
        download_dir=str(tmp_path),
        transport=httpx.MockTransport(handler),
        soundcloud_service=cast(Any, soundcloud),
    )

    result = service.sync_tracks(
        [PlaylistTrack(title="Teardrop", artist="Massive Attack")]
    )

    assert result["summary"]["downloaded"] == 0
    assert result["summary"]["not_found"] == 1
    assert soundcloud.search_calls == 0


def test_deezer_review_candidates_can_include_soundcloud_when_requested() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "api.deezer.com" and request.url.path == "/search":
            return httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "id": 321,
                            "title": "Teardrop (Live Cover)",
                            "artist": {"name": "Someone Else"},
                            "album": {"title": "Loose Covers"},
                            "duration": 245,
                        }
                    ]
                },
            )

        raise AssertionError(f"Unexpected request: {request.method} {request.url}")

    class StubSoundCloud:
        def is_configured(self) -> bool:
            return True

        def search_track(
            self, track: PlaylistTrack, limit: int = 10
        ) -> list[dict[str, object]]:
            return [
                {
                    "id": "soundcloud:123",
                    "provider": "soundcloud",
                    "provider_label": "SoundCloud",
                    "soundcloud_id": "123",
                    "title": track.title,
                    "artist": track.artist,
                    "album": "SoundCloud",
                    "duration_seconds": 245,
                    "link": "https://soundcloud.com/demo/teardrop",
                    "score": 91.0,
                    "accepted": True,
                }
            ]

    service = DeezerDownloadService(
        arl="demo-cookie",
        download_dir="/tmp/downloads",
        transport=httpx.MockTransport(handler),
        soundcloud_service=cast(Any, StubSoundCloud()),
    )

    ranked = service.search_track(
        PlaylistTrack(title="Teardrop", artist="Massive Attack"),
        include_soundcloud=True,
    )

    assert ranked[0]["provider"] == "soundcloud"
    assert {candidate["provider"] for candidate in ranked} == {"deezer", "soundcloud"}


def test_deezer_sync_fetches_musicbrainz_recording_id_from_listenbrainz(
    tmp_path,
    monkeypatch,
) -> None:
    def deezer_handler(request: httpx.Request) -> httpx.Response:
        if (
            request.url.host == "www.deezer.com"
            and request.url.path == "/ajax/gw-light.php"
        ):
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
                return httpx.Response(
                    200, json={"results": {"TRACK_TOKEN": "track-token"}}
                )

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
                                    "sources": [
                                        {"url": "https://cdn.example.test/track.flac"}
                                    ],
                                }
                            ]
                        }
                    ]
                },
            )

        if request.url.host == "cdn.example.test":
            return httpx.Response(200, content=(b"0" * 2048) + b"abc")

        raise AssertionError(
            f"Unexpected Deezer request: {request.method} {request.url}"
        )

    def listenbrainz_handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/1/metadata/lookup/"
        return httpx.Response(
            200,
            json={
                "payload": {
                    "recording_mbid": "abcd1234-0000-1111-2222-abcdefabcdef",
                    "release_mbid": "fedcba98-7654-3210-ffff-eeeeeeeeeeee",
                }
            },
        )

    service = DeezerDownloadService(
        arl="demo-cookie",
        download_dir=str(tmp_path),
        quality="FLAC",
        transport=httpx.MockTransport(deezer_handler),
        listenbrainz_service=ListenBrainzService(
            username="demo",
            auth_token="secret-token",
            transport=httpx.MockTransport(listenbrainz_handler),
        ),
        musicbrainz_service=None,
    )

    monkeypatch.setattr(
        "app.services.deezer_download.ensure_cover_art",
        lambda *args, **kwargs: Path(args[0]) / "cover.jpg",
    )

    result = service.sync_tracks(
        [PlaylistTrack(title="Teardrop", artist="Massive Attack", track_number=1)]
    )

    metadata_path = Path(result["results"][0]["download"]["path"]).with_suffix(".xml")
    metadata_xml = metadata_path.read_text(encoding="utf-8")

    assert (
        "<musicbrainztrackid>abcd1234-0000-1111-2222-abcdefabcdef</musicbrainztrackid>"
        in metadata_xml
    )


def test_deezer_sync_fetches_musicbrainz_recording_id_from_musicbrainz(
    tmp_path,
    monkeypatch,
) -> None:
    def deezer_handler(request: httpx.Request) -> httpx.Response:
        if (
            request.url.host == "www.deezer.com"
            and request.url.path == "/ajax/gw-light.php"
        ):
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
                return httpx.Response(
                    200, json={"results": {"TRACK_TOKEN": "track-token"}}
                )

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
                                    "sources": [
                                        {"url": "https://cdn.example.test/track.flac"}
                                    ],
                                }
                            ]
                        }
                    ]
                },
            )

        if request.url.host == "cdn.example.test":
            return httpx.Response(200, content=(b"0" * 2048) + b"abc")

        raise AssertionError(
            f"Unexpected Deezer request: {request.method} {request.url}"
        )

    def musicbrainz_handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/ws/2/recording"
        return httpx.Response(
            200,
            json={
                "recordings": [
                    {
                        "id": "abcd1234-0000-1111-2222-abcdefabcdef",
                        "title": "Teardrop",
                        "score": "100",
                        "artist-credit": [{"name": "Massive Attack"}],
                        "releases": [{"title": "Mezzanine"}],
                    }
                ]
            },
        )

    service = DeezerDownloadService(
        arl="demo-cookie",
        download_dir=str(tmp_path),
        quality="FLAC",
        transport=httpx.MockTransport(deezer_handler),
        musicbrainz_service=MusicBrainzService(
            transport=httpx.MockTransport(musicbrainz_handler)
        ),
    )

    monkeypatch.setattr(
        "app.services.deezer_download.ensure_cover_art",
        lambda *args, **kwargs: Path(args[0]) / "cover.jpg",
    )

    result = service.sync_tracks(
        [PlaylistTrack(title="Teardrop", artist="Massive Attack", track_number=1)]
    )

    metadata_path = Path(result["results"][0]["download"]["path"]).with_suffix(".xml")
    metadata_xml = metadata_path.read_text(encoding="utf-8")

    assert (
        "<musicbrainztrackid>abcd1234-0000-1111-2222-abcdefabcdef</musicbrainztrackid>"
        in metadata_xml
    )


def test_deezer_sync_logs_when_musicbrainz_recording_id_cannot_be_resolved(
    tmp_path,
    caplog,
    monkeypatch,
) -> None:
    def deezer_handler(request: httpx.Request) -> httpx.Response:
        if (
            request.url.host == "www.deezer.com"
            and request.url.path == "/ajax/gw-light.php"
        ):
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
                return httpx.Response(
                    200, json={"results": {"TRACK_TOKEN": "track-token"}}
                )

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
                                    "sources": [
                                        {"url": "https://cdn.example.test/track.flac"}
                                    ],
                                }
                            ]
                        }
                    ]
                },
            )

        if request.url.host == "cdn.example.test":
            return httpx.Response(200, content=(b"0" * 2048) + b"abc")

        raise AssertionError(
            f"Unexpected Deezer request: {request.method} {request.url}"
        )

    def listenbrainz_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"payload": {}})

    def musicbrainz_handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/ws/2/recording"
        return httpx.Response(200, json={"recordings": []})

    caplog.set_level("WARNING")

    service = DeezerDownloadService(
        arl="demo-cookie",
        download_dir=str(tmp_path),
        quality="FLAC",
        transport=httpx.MockTransport(deezer_handler),
        listenbrainz_service=ListenBrainzService(
            username="demo",
            auth_token="secret-token",
            transport=httpx.MockTransport(listenbrainz_handler),
        ),
        musicbrainz_service=MusicBrainzService(
            transport=httpx.MockTransport(musicbrainz_handler)
        ),
    )

    monkeypatch.setattr(
        "app.services.deezer_download.ensure_cover_art",
        lambda *args, **kwargs: Path(args[0]) / "cover.jpg",
    )

    service.sync_tracks(
        [PlaylistTrack(title="Teardrop", artist="Massive Attack", track_number=1)]
    )

    assert (
        "No MusicBrainz recording ID found for Massive Attack - Mezzanine - 1 - Teardrop"
        in caplog.text
    )
