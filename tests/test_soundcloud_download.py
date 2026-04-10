from __future__ import annotations

from app.models import PlaylistTrack
from app.services.soundcloud_download import SoundCloudDownloadService


class StubYDL:
    def __init__(self, opts, payload) -> None:
        self.opts = opts
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def extract_info(self, url: str, download: bool = False):
        if url.startswith("scsearch"):
            return self.payload
        raise AssertionError(f"Unexpected yt-dlp request: {url!r}, download={download}")


class TimeoutYDL(StubYDL):
    def extract_info(self, url: str, download: bool = False):
        raise TimeoutError("_ssl.c:993: The handshake operation timed out")


def test_soundcloud_search_track_returns_ranked_match() -> None:
    payload = {
        "entries": [
            {
                "id": "12345",
                "title": "Teardrop",
                "uploader": "Massive Attack",
                "duration": 245,
                "webpage_url": "https://soundcloud.com/demo/teardrop",
            }
        ]
    }

    service = SoundCloudDownloadService(
        download_dir="/tmp/downloads",
        extractor_factory=lambda opts: StubYDL(opts, payload),
    )

    ranked = service.search_track(PlaylistTrack(title="Teardrop", artist="Massive Attack"))

    assert ranked[0]["provider"] == "soundcloud"
    assert ranked[0]["title"] == "Teardrop"
    assert ranked[0]["album"] == "SoundCloud"
    assert ranked[0]["accepted"] is True


def test_soundcloud_uses_retry_and_timeout_options_for_search() -> None:
    seen: dict[str, object] = {}

    def factory(opts):
        seen.update(opts)
        return StubYDL(opts, {"entries": []})

    service = SoundCloudDownloadService(
        download_dir="/tmp/downloads",
        extractor_factory=factory,
        request_timeout=25.0,
        request_retries=4,
    )

    service.search_track(PlaylistTrack(title="Teardrop", artist="Massive Attack"))

    assert seen["socket_timeout"] == 25.0
    assert seen["retries"] == 4
    assert seen["extractor_retries"] == 4
    assert seen["source_address"] == "0.0.0.0"


def test_soundcloud_search_track_returns_empty_on_handshake_timeout() -> None:
    service = SoundCloudDownloadService(
        download_dir="/tmp/downloads",
        extractor_factory=lambda opts: TimeoutYDL(opts, {"entries": []}),
    )

    ranked = service.search_track(PlaylistTrack(title="Teardrop", artist="Massive Attack"))

    assert ranked == []


def test_soundcloud_uses_soundcloud_album_when_provider_album_missing(tmp_path) -> None:
    service = SoundCloudDownloadService(
        download_dir=str(tmp_path),
        extractor_factory=lambda opts: StubYDL(opts, {"entries": []}),
    )

    stem_path = service._build_stem_path(
        {
            "title": "Teardrop",
            "artist": "Massive Attack",
            "album": "",
        },
        PlaylistTrack(title="Teardrop", artist="Massive Attack", album="Mezzanine"),
    )

    assert stem_path == tmp_path / "Massive Attack" / "SoundCloud" / "Teardrop"
