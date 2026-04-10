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
    assert ranked[0]["accepted"] is True
