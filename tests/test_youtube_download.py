from __future__ import annotations

from pathlib import Path

from app.models import PlaylistTrack
from app.services.youtube_download import YouTubeDownloadService


class StubYDL:
    def __init__(self, opts, payload) -> None:
        self.opts = opts
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def extract_info(self, url: str, download: bool = False):
        if not download:
            raise AssertionError(f"Unexpected yt-dlp request: {url!r}, download={download}")

        filepath = self.opts["outtmpl"]["default"].replace("%(ext)s", "m4a")
        Path(filepath).write_text("fake audio", encoding="utf-8")
        return {
            "filepath": filepath,
            "ext": "m4a",
            "artist": "Massive Attack",
            "uploader": "Massive Attack",
            "album": "YouTube",
            "playlist_title": "",
            "title": "Track.Name",
        }


def test_youtube_download_preserves_dotted_title_before_audio_extension(tmp_path) -> None:
    service = YouTubeDownloadService(
        download_dir=str(tmp_path),
        extractor_factory=lambda opts: StubYDL(opts, {}),
    )

    result = service.resolve_track_selection(
        PlaylistTrack(title="Track.Name", artist="Massive Attack", album="Mezzanine"),
        {
            "title": "Track.Name",
            "artist": "Massive Attack",
            "album": "YouTube",
            "link": "https://www.youtube.com/watch?v=demo",
            "provider": "youtube",
        },
    )

    output_path = Path(result["download"]["path"])

    assert output_path.name == "Massive Attack - 0 - Track.Name.m4a"
    assert result["download"]["extension"] == ".m4a"
    assert output_path.exists()
