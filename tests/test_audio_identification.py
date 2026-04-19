from __future__ import annotations

import subprocess

import httpx
from app.services.audio_identification import AcoustIdService


class StubMusicBrainzService:
    def __init__(self, response: dict[str, object] | None = None) -> None:
        self.response = response or {
            "recording_mbid": "recording-123",
            "release_mbid": "release-456",
            "title": "Teardrop",
            "artist": "Massive Attack",
            "album": "Mezzanine",
            "albumartist": "Massive Attack",
            "artist_mbid": "artist-1",
            "albumartist_mbid": "artist-1",
            "track_number": 10,
        }

    def lookup_recording_details(self, **kwargs):
        del kwargs
        return dict(self.response)


def test_identify_track_returns_musicbrainz_details(tmp_path) -> None:
    audio_path = tmp_path / "teardrop.flac"
    audio_path.write_bytes(b"audio")

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v2/lookup"
        body = request.content.decode("utf-8")
        assert "meta=recordings+releasegroups+releases" in body
        return httpx.Response(
            200,
            json={
                "status": "ok",
                "results": [
                    {
                        "id": "acoustid-1",
                        "score": 0.97,
                        "recordings": [
                            {
                                "id": "recording-123",
                                "title": "Teardrop",
                                "artists": [
                                    {"id": "artist-1", "name": "Massive Attack"}
                                ],
                                "releases": [
                                    {"id": "release-456", "title": "Mezzanine"}
                                ],
                            }
                        ],
                    }
                ],
            },
        )

    service = AcoustIdService(
        api_key="demo-key",
        fpcalc_path="fpcalc",
        transport=httpx.MockTransport(handler),
        command_runner=lambda *args, **kwargs: subprocess.CompletedProcess(
            args[0],
            0,
            stdout='{"duration": 245.4, "fingerprint": "abc123"}',
            stderr="",
        ),
    )

    result = service.identify_track(
        audio_path, musicbrainz_service=StubMusicBrainzService()
    )

    assert result["accepted"] is True
    assert result["match"]["recording_mbid"] == "recording-123"
    assert result["match"]["release_mbid"] == "release-456"
    assert result["match"]["album"] == "Mezzanine"
    assert result["match"]["acoustid_id"] == "acoustid-1"


def test_identify_track_marks_low_score_as_unaccepted(tmp_path) -> None:
    audio_path = tmp_path / "unknown.flac"
    audio_path.write_bytes(b"audio")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "status": "ok",
                "results": [
                    {
                        "id": "acoustid-2",
                        "score": 0.42,
                        "recordings": [
                            {
                                "id": "recording-999",
                                "title": "Unknown Song",
                                "artists": [
                                    {"id": "artist-9", "name": "Unknown Artist"}
                                ],
                                "releases": [
                                    {"id": "release-9", "title": "Unknown Album"}
                                ],
                            }
                        ],
                    }
                ],
            },
        )

    service = AcoustIdService(
        api_key="demo-key",
        fpcalc_path="fpcalc",
        score_threshold=0.9,
        transport=httpx.MockTransport(handler),
        command_runner=lambda *args, **kwargs: subprocess.CompletedProcess(
            args[0],
            0,
            stdout='{"duration": 245.4, "fingerprint": "abc123"}',
            stderr="",
        ),
    )

    result = service.identify_track(
        audio_path,
        musicbrainz_service=StubMusicBrainzService(
            {
                "recording_mbid": "recording-999",
                "release_mbid": "release-9",
                "title": "Unknown Song",
                "artist": "Unknown Artist",
                "album": "Unknown Album",
                "albumartist": "Unknown Artist",
                "artist_mbid": "artist-9",
                "albumartist_mbid": "artist-9",
                "track_number": None,
            }
        ),
    )

    assert result["accepted"] is False
    assert result["match"]["recording_mbid"] == "recording-999"
    assert result["match"]["acoustid_score"] == 0.42
