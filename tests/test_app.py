from __future__ import annotations

import io
import time
from threading import Lock

from app import create_app
from app.models import PlaylistTrack, PlaylistUpload
from app.services.ingest import save_uploaded_playlist
from app.services.sync_jobs import _run_sync_job, create_sync_job, get_sync_job


def test_index_and_health_routes() -> None:
    app = create_app()
    client = app.test_client()

    index_response = client.get("/")
    health_response = client.get("/api/health")

    assert index_response.status_code == 200
    assert b"Playlist Sync" in index_response.data
    assert b"Import playlist and start sync" in index_response.data
    assert b'class="app-tab is-disabled">Review<' not in index_response.data
    assert health_response.status_code == 200
    assert health_response.json["status"] == "ok"


def test_upload_endpoint_keeps_playlist_in_memory(tmp_path) -> None:
    app = create_app()
    app.config.update(TESTING=True, UPLOAD_FOLDER=str(tmp_path))
    client = app.test_client()

    response = client.post(
        "/api/upload",
        data={
            "file": (
                io.BytesIO(b"#EXTM3U\n#EXTINF:245,Massive Attack - Teardrop\n/music/test.flac\n"),
                "my-playlist.m3u",
            )
        },
        content_type="multipart/form-data",
    )

    assert response.status_code == 201
    assert response.json["count"] == 1
    assert response.json["stored_name"].endswith(".m3u")
    assert response.json["saved_path"].startswith("memory://")
    assert not list(tmp_path.iterdir())


def test_sync_endpoint_requires_deezer_configuration() -> None:
    app = create_app()
    app.config.update(
        TESTING=True,
        DEEZER_ARL="",
        NAVIDROME_MUSIC_ROOT="",
    )
    client = app.test_client()

    response = client.post(
        "/api/sync",
        json={"tracks": [{"title": "Teardrop", "artist": "Massive Attack"}]},
    )

    assert response.status_code == 400
    assert "DEEZER_ARL" in response.json["error"]


def test_index_keeps_created_for_you_playlists_visible_after_import(monkeypatch) -> None:
    app = create_app()
    app.config.update(TESTING=True, LISTENBRAINZ_USERNAME="demo")
    client = app.test_client()

    class StubListenBrainz:
        def is_configured(self) -> bool:
            return True

        def list_playlists(self, exclude_playlist_ids=None):
            return [
                {
                    "playlist_id": "created-1",
                    "title": "Fresh Finds For You",
                    "source": "createdfor",
                    "source_label": "Created For You",
                    "jspf_url": "https://listenbrainz.org/playlist/created-1/export/jspf",
                },
                {
                    "playlist_id": "user-1",
                    "title": "Imported Favorites",
                    "source": "user",
                    "source_label": "Your Playlists",
                    "jspf_url": "https://listenbrainz.org/playlist/user-1/export/jspf",
                },
            ]

    monkeypatch.setattr(
        "app.routes.web.ListenBrainzService.from_config",
        lambda config: StubListenBrainz(),
    )
    monkeypatch.setattr(
        "app.routes.web.find_imported_listenbrainz_playlist_ids",
        lambda *args, **kwargs: {"created-1", "user-1"},
    )
    monkeypatch.setattr(
        "app.routes.web.load_settings",
        lambda path: {"playlist_targets": ["weekly exploration", "weekly jams"]},
    )

    response = client.get("/")

    assert response.status_code == 200
    assert b"Fresh Finds For You" in response.data
    assert b"Imported Favorites" not in response.data


def test_sync_status_page_and_json_endpoint() -> None:
    app = create_app()
    app.config.update(TESTING=True)
    client = app.test_client()

    upload = PlaylistUpload(
        source_kind="upload",
        original_name="demo.m3u",
        stored_name="demo-1234.m3u",
        saved_path="g:/tmp/demo-1234.m3u",
        tracks=[PlaylistTrack(title="Teardrop", artist="Massive Attack")],
    )
    job_id = create_sync_job(upload, max_tracks=1)

    page_response = client.get(f"/sync/{job_id}")
    status_response = client.get(f"/sync/{job_id}/status")

    assert page_response.status_code == 200
    assert b"Live download sync" in page_response.data
    assert status_response.status_code == 200
    assert status_response.json["job_id"] == job_id
    assert status_response.json["sync"]["summary"]["requested"] == 1


def test_sync_status_page_shows_low_confidence_review_controls(monkeypatch) -> None:
    app = create_app()
    app.config.update(TESTING=True)
    client = app.test_client()

    job = {
        "job_id": "job-low",
        "status": "completed",
        "created_at": "",
        "started_at": "",
        "completed_at": "",
        "progress_percent": 100,
        "error": "",
        "upload": {
            "saved_path": "memory://demo",
            "original_name": "demo.m3u",
            "playlist_name": "Demo playlist",
        },
        "sync": {
            "mode": "download",
            "provider": "deezer",
            "threshold": 72,
            "processing_mode": "sequential",
            "started_at": "",
            "completed_at": "",
            "summary": {
                "requested": 1,
                "processed": 1,
                "preview": 0,
                "downloaded": 0,
                "already_available": 0,
                "low_confidence": 1,
                "not_found": 0,
                "failed": 0,
            },
            "playlist_export": {
                "configured": True,
                "written": False,
                "pending_review": True,
                "reason": (
                    "Resolve the low-confidence tracks below before exporting "
                    "this playlist to Navidrome."
                ),
            },
            "results": [
                {
                    "index": 1,
                    "status": "low_confidence",
                    "message": "Best match was below the configured confidence threshold.",
                    "track": {
                        "artist": "Massive Attack",
                        "title": "Teardrop",
                        "album": "Mezzanine",
                    },
                    "match": {
                        "artist": "Massive Attack",
                        "title": "Teardrop",
                        "album": "Teardrop",
                        "score": 68.5,
                        "deezer_id": 12345,
                        "provider": "deezer",
                        "provider_label": "Deezer",
                    },
                    "candidates": [
                        {
                            "artist": "Massive Attack",
                            "title": "Teardrop",
                            "album": "Teardrop",
                            "score": 68.5,
                            "deezer_id": 12345,
                            "provider": "deezer",
                            "provider_label": "Deezer",
                        },
                        {
                            "artist": "Massive Attack",
                            "title": "Teardrop",
                            "album": "SoundCloud",
                            "score": 91.0,
                            "id": "soundcloud:123",
                            "provider": "soundcloud",
                            "provider_label": "SoundCloud",
                        },
                    ],
                }
            ],
        },
    }

    monkeypatch.setattr("app.routes.web.get_sync_job", lambda job_id: job)

    response = client.get("/sync/job-low")

    assert response.status_code == 200
    assert b"Resolve low-confidence tracks before Navidrome export" in response.data
    assert b"Search again" in response.data
    assert b"Download selected match" in response.data
    assert b"Downloading selected match" in response.data
    assert b"Accept all remaining as missing" in response.data
    assert b"Download all selected" in response.data
    assert b"[Deezer]" in response.data
    assert b"[SoundCloud]" in response.data
    assert b"SoundCloud review progress" not in response.data


def test_sync_review_bulk_skip_action_redirects_back_to_status(monkeypatch) -> None:
    app = create_app()
    app.config.update(TESTING=True, SECRET_KEY="test-secret")
    client = app.test_client()

    monkeypatch.setattr(
        "app.routes.web.skip_all_low_confidence_candidates",
        lambda job_id: {"sync": {"summary": {"low_confidence": 0}}},
    )

    response = client.post(
        "/sync/job-low/review/bulk",
        data={"action": "skip_all"},
        follow_redirects=False,
    )

    assert response.status_code == 302
    assert response.headers["Location"].endswith("/sync/job-low")


def test_sync_review_bulk_download_selected_action_redirects_back_to_status(
    monkeypatch,
) -> None:
    app = create_app()
    app.config.update(TESTING=True, SECRET_KEY="test-secret")
    client = app.test_client()

    monkeypatch.setattr(
        "app.routes.web.download_selected_low_confidence_candidates",
        lambda job_id, selections: {
            "sync": {"summary": {"low_confidence": 1}},
            "bulk_summary": {"downloaded": 1, "remaining": 1, "attempted": len(selections)},
        },
    )

    response = client.post(
        "/sync/job-low/review/bulk",
        data={
            "action": "download_selected",
            "selected_item_index": ["1"],
            "selected_candidate_id": ["soundcloud:123"],
        },
        follow_redirects=False,
    )

    assert response.status_code == 302
    assert response.headers["Location"].endswith("/sync/job-low")


def test_sync_review_download_action_returns_json_for_async_progress(monkeypatch) -> None:
    app = create_app()
    app.config.update(TESTING=True, SECRET_KEY="test-secret")
    client = app.test_client()

    def resolve_candidate(job_id, item_index, deezer_id, title="", artist="", album=""):
        return {"sync": {"summary": {"low_confidence": 0}}}

    monkeypatch.setattr(
        "app.routes.web.resolve_low_confidence_candidate",
        resolve_candidate,
    )

    response = client.post(
        "/sync/job-low/review",
        data={
            "action": "download",
            "item_index": "1",
            "candidate_id": "soundcloud:123",
            "title": "Teardrop",
            "artist": "Massive Attack",
        },
        headers={"X-Requested-With": "XMLHttpRequest", "Accept": "application/json"},
    )

    assert response.status_code == 200
    assert response.json["ok"] is True
    assert response.json["redirect_url"].endswith("/sync/job-low")


def test_sync_status_page_shows_soundcloud_search_message_while_review_preparing(
    monkeypatch,
) -> None:
    app = create_app()
    app.config.update(TESTING=True)
    client = app.test_client()

    job = {
        "job_id": "job-preparing",
        "status": "running",
        "created_at": "",
        "started_at": "",
        "completed_at": "",
        "progress_percent": 100,
        "error": "",
        "upload": {
            "saved_path": "memory://demo",
            "original_name": "demo.m3u",
            "playlist_name": "Demo playlist",
        },
        "sync": {
            "mode": "download",
            "provider": "deezer",
            "threshold": 72,
            "processing_mode": "sequential",
            "started_at": "",
            "completed_at": "",
            "review_preparing": True,
            "review_search_status": {"completed": 1, "total": 2},
            "summary": {
                "requested": 2,
                "processed": 2,
                "preview": 0,
                "downloaded": 0,
                "already_available": 0,
                "low_confidence": 2,
                "not_found": 0,
                "failed": 0,
            },
            "results": [],
        },
    }

    monkeypatch.setattr("app.routes.web.get_sync_job", lambda job_id: job)

    response = client.get("/sync/job-preparing")

    assert response.status_code == 200
    assert b"Running SoundCloud searches for low-confidence tracks" in response.data


def test_sync_job_holds_playlist_export_until_low_confidence_is_resolved(tmp_path) -> None:
    class StubSoundCloudService:
        def __init__(self) -> None:
            self.search_requests: list[tuple[str, int, int | None]] = []

        def is_configured(self) -> bool:
            return True

        def search_track(self, track, limit=10, *, max_queries=None):
            self.search_requests.append((track.title, limit, max_queries))
            return [
                {
                    "title": "Teardrop",
                    "artist": "Massive Attack",
                    "album": "SoundCloud",
                    "score": 91.0,
                    "id": "soundcloud:123",
                    "provider": "soundcloud",
                    "provider_label": "SoundCloud",
                },
            ]

    class StubService:
        def __init__(self) -> None:
            self.soundcloud_service = StubSoundCloudService()

        def sync_tracks(self, tracks, max_tracks=None, progress_callback=None):
            result = {
                "mode": "download",
                "provider": "deezer",
                "threshold": 72,
                "processing_mode": "sequential",
                "started_at": "2026-04-09T00:00:00+00:00",
                "completed_at": "2026-04-09T00:00:01+00:00",
                "summary": {
                    "requested": 1,
                    "processed": 1,
                    "preview": 0,
                    "downloaded": 0,
                    "already_available": 0,
                    "low_confidence": 1,
                    "not_found": 0,
                    "failed": 0,
                },
                "results": [
                    {
                        "index": 1,
                        "track": {"title": "Teardrop", "artist": "Massive Attack"},
                        "status": "low_confidence",
                        "message": "Best match was below the configured confidence threshold.",
                        "match": {"title": "Teardrop", "artist": "Massive Attack", "score": 68.5},
                        "candidates": [
                            {
                                "title": "Teardrop",
                                "artist": "Massive Attack",
                                "score": 68.5,
                                "deezer_id": 12345,
                                "provider": "deezer",
                                "provider_label": "Deezer",
                            }
                        ],
                    }
                ],
            }
            if progress_callback is not None:
                progress_callback(result)
            return result

        def search_track(self, track, limit=8, include_soundcloud=False):
            raise AssertionError(
                "Review preload should reuse the existing Deezer candidates instead of "
                "running the full provider search again."
            )

    service = StubService()
    upload = PlaylistUpload(
        source_kind="upload",
        original_name="demo.m3u",
        playlist_name="Weekly Jams",
        stored_name="demo-1234.m3u",
        saved_path="memory://demo-1234",
        tracks=[PlaylistTrack(title="Teardrop", artist="Massive Attack")],
    )

    job_id = create_sync_job(upload, max_tracks=1)
    _run_sync_job(
        job_id,
        upload,
        service,
        1,
        str(tmp_path / "navidrome_playlists"),
        "",
    )

    job = get_sync_job(job_id)

    assert job is not None
    assert job["status"] == "completed"
    assert job["sync"]["playlist_export"]["written"] is False
    assert job["sync"]["playlist_export"]["pending_review"] is True
    assert any(
        candidate.get("provider") == "soundcloud"
        for candidate in job["sync"]["results"][0]["candidates"]
    )
    assert service.soundcloud_service.search_requests == [("Teardrop", 4, 1)]
    assert not (tmp_path / "navidrome_playlists" / "Weekly Jams.m3u").exists()


def test_sync_job_keeps_soundcloud_candidates_visible_when_deezer_list_is_full(
    tmp_path,
) -> None:
    class StubSoundCloudService:
        def is_configured(self) -> bool:
            return True

        def search_track(self, track, limit=10, *, max_queries=None):
            return [
                {
                    "title": track.title,
                    "artist": track.artist,
                    "album": "SoundCloud",
                    "score": 91.0,
                    "id": f"soundcloud:{track.title}",
                    "provider": "soundcloud",
                    "provider_label": "SoundCloud",
                }
            ]

    class StubService:
        def __init__(self) -> None:
            self.soundcloud_service = StubSoundCloudService()

        def sync_tracks(self, tracks, max_tracks=None, progress_callback=None):
            deezer_candidates = [
                {
                    "title": f"Teardrop option {index}",
                    "artist": "Massive Attack",
                    "score": 68.0 - index,
                    "deezer_id": 12000 + index,
                    "provider": "deezer",
                    "provider_label": "Deezer",
                }
                for index in range(8)
            ]
            result = {
                "mode": "download",
                "provider": "deezer",
                "threshold": 72,
                "processing_mode": "sequential",
                "started_at": "2026-04-09T00:00:00+00:00",
                "completed_at": "2026-04-09T00:00:01+00:00",
                "summary": {
                    "requested": 1,
                    "processed": 1,
                    "preview": 0,
                    "downloaded": 0,
                    "already_available": 0,
                    "low_confidence": 1,
                    "not_found": 0,
                    "failed": 0,
                },
                "results": [
                    {
                        "index": 1,
                        "track": {"title": "Teardrop", "artist": "Massive Attack"},
                        "status": "low_confidence",
                        "match": deezer_candidates[0],
                        "candidates": deezer_candidates,
                    }
                ],
            }
            if progress_callback is not None:
                progress_callback(result)
            return result

        def search_track(self, track, limit=8, include_soundcloud=False):
            raise AssertionError("The preload path should reuse the Deezer candidates.")

    service = StubService()
    upload = PlaylistUpload(
        source_kind="upload",
        original_name="demo.m3u",
        playlist_name="Candidate Mix",
        stored_name="demo-full.m3u",
        saved_path="memory://demo-full",
        tracks=[PlaylistTrack(title="Teardrop", artist="Massive Attack")],
    )

    job_id = create_sync_job(upload, max_tracks=1)
    _run_sync_job(
        job_id,
        upload,
        service,
        1,
        str(tmp_path / "navidrome_playlists"),
        "",
    )

    job = get_sync_job(job_id)

    assert job is not None
    assert any(
        candidate.get("provider") == "soundcloud"
        for candidate in job["sync"]["results"][0]["candidates"]
    )


def test_sync_job_uses_broader_soundcloud_fallback_when_fast_preload_finds_nothing(
    tmp_path,
) -> None:
    class StubSoundCloudService:
        def __init__(self) -> None:
            self.search_requests: list[int | None] = []

        def is_configured(self) -> bool:
            return True

        def search_track(self, track, limit=10, *, max_queries=None):
            self.search_requests.append(max_queries)
            if max_queries == 1:
                return []
            return [
                {
                    "title": track.title,
                    "artist": track.artist,
                    "album": "SoundCloud",
                    "score": 90.0,
                    "id": f"soundcloud:{track.title}",
                    "provider": "soundcloud",
                    "provider_label": "SoundCloud",
                }
            ]

    class StubService:
        def __init__(self) -> None:
            self.soundcloud_service = StubSoundCloudService()

        def sync_tracks(self, tracks, max_tracks=None, progress_callback=None):
            result = {
                "mode": "download",
                "provider": "deezer",
                "threshold": 72,
                "processing_mode": "sequential",
                "started_at": "2026-04-09T00:00:00+00:00",
                "completed_at": "2026-04-09T00:00:01+00:00",
                "summary": {
                    "requested": 1,
                    "processed": 1,
                    "preview": 0,
                    "downloaded": 0,
                    "already_available": 0,
                    "low_confidence": 1,
                    "not_found": 0,
                    "failed": 0,
                },
                "results": [
                    {
                        "index": 1,
                        "track": {"title": "Teardrop", "artist": "Massive Attack"},
                        "status": "low_confidence",
                        "match": {
                            "title": "Teardrop",
                            "artist": "Massive Attack",
                            "score": 68.5,
                        },
                        "candidates": [
                            {
                                "title": "Teardrop",
                                "artist": "Massive Attack",
                                "score": 68.5,
                                "deezer_id": 12345,
                                "provider": "deezer",
                                "provider_label": "Deezer",
                            }
                        ],
                    }
                ],
            }
            if progress_callback is not None:
                progress_callback(result)
            return result

        def search_track(self, track, limit=8, include_soundcloud=False):
            raise AssertionError("The preload path should not rerun the full Deezer search.")

    service = StubService()
    upload = PlaylistUpload(
        source_kind="upload",
        original_name="demo.m3u",
        playlist_name="Fallback Demo",
        stored_name="demo-fallback.m3u",
        saved_path="memory://demo-fallback",
        tracks=[PlaylistTrack(title="Teardrop", artist="Massive Attack")],
    )

    job_id = create_sync_job(upload, max_tracks=1)
    _run_sync_job(
        job_id,
        upload,
        service,
        1,
        str(tmp_path / "navidrome_playlists"),
        "",
    )

    job = get_sync_job(job_id)

    assert job is not None
    assert any(
        candidate.get("provider") == "soundcloud"
        for candidate in job["sync"]["results"][0]["candidates"]
    )
    assert service.soundcloud_service.search_requests == [1, 3]


def test_sync_job_preloads_low_confidence_tracks_in_parallel(tmp_path) -> None:
    class ParallelSoundCloudService:
        def __init__(self) -> None:
            self._lock = Lock()
            self.active = 0
            self.max_active = 0

        def is_configured(self) -> bool:
            return True

        def search_track(self, track, limit=10, *, max_queries=None):
            with self._lock:
                self.active += 1
                self.max_active = max(self.max_active, self.active)
            try:
                time.sleep(0.05)
                return [
                    {
                        "title": track.title,
                        "artist": track.artist,
                        "album": "SoundCloud",
                        "score": 88.0,
                        "id": f"soundcloud:{track.title}",
                        "provider": "soundcloud",
                        "provider_label": "SoundCloud",
                    }
                ]
            finally:
                with self._lock:
                    self.active -= 1

    class StubService:
        def __init__(self) -> None:
            self.soundcloud_service = ParallelSoundCloudService()

        def sync_tracks(self, tracks, max_tracks=None, progress_callback=None):
            result = {
                "mode": "download",
                "provider": "deezer",
                "threshold": 72,
                "processing_mode": "sequential",
                "started_at": "2026-04-09T00:00:00+00:00",
                "completed_at": "2026-04-09T00:00:01+00:00",
                "summary": {
                    "requested": 2,
                    "processed": 2,
                    "preview": 0,
                    "downloaded": 0,
                    "already_available": 0,
                    "low_confidence": 2,
                    "not_found": 0,
                    "failed": 0,
                },
                "results": [
                    {
                        "index": 1,
                        "track": {"title": "Teardrop", "artist": "Massive Attack"},
                        "status": "low_confidence",
                        "match": {"title": "Teardrop", "artist": "Massive Attack"},
                        "candidates": [
                            {
                                "title": "Teardrop",
                                "artist": "Massive Attack",
                                "deezer_id": 12345,
                                "provider": "deezer",
                                "provider_label": "Deezer",
                            }
                        ],
                    },
                    {
                        "index": 2,
                        "track": {"title": "Angel", "artist": "Massive Attack"},
                        "status": "low_confidence",
                        "match": {"title": "Angel", "artist": "Massive Attack"},
                        "candidates": [
                            {
                                "title": "Angel",
                                "artist": "Massive Attack",
                                "deezer_id": 67890,
                                "provider": "deezer",
                                "provider_label": "Deezer",
                            }
                        ],
                    },
                ],
            }
            if progress_callback is not None:
                progress_callback(result)
            return result

        def search_track(self, track, limit=8, include_soundcloud=False):
            raise AssertionError("Parallel preload should not rerun the full Deezer search.")

    service = StubService()
    upload = PlaylistUpload(
        source_kind="upload",
        original_name="demo.m3u",
        playlist_name="Parallel Demo",
        stored_name="demo-5678.m3u",
        saved_path="memory://demo-5678",
        tracks=[
            PlaylistTrack(title="Teardrop", artist="Massive Attack"),
            PlaylistTrack(title="Angel", artist="Massive Attack"),
        ],
    )

    job_id = create_sync_job(upload, max_tracks=2)
    _run_sync_job(
        job_id,
        upload,
        service,
        2,
        str(tmp_path / "navidrome_playlists"),
        "",
    )

    assert service.soundcloud_service.max_active >= 2


def test_index_keeps_link_to_active_sync_job() -> None:
    app = create_app()
    app.config.update(TESTING=True)
    client = app.test_client()

    upload = PlaylistUpload(
        source_kind="upload",
        original_name="demo.m3u",
        stored_name="demo-1234.m3u",
        saved_path="g:/tmp/demo-1234.m3u",
        tracks=[PlaylistTrack(title="Teardrop", artist="Massive Attack")],
    )
    job_id = create_sync_job(upload, max_tracks=1)

    with client.session_transaction() as session:
        session["active_sync_job_id"] = job_id

    response = client.get("/")

    assert response.status_code == 200
    assert f"/sync/{job_id}".encode() in response.data
    assert b"Open active sync status" in response.data


def test_import_playlist_redirects_straight_to_sync_when_deezer_is_configured(
    tmp_path,
    monkeypatch,
) -> None:
    app = create_app()
    app.config.update(
        TESTING=True,
        UPLOAD_FOLDER=str(tmp_path / "uploads"),
        NAVIDROME_PLAYLISTS_DIR=str(tmp_path / "navidrome_playlists"),
        PLAYLIST_DB_PATH=str(tmp_path / "playlist_history.db"),
        SYNC_MAX_TRACKS=25,
    )
    client = app.test_client()

    class StubDeezer:
        def is_configured(self) -> bool:
            return True

    monkeypatch.setattr(
        "app.routes.web.DeezerDownloadService.from_config", lambda config: StubDeezer()
    )
    monkeypatch.setattr(
        "app.routes.web.start_sync_job",
        lambda upload, service, max_tracks, navidrome_playlists_dir, playlist_db_path: "job-123",
    )

    response = client.post(
        "/review",
        data={
            "playlist_file": (
                io.BytesIO(
                    b"#EXTM3U\n"
                    b"#EXTINF:245,Massive Attack - Teardrop\n"
                    b"Massive Attack/Mezzanine/Teardrop.flac\n"
                ),
                "my-playlist.m3u",
            )
        },
        content_type="multipart/form-data",
    )

    assert response.status_code == 302
    assert response.headers["Location"].endswith("/sync/job-123")

    with client.session_transaction() as session:
        assert session["active_sync_job_id"] == "job-123"
        assert session["active_review_saved_path"].startswith("memory://")


def test_review_page_can_be_reloaded_and_export_playlist_from_ui(tmp_path) -> None:
    app = create_app()
    app.config.update(
        TESTING=True,
        UPLOAD_FOLDER=str(tmp_path / "uploads"),
        NAVIDROME_PLAYLISTS_DIR=str(tmp_path / "navidrome_playlists"),
    )
    client = app.test_client()

    upload = save_uploaded_playlist(
        app.config["UPLOAD_FOLDER"],
        "weekly-jams.m3u",
        (
            b"#EXTM3U\n"
            b"#EXTINF:245,Massive Attack - Teardrop\n"
            b"Massive Attack/Mezzanine/Teardrop.flac\n"
        ),
    )

    review_response = client.get("/review", query_string={"saved_path": upload.saved_path})
    export_response = client.post(
        "/navidrome/export",
        data={
            "saved_path": upload.saved_path,
            "playlist_name": "Weekly Jams for geekdadkevin, week of 2026-04-07 Tue",
        },
        follow_redirects=True,
    )

    assert review_response.status_code == 200
    assert b"Review playlist" in review_response.data
    assert b"Create/update Navidrome playlist now" in review_response.data

    assert export_response.status_code == 200
    assert b"Navidrome playlist updated" in export_response.data
    assert (tmp_path / "navidrome_playlists" / "Weekly Jams.m3u").exists()


def test_export_playlist_from_ui_does_not_preserve_stale_source_path(tmp_path) -> None:
    app = create_app()
    app.config.update(
        TESTING=True,
        UPLOAD_FOLDER=str(tmp_path / "uploads"),
        NAVIDROME_PLAYLISTS_DIR=str(tmp_path / "navidrome_playlists"),
        PLAYLIST_DB_PATH=str(tmp_path / "playlist_history.db"),
        NAVIDROME_MUSIC_ROOT=str(tmp_path / "music"),
        NAVIDROME_M3U_PATH_PREFIX="..",
    )
    client = app.test_client()

    upload = save_uploaded_playlist(
        app.config["UPLOAD_FOLDER"],
        "weekly-jams.m3u",
        (
            b"#EXTM3U\n"
            b"#EXTINF:185,Better Than Ezra - Good\n"
            b"../Better Than Ezra/Deluxe/01-02 - Good.ogg\n"
        ),
    )

    response = client.post(
        "/navidrome/export",
        data={
            "saved_path": upload.saved_path,
            "playlist_name": "Weekly Jams",
        },
        follow_redirects=True,
    )

    written = (tmp_path / "navidrome_playlists" / "Weekly Jams.m3u").read_text(encoding="utf-8")

    assert response.status_code == 200
    assert "# MISSING: Better Than Ezra - Good" in written
    assert "\n../Better Than Ezra/Deluxe/01-02 - Good.ogg\n" not in written
