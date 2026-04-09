from __future__ import annotations

import io

from app import create_app
from app.models import PlaylistTrack, PlaylistUpload
from app.services.ingest import save_uploaded_playlist
from app.services.sync_jobs import create_sync_job


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
