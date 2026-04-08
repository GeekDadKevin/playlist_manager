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
    assert b"Octo Playlist Sync" in index_response.data
    assert health_response.status_code == 200
    assert health_response.json["status"] == "ok"


def test_upload_endpoint_saves_playlist_file(tmp_path) -> None:
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
    assert list(tmp_path.iterdir())


def test_sync_endpoint_requires_octo_configuration() -> None:
    app = create_app()
    app.config.update(
        TESTING=True,
        OCTO_FIESTA_BASE_URL="",
        OCTO_FIESTA_USERNAME="",
        OCTO_FIESTA_PASSWORD="",
        OCTO_FIESTA_TOKEN="",
        OCTO_FIESTA_SALT="",
    )
    client = app.test_client()

    response = client.post(
        "/api/sync",
        json={"tracks": [{"title": "Teardrop", "artist": "Massive Attack"}]},
    )

    assert response.status_code == 400
    assert "OCTO_FIESTA_BASE_URL" in response.json["error"]


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
    assert b"Octo-Fiesta live sync" in page_response.data
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
    assert (tmp_path / "navidrome_playlists" / "weekly-jams.m3u").exists()
