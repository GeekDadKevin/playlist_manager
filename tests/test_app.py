from __future__ import annotations

import io
import sqlite3
import time
from threading import Lock

from app import create_app
from app.models import PlaylistTrack, PlaylistUpload
from app.services.ingest import save_uploaded_playlist
from app.services.library_index import (
    list_musicbrainz_tag_candidates,
    load_library_tool_run,
    record_audio_health_result,
    record_library_tool_run,
    refresh_library_index,
)
from app.services.song_metadata import write_song_metadata_xml
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


def test_tools_page_lists_audio_integrity_checker(tmp_path) -> None:
    app = create_app()
    app.config.update(
        TESTING=True,
        NAVIDROME_MUSIC_ROOT=str(tmp_path),
        LIBRARY_INDEX_DB_PATH=str(tmp_path / "library_index.db"),
    )

    audio_path = tmp_path / "Massive Attack" / "Mezzanine" / "Teardrop.flac"
    audio_path.parent.mkdir(parents=True, exist_ok=True)
    audio_path.write_bytes(b"audio")
    write_song_metadata_xml(
        audio_path,
        title="Teardrop",
        artist="Massive Attack",
        album="Mezzanine",
        provider="youtube",
        downloaded_from="youtube",
        source="https://www.youtube.com/watch?v=teardrop",
    )
    refresh_library_index(app.config["LIBRARY_INDEX_DB_PATH"], tmp_path)
    record_audio_health_result(
        app.config["LIBRARY_INDEX_DB_PATH"],
        audio_path,
        status="warning",
        message="suspicious header",
    )

    client = app.test_client()

    response = client.get("/tools")
    filtered = client.get("/tools?report_filter=corrupted-audio")

    assert response.status_code == 200
    assert b"Refresh Library Catalog" in response.data
    assert b"Check Audio Integrity" in response.data
    assert b"Identify By Structure" in response.data
    assert b"Identify Tracks By Audio" in response.data
    assert b"Sync XML Sidecars" in response.data
    assert b"Fix Audio Tags" in response.data
    assert b"Enrich MusicBrainz Tags" not in response.data
    assert b"Last Summary" in response.data
    assert b"Catalog Report" in response.data
    assert b"Needs MB Verify" in response.data
    assert b"Accepted As Is" in response.data
    assert b"View Status" in response.data
    assert b"Navigation is blocked until the current run finishes" in response.data
    assert b"Missing XML" in response.data
    assert filtered.status_code == 200
    assert b"suspicious header" in filtered.data


def test_catalog_page_lists_anomaly_rows(tmp_path) -> None:
    app = create_app()
    app.config.update(
        TESTING=True,
        NAVIDROME_MUSIC_ROOT=str(tmp_path),
        LIBRARY_INDEX_DB_PATH=str(tmp_path / "library_index.db"),
        DATA_DIR=str(tmp_path / "data"),
    )

    audio_path = tmp_path / "Burial" / "Untrue" / "Archangel.flac"
    audio_path.parent.mkdir(parents=True, exist_ok=True)
    audio_path.write_bytes(b"audio")
    refresh_library_index(app.config["LIBRARY_INDEX_DB_PATH"], tmp_path)

    with sqlite3.connect(app.config["LIBRARY_INDEX_DB_PATH"]) as conn:
        conn.execute(
            "UPDATE library_files SET embedded_title = ?, embedded_artist = ?, embedded_album = ?, "
            "embedded_albumartist = ?, embedded_track_number = ?, audio_health_status = ?, "
            "audio_health_message = ?, identify_audio_review_status = ? WHERE audio_path = ?",
            (
                "Archangel",
                "Burial",
                "Untrue",
                "Burial",
                "",
                "warning",
                "bad frame",
                "accepted-as-is",
                str(audio_path),
            ),
        )
        conn.commit()

    client = app.test_client()
    response = client.get("/catalog?issue_filter=missing-info")

    assert response.status_code == 200
    assert b"Track Catalog" in response.data
    assert b"Archangel.flac" in response.data
    assert b"Missing Info" in response.data
    assert b"Accepted As Is" in response.data
    assert b"Run On Selected" in response.data
    assert b"MusicBrainz" in response.data


def test_catalog_batch_route_uses_selected_paths(tmp_path, monkeypatch) -> None:
    app = create_app()
    app.config.update(
        TESTING=True,
        NAVIDROME_MUSIC_ROOT=str(tmp_path),
        LIBRARY_INDEX_DB_PATH=str(tmp_path / "library_index.db"),
        DATA_DIR=str(tmp_path / "data"),
    )
    captured: dict[str, object] = {}

    def fake_batch(config, *, action, relative_paths, dry_run=False):
        captured["action"] = action
        captured["relative_paths"] = relative_paths
        captured["dry_run"] = dry_run
        return {
            "label": "Fix Audio Tags",
            "summary_line": "SUMMARY  total_changed=1",
            "exit_code": 0,
        }

    monkeypatch.setattr("app.routes.web.run_catalog_batch_action", fake_batch)

    client = app.test_client()
    response = client.post(
        "/catalog/batch",
        data={
            "action": "fix-tags",
            "selected_paths": ["Burial/Untrue/Archangel.flac"],
            "dry_run": "on",
            "return_query": "issue_filter=missing-info",
        },
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert captured == {
        "action": "fix-tags",
        "relative_paths": ["Burial/Untrue/Archangel.flac"],
        "dry_run": True,
    }
    assert b"Fix Audio Tags: SUMMARY  total_changed=1" in response.data


def test_catalog_batch_start_route_returns_live_job_metadata(tmp_path, monkeypatch) -> None:
    app = create_app()
    app.config.update(
        TESTING=True,
        NAVIDROME_MUSIC_ROOT=str(tmp_path),
        LIBRARY_INDEX_DB_PATH=str(tmp_path / "library_index.db"),
        DATA_DIR=str(tmp_path / "data"),
    )
    captured: dict[str, object] = {}

    def fake_start_process_job(tool, **kwargs):
        captured["tool"] = tool
        captured.update(kwargs)
        return {
            "tool": tool,
            "label": kwargs["label"],
            "status": "running",
            "lines": ["CHECK: 1/1"],
        }

    monkeypatch.setattr("app.routes.web.start_process_job", fake_start_process_job)

    client = app.test_client()
    response = client.post(
        "/catalog/batch/start",
        data={
            "action": "check-audio",
            "selected_paths": ["Burial/Untrue/Archangel.flac"],
            "dry_run": "on",
        },
    )

    assert response.status_code == 202
    assert response.json["ok"] is True
    assert response.json["stream_url"] == "/catalog/batch/stream"
    assert captured["tool"] == "catalog-batch"
    assert captured["dry_run"] is True
    assert "run_catalog_batch.py" in " ".join(captured["cmd"])
    assert captured["metadata"]["selected_count"] == 1


def test_catalog_batch_stream_route_emits_lines_and_exit(monkeypatch) -> None:
    app = create_app()
    app.config.update(TESTING=True)

    snapshots = iter(
        [
            {
                "tool": "catalog-batch",
                "status": "running",
                "lines": ["CHECK: 1/2"],
            },
            {
                "tool": "catalog-batch",
                "status": "done",
                "exit_code": 0,
                "lines": ["CHECK: 1/2", "SUMMARY total_changed=1"],
            },
        ]
    )

    def fake_get_tool_status(tool, line_limit=400):
        return next(snapshots)

    monkeypatch.setattr("app.routes.web.get_tool_status", fake_get_tool_status)
    monkeypatch.setattr("app.routes.web.time.sleep", lambda _seconds: None)

    client = app.test_client()
    response = client.get("/catalog/batch/stream")
    payload = response.data.decode("utf-8")

    assert response.status_code == 200
    assert response.mimetype == "text/event-stream"
    assert "data: CHECK: 1/2" in payload
    assert "data: SUMMARY total_changed=1" in payload
    assert "data: __EXIT__0" in payload


def test_catalog_batch_status_and_stop_routes_use_shared_tool_state(monkeypatch) -> None:
    app = create_app()
    app.config.update(TESTING=True)

    monkeypatch.setattr(
        "app.routes.web.get_tool_status",
        lambda tool, line_limit=400: {
            "tool": tool,
            "label": "Catalog Batch · Check Audio Integrity",
            "status": "running",
            "lines": ["CHECK: 1/1"],
        },
    )
    monkeypatch.setattr(
        "app.routes.web.stop_tool",
        lambda tool: {
            "tool": tool,
            "label": "Catalog Batch · Check Audio Integrity",
            "status": "stopping",
            "lines": ["CHECK: 1/1", "Stopping..."],
        },
    )

    client = app.test_client()
    status_response = client.get("/catalog/batch/status")
    stop_response = client.post("/catalog/batch/stop")

    assert status_response.status_code == 200
    assert status_response.json["active"] is True
    assert status_response.json["primary"]["tool"] == "catalog-batch"
    assert stop_response.status_code == 200
    assert stop_response.json["primary"]["status"] == "stopping"


def test_tools_page_shows_ffmpeg_indicator_for_audio_checker(tmp_path, monkeypatch) -> None:
    app = create_app()
    app.config.update(TESTING=True, NAVIDROME_MUSIC_ROOT=str(tmp_path))

    monkeypatch.setattr("app.routes.web.find_ffmpeg_executable", lambda: "C:/ffmpeg/bin/ffmpeg.exe")

    client = app.test_client()
    response = client.get("/tools")

    assert response.status_code == 200
    assert b"tool-presence-dot present" in response.data
    assert b"ffmpeg detected" in response.data


def test_tools_page_shows_last_identify_audio_review_panel(tmp_path) -> None:
    app = create_app()
    app.config.update(
        TESTING=True,
        NAVIDROME_MUSIC_ROOT=str(tmp_path),
        LIBRARY_INDEX_DB_PATH=str(tmp_path / "library_index.db"),
    )
    record_library_tool_run(
        app.config["LIBRARY_INDEX_DB_PATH"],
        tool_name="identify-audio",
        root=tmp_path,
        run_mode="incremental",
        started_at="2026-04-15T12:00:00+00:00",
        completed_at="2026-04-15T12:02:00+00:00",
        scanned_count=3,
        changed_count=1,
        error_count=2,
        result={
            "review": {
                "low_confidence_count": 1,
                "no_match_count": 1,
                "recorded_count": 2,
                "truncated_count": 0,
                "low_confidence_items": [
                    {
                        "relative_path": "Unknown/Album/maybe.flac",
                        "reason_label": "Low confidence",
                        "match_artist": "Possible Artist",
                        "match_title": "Possible Song",
                        "match_album": "Possible Album",
                        "acoustid_score": 0.41,
                    }
                ],
                "no_match_items": [
                    {
                        "relative_path": "Unknown/Album/missing.flac",
                        "message": "No AcoustID recording match was returned for this file.",
                    }
                ],
            }
        },
    )

    client = app.test_client()
    response = client.get("/tools")

    assert response.status_code == 200
    assert b"Last Fingerprint Review" in response.data
    assert b"Review Saved Matches" in response.data
    assert b"Fingerprint Review" in response.data
    assert b"Manual Fingerprint Review" in response.data
    assert b"No Fingerprint Match" in response.data
    assert b">Keep<" in response.data
    assert b"Similarity check" not in response.data


def test_identify_audio_review_accept_updates_saved_review(tmp_path, monkeypatch) -> None:
    app = create_app()
    app.config.update(
        TESTING=True,
        NAVIDROME_MUSIC_ROOT=str(tmp_path),
        LIBRARY_INDEX_DB_PATH=str(tmp_path / "library_index.db"),
    )
    audio_path = tmp_path / "Unknown" / "Album" / "maybe.flac"
    audio_path.parent.mkdir(parents=True, exist_ok=True)
    audio_path.write_bytes(b"audio")
    write_song_metadata_xml(audio_path, title="maybe", artist="Unknown", album="Album")
    record_library_tool_run(
        app.config["LIBRARY_INDEX_DB_PATH"],
        tool_name="identify-audio",
        root=tmp_path,
        run_mode="incremental",
        started_at="2026-04-15T12:00:00+00:00",
        completed_at="2026-04-15T12:02:00+00:00",
        scanned_count=1,
        changed_count=0,
        error_count=1,
        result={
            "review": {
                "low_confidence_count": 1,
                "no_match_count": 0,
                "recorded_count": 1,
                "truncated_count": 0,
                "low_confidence_items": [
                    {
                        "relative_path": "Unknown/Album/maybe.flac",
                        "recording_mbid": "recording-123",
                        "release_mbid": "release-456",
                        "match_artist": "Possible Artist",
                        "match_title": "Possible Song",
                        "match_album": "Possible Album",
                        "match_albumartist": "Possible Artist",
                        "artist_mbid": "artist-1",
                        "albumartist_mbid": "artist-1",
                        "track_number": 7,
                    }
                ],
                "no_match_items": [],
            }
        },
    )
    run = load_library_tool_run(app.config["LIBRARY_INDEX_DB_PATH"], 1)
    assert run is not None

    captured: dict[str, object] = {}
    monkeypatch.setattr(
        "app.routes.web.apply_identification_metadata",
        lambda audio_path, details: captured.setdefault("applied", (str(audio_path), details)),
    )
    monkeypatch.setattr(
        "app.routes.web.refresh_library_index_for_paths",
        lambda db_path, root, audio_paths, scan_xml_sidecars=True: captured.setdefault(
            "refreshed", [str(path) for path in audio_paths]
        ),
    )

    client = app.test_client()
    response = client.post(
        "/tools/identify-audio/review/accept",
        data={
            "run_id": run["id"],
            "relative_path": "Unknown/Album/maybe.flac",
            "report_filter": "missing-xml",
        },
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b"Accepted fingerprint match" in response.data
    assert captured["refreshed"] == [str(audio_path)]
    updated_run = load_library_tool_run(app.config["LIBRARY_INDEX_DB_PATH"], run["id"])
    assert updated_run is not None
    review = updated_run["result"]["review"]
    assert review["low_confidence_count"] == 0
    assert review["low_confidence_items"] == []


def test_identify_audio_review_keep_marks_file_as_accepted_without_metadata_changes(
    tmp_path, monkeypatch
) -> None:
    app = create_app()
    app.config.update(
        TESTING=True,
        NAVIDROME_MUSIC_ROOT=str(tmp_path),
        LIBRARY_INDEX_DB_PATH=str(tmp_path / "library_index.db"),
    )
    audio_path = tmp_path / "Unknown" / "Album" / "maybe.flac"
    audio_path.parent.mkdir(parents=True, exist_ok=True)
    audio_path.write_bytes(b"audio")
    write_song_metadata_xml(audio_path, title="maybe", artist="Unknown", album="Album")
    refresh_library_index(app.config["LIBRARY_INDEX_DB_PATH"], tmp_path)
    record_library_tool_run(
        app.config["LIBRARY_INDEX_DB_PATH"],
        tool_name="identify-audio",
        root=tmp_path,
        run_mode="incremental",
        started_at="2026-04-15T12:00:00+00:00",
        completed_at="2026-04-15T12:02:00+00:00",
        scanned_count=1,
        changed_count=0,
        error_count=1,
        result={
            "review": {
                "low_confidence_count": 1,
                "no_match_count": 0,
                "recorded_count": 1,
                "truncated_count": 0,
                "low_confidence_items": [
                    {
                        "relative_path": "Unknown/Album/maybe.flac",
                        "recording_mbid": "recording-123",
                        "match_artist": "Possible Artist",
                        "match_title": "Possible Song",
                    }
                ],
                "no_match_items": [],
            }
        },
    )
    run = load_library_tool_run(app.config["LIBRARY_INDEX_DB_PATH"], 1)
    assert run is not None

    captured: dict[str, object] = {}
    monkeypatch.setattr(
        "app.routes.web.apply_identification_metadata",
        lambda audio_path, details: captured.setdefault("applied", True),
    )
    monkeypatch.setattr(
        "app.routes.web.refresh_library_index_for_paths",
        lambda *args, **kwargs: captured.setdefault("refreshed", True),
    )

    client = app.test_client()
    response = client.post(
        "/tools/identify-audio/review/keep",
        data={
            "run_id": run["id"],
            "relative_path": "Unknown/Album/maybe.flac",
            "report_filter": "missing-xml",
        },
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b"Kept Unknown/Album/maybe.flac as-is" in response.data
    assert "applied" not in captured
    assert "refreshed" not in captured
    updated_run = load_library_tool_run(app.config["LIBRARY_INDEX_DB_PATH"], run["id"])
    assert updated_run is not None
    review = updated_run["result"]["review"]
    assert review["low_confidence_count"] == 0
    assert review["low_confidence_items"] == []
    assert list_musicbrainz_tag_candidates(app.config["LIBRARY_INDEX_DB_PATH"], tmp_path) == []



def test_identify_audio_review_retry_keeps_guardrail_mismatch_in_review(
    tmp_path, monkeypatch
) -> None:
    app = create_app()
    app.config.update(
        TESTING=True,
        NAVIDROME_MUSIC_ROOT=str(tmp_path),
        LIBRARY_INDEX_DB_PATH=str(tmp_path / "library_index.db"),
        ACOUSTID_API_KEY="demo-key",
        FPCALC_BIN="fpcalc",
    )
    audio_path = tmp_path / "2 LIVE CREW" / "Banned In the USA" / "06 - Strip Club.flac"
    audio_path.parent.mkdir(parents=True, exist_ok=True)
    audio_path.write_bytes(b"audio")
    write_song_metadata_xml(
        audio_path,
        title="Strip Club",
        artist="The 2 Live Crew",
        album="Banned in the U.S.A.",
    )
    record_library_tool_run(
        app.config["LIBRARY_INDEX_DB_PATH"],
        tool_name="identify-audio",
        root=tmp_path,
        run_mode="incremental",
        started_at="2026-04-16T10:00:00+00:00",
        completed_at="2026-04-16T10:02:00+00:00",
        scanned_count=1,
        changed_count=0,
        error_count=1,
        result={
            "review": {
                "low_confidence_count": 1,
                "no_match_count": 0,
                "recorded_count": 1,
                "truncated_count": 0,
                "low_confidence_items": [
                    {
                        "relative_path": "2 LIVE CREW/Banned In the USA/06 - Strip Club.flac",
                        "reason": "guardrail",
                        "reason_label": "Similarity check",
                        "message": "Fingerprint match needs manual review.",
                    }
                ],
                "no_match_items": [],
            }
        },
    )
    run = load_library_tool_run(app.config["LIBRARY_INDEX_DB_PATH"], 1)
    assert run is not None

    class StubAcoustIdService:
        def identify_track(self, audio_path, *, musicbrainz_service=None):
            return {
                "accepted": True,
                "match": {
                    "recording_mbid": "recording-wrong",
                    "release_mbid": "release-wrong",
                    "title": "News Flash Nation by Storm",
                    "artist": "Luke The 2 Live Crew",
                    "album": "Banned in the U.S.A. The Luke LP",
                    "albumartist": "Luke The 2 Live Crew",
                    "artist_mbid": "artist-wrong",
                    "albumartist_mbid": "artist-wrong",
                    "track_number": 8,
                    "acoustid_id": "acoustid-wrong",
                    "acoustid_score": 0.99,
                },
            }

    captured: dict[str, object] = {}
    monkeypatch.setattr(
        "app.routes.web.AcoustIdService.from_config",
        lambda config: StubAcoustIdService(),
    )
    monkeypatch.setattr(
        "app.routes.web.refresh_library_index_for_paths",
        lambda db_path, root, audio_paths, scan_xml_sidecars=True: captured.setdefault(
            "refreshed", [str(path) for path in audio_paths]
        ),
    )

    client = app.test_client()
    response = client.post(
        "/tools/identify-audio/review/retry",
        data={
            "run_id": run["id"],
            "relative_path": "2 LIVE CREW/Banned In the USA/06 - Strip Club.flac",
            "report_filter": "missing-xml",
        },
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b"still needs review" in response.data
    assert "refreshed" not in captured
    updated_run = load_library_tool_run(app.config["LIBRARY_INDEX_DB_PATH"], run["id"])
    assert updated_run is not None
    review = updated_run["result"]["review"]
    assert review["low_confidence_count"] == 1
    assert review["low_confidence_items"][0]["reason"] == "guardrail"

def test_identify_audio_review_retry_rewrites_saved_candidate(tmp_path, monkeypatch) -> None:
    app = create_app()
    app.config.update(
        TESTING=True,
        NAVIDROME_MUSIC_ROOT=str(tmp_path),
        LIBRARY_INDEX_DB_PATH=str(tmp_path / "library_index.db"),
        ACOUSTID_API_KEY="demo-key",
        FPCALC_BIN="fpcalc",
    )
    audio_path = tmp_path / "Unknown" / "Album" / "missing.flac"
    audio_path.parent.mkdir(parents=True, exist_ok=True)
    audio_path.write_bytes(b"audio")
    write_song_metadata_xml(audio_path, title="missing", artist="Unknown", album="Album")
    record_library_tool_run(
        app.config["LIBRARY_INDEX_DB_PATH"],
        tool_name="identify-audio",
        root=tmp_path,
        run_mode="incremental",
        started_at="2026-04-15T12:00:00+00:00",
        completed_at="2026-04-15T12:02:00+00:00",
        scanned_count=1,
        changed_count=0,
        error_count=1,
        result={
            "review": {
                "low_confidence_count": 0,
                "no_match_count": 1,
                "recorded_count": 1,
                "truncated_count": 0,
                "low_confidence_items": [],
                "no_match_items": [
                    {
                        "relative_path": "Unknown/Album/missing.flac",
                        "message": "No AcoustID recording match was returned for this file.",
                    }
                ],
            }
        },
    )
    run = load_library_tool_run(app.config["LIBRARY_INDEX_DB_PATH"], 1)
    assert run is not None

    class StubAcoustIdService:
        def identify_track(self, audio_path, *, musicbrainz_service=None):
            return {
                "accepted": False,
                "match": {
                    "recording_mbid": "recording-low",
                    "release_mbid": "release-low",
                    "title": "Possible Song",
                    "artist": "Possible Artist",
                    "album": "Possible Album",
                    "albumartist": "Possible Artist",
                    "artist_mbid": "artist-low",
                    "albumartist_mbid": "artist-low",
                    "track_number": 4,
                    "acoustid_id": "acoustid-low",
                    "acoustid_score": 0.55,
                },
            }

    monkeypatch.setattr(
        "app.routes.web.AcoustIdService.from_config",
        lambda config: StubAcoustIdService(),
    )
    monkeypatch.setattr(
        "app.routes.web.MusicBrainzService.from_config",
        lambda config: object(),
    )
    monkeypatch.setattr(
        "app.routes.web.lookup_musicbrainz_metadata_match",
        lambda *args, **kwargs: {},
    )

    client = app.test_client()
    response = client.post(
        "/tools/identify-audio/review/retry",
        data={
            "run_id": run["id"],
            "relative_path": "Unknown/Album/missing.flac",
            "report_filter": "missing-xml",
        },
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b"still needs review" in response.data
    updated_run = load_library_tool_run(app.config["LIBRARY_INDEX_DB_PATH"], run["id"])
    assert updated_run is not None
    review = updated_run["result"]["review"]
    assert review["no_match_count"] == 0
    assert review["low_confidence_count"] == 1
    assert review["low_confidence_items"][0]["match_title"] == "Possible Song"


def test_identify_audio_review_retry_redirect_reopens_retry_result_dialog(
    tmp_path, monkeypatch
) -> None:
    app = create_app()
    app.config.update(
        TESTING=True,
        NAVIDROME_MUSIC_ROOT=str(tmp_path),
        LIBRARY_INDEX_DB_PATH=str(tmp_path / "library_index.db"),
        ACOUSTID_API_KEY="demo-key",
        FPCALC_BIN="fpcalc",
    )
    audio_path = tmp_path / "Unknown" / "Album" / "missing.flac"
    audio_path.parent.mkdir(parents=True, exist_ok=True)
    audio_path.write_bytes(b"audio")
    write_song_metadata_xml(audio_path, title="missing", artist="Unknown", album="Album")
    record_library_tool_run(
        app.config["LIBRARY_INDEX_DB_PATH"],
        tool_name="identify-audio",
        root=tmp_path,
        run_mode="incremental",
        started_at="2026-04-15T12:00:00+00:00",
        completed_at="2026-04-15T12:02:00+00:00",
        scanned_count=1,
        changed_count=0,
        error_count=1,
        result={
            "review": {
                "low_confidence_count": 0,
                "no_match_count": 1,
                "recorded_count": 1,
                "truncated_count": 0,
                "low_confidence_items": [],
                "no_match_items": [
                    {
                        "relative_path": "Unknown/Album/missing.flac",
                        "message": "No AcoustID recording match was returned for this file.",
                    }
                ],
            }
        },
    )

    class StubAcoustIdService:
        def identify_track(self, audio_path, *, musicbrainz_service=None):
            return {"accepted": False, "match": {}}

    monkeypatch.setattr(
        "app.routes.web.AcoustIdService.from_config",
        lambda config: StubAcoustIdService(),
    )
    monkeypatch.setattr(
        "app.routes.web.MusicBrainzService.from_config",
        lambda config: object(),
    )
    monkeypatch.setattr(
        "app.routes.web.lookup_musicbrainz_metadata_match",
        lambda *args, **kwargs: {},
    )

    client = app.test_client()
    response = client.post(
        "/tools/identify-audio/review/retry",
        data={
            "run_id": 1,
            "relative_path": "Unknown/Album/missing.flac",
            "report_filter": "missing-xml",
        },
        follow_redirects=False,
    )

    assert response.status_code == 302
    location = response.headers["Location"]
    assert "open_identify_review=1" in location
    assert "open_identify_retry_result=1" in location
    assert "identify_retry_path=Unknown/Album/missing.flac" in location


def test_identify_audio_review_retry_uses_musicbrainz_metadata_fallback(
    tmp_path,
    monkeypatch,
) -> None:
    app = create_app()
    app.config.update(
        TESTING=True,
        NAVIDROME_MUSIC_ROOT=str(tmp_path),
        LIBRARY_INDEX_DB_PATH=str(tmp_path / "library_index.db"),
        ACOUSTID_API_KEY="demo-key",
        FPCALC_BIN="fpcalc",
    )
    audio_path = tmp_path / "Unknown" / "Album" / "missing.flac"
    audio_path.parent.mkdir(parents=True, exist_ok=True)
    audio_path.write_bytes(b"audio")
    write_song_metadata_xml(audio_path, title="missing", artist="Unknown", album="Album")
    record_library_tool_run(
        app.config["LIBRARY_INDEX_DB_PATH"],
        tool_name="identify-audio",
        root=tmp_path,
        run_mode="incremental",
        started_at="2026-04-15T12:00:00+00:00",
        completed_at="2026-04-15T12:02:00+00:00",
        scanned_count=1,
        changed_count=0,
        error_count=1,
        result={
            "review": {
                "low_confidence_count": 0,
                "no_match_count": 1,
                "recorded_count": 1,
                "truncated_count": 0,
                "low_confidence_items": [],
                "no_match_items": [
                    {
                        "relative_path": "Unknown/Album/missing.flac",
                        "message": "No AcoustID recording match was returned for this file.",
                    }
                ],
            }
        },
    )

    class StubAcoustIdService:
        def identify_track(self, audio_path, *, musicbrainz_service=None):
            return {"accepted": False, "match": {}}

    monkeypatch.setattr(
        "app.routes.web.AcoustIdService.from_config",
        lambda config: StubAcoustIdService(),
    )
    monkeypatch.setattr(
        "app.routes.web.MusicBrainzService.from_config",
        lambda config: object(),
    )
    monkeypatch.setattr(
        "app.routes.web.lookup_musicbrainz_metadata_match",
        lambda *args, **kwargs: {
            "recording_mbid": "recording-123",
            "release_mbid": "release-456",
            "title": "Recovered Song",
            "artist": "Recovered Artist",
            "album": "Recovered Album",
            "albumartist": "Recovered Artist",
            "artist_mbid": "artist-1",
            "albumartist_mbid": "artist-1",
            "track_number": 4,
        },
    )
    monkeypatch.setattr(
        "app.routes.web.apply_identification_metadata",
        lambda *args, **kwargs: None,
    )

    client = app.test_client()
    response = client.post(
        "/tools/identify-audio/review/retry",
        data={
            "run_id": 1,
            "relative_path": "Unknown/Album/missing.flac",
            "report_filter": "missing-xml",
        },
        follow_redirects=True,
    )

    assert response.status_code == 200
    updated_run = load_library_tool_run(app.config["LIBRARY_INDEX_DB_PATH"], 1)
    assert updated_run is not None
    review = updated_run["result"]["review"]
    assert review["no_match_count"] == 0
    assert review["low_confidence_count"] == 0


def test_tools_page_handles_busy_library_index(tmp_path, monkeypatch) -> None:
    app = create_app()
    app.config.update(TESTING=True, NAVIDROME_MUSIC_ROOT=str(tmp_path))

    def raise_locked(*args, **kwargs):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr("app.routes.web.get_library_report_counts", raise_locked)

    client = app.test_client()
    response = client.get("/tools")

    assert response.status_code == 200
    assert b"Library catalog is busy right now" in response.data
    assert b"Check Audio Integrity" in response.data


def test_tools_page_does_not_refresh_catalog_on_load(tmp_path, monkeypatch) -> None:
    app = create_app()
    app.config.update(TESTING=True, NAVIDROME_MUSIC_ROOT=str(tmp_path))

    def fail_refresh(*args, **kwargs):
        raise AssertionError("refresh_library_index should not run on plain page load")

    monkeypatch.setattr("app.routes.web.refresh_library_index", fail_refresh)

    client = app.test_client()
    response = client.get("/tools")

    assert response.status_code == 200
    assert b"Refresh Catalog" in response.data
    assert b"Indexed Audio" in response.data
    assert b">0<" in response.data


def test_tools_status_endpoint_returns_current_tool_snapshot(monkeypatch) -> None:
    app = create_app()
    app.config.update(TESTING=True)
    client = app.test_client()

    monkeypatch.setattr(
        "app.routes.web.get_tool_status_snapshot",
        lambda line_limit=200: {
            "active": True,
            "primary": {
                "tool": "refresh-catalog",
                "label": "Refresh Library Catalog",
                "status": "running",
                "started_at": "2026-04-14T21:00:00+00:00",
                "completed_at": "",
                "exit_code": None,
                "dry_run": False,
                "full_scan": True,
                "limit": None,
                "line_count": 2,
                "lines": ["refresh_library_catalog", "SUMMARY indexed_audio=10"],
            },
            "tools": [],
        },
    )

    response = client.get("/tools/status?line_limit=50")

    assert response.status_code == 200
    assert response.json["active"] is True
    assert response.json["primary"]["tool"] == "refresh-catalog"
    assert response.json["primary"]["status"] == "running"
    assert response.json["primary"]["lines"][-1] == "SUMMARY indexed_audio=10"


def test_tools_status_endpoint_can_return_specific_tool_snapshot(monkeypatch) -> None:
    app = create_app()
    app.config.update(TESTING=True)
    client = app.test_client()

    monkeypatch.setattr(
        "app.routes.web.get_tool_status",
        lambda tool, line_limit=200: {
            "tool": tool,
            "label": "Refresh Library Catalog",
            "status": "done",
            "started_at": "2026-04-14T21:00:00+00:00",
            "completed_at": "2026-04-14T21:01:00+00:00",
            "exit_code": 0,
            "dry_run": False,
            "full_scan": True,
            "limit": None,
            "line_count": 2,
            "lines": ["refresh_library_catalog", "SUMMARY indexed_audio=10"],
        },
    )

    response = client.get("/tools/status?tool=refresh-catalog&line_limit=50")

    assert response.status_code == 200
    assert response.json["active"] is False
    assert response.json["primary"]["tool"] == "refresh-catalog"
    assert response.json["primary"]["status"] == "done"
    assert response.json["primary"]["lines"][-1] == "SUMMARY indexed_audio=10"


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


def test_sync_status_page_mentions_bulk_download_feedback(monkeypatch) -> None:
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
    assert b'This page will stay on the review tab while the downloads finish.' in response.data


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
    assert response.json["job"]["sync"]["summary"]["low_confidence"] == 0


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
