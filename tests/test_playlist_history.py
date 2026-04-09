from __future__ import annotations

from app import create_app
from app.services.playlist_history import (
    export_playlist_from_history,
    init_playlist_history,
    record_playlist_run,
)


def test_settings_page_lists_tracked_playlists_and_stats(tmp_path) -> None:
    app = create_app()
    app.config.update(
        TESTING=True,
        SETTINGS_FILE=str(tmp_path / "settings.json"),
        PLAYLIST_DB_PATH=str(tmp_path / "playlist_history.db"),
        NAVIDROME_PLAYLISTS_DIR=str(tmp_path / "navidrome"),
    )
    init_playlist_history(app.config["PLAYLIST_DB_PATH"])
    record_playlist_run(
        app.config["PLAYLIST_DB_PATH"],
        playlist_name="Weekly Exploration for geekdadkevin, week of 2026-04-06 Mon",
        source_kind="remote-jspf",
        original_name="listenbrainz.jspf",
        remote_url="https://listenbrainz.org/playlist/11111111-1111-1111-1111-111111111111/export/jspf",
        saved_path=str(tmp_path / "uploads" / "listenbrainz-1111.jspf"),
        sync_result={
            "summary": {
                "requested": 3,
                "processed": 3,
                "downloaded": 1,
                "already_available": 1,
                "failed": 1,
                "preview": 0,
                "low_confidence": 0,
                "not_found": 0,
            },
            "results": [
                {
                    "index": 1,
                    "status": "downloaded",
                    "track": {
                        "title": "Teardrop",
                        "artist": "Massive Attack",
                        "album": "Mezzanine",
                        "source": "https://musicbrainz.org/recording/teardrop",
                    },
                    "match": {"id": "ext-deezer-track-1", "score": 98},
                    "resolved_match": {"path": "Massive Attack/Mezzanine/Teardrop.flac"},
                },
                {
                    "index": 2,
                    "status": "already_available",
                    "track": {
                        "title": "Windowlicker",
                        "artist": "Aphex Twin",
                        "album": "Windowlicker",
                        "source": "Aphex Twin/Windowlicker/Windowlicker.flac",
                    },
                    "match": {
                        "id": "track-2",
                        "score": 96,
                        "path": "Aphex Twin/Windowlicker/Windowlicker.flac",
                    },
                },
                {
                    "index": 3,
                    "status": "failed",
                    "track": {
                        "title": "Unknown Song",
                        "artist": "Unknown Artist",
                        "source": "https://musicbrainz.org/recording/unknown",
                    },
                    "match": {"id": "ext-deezer-track-3", "score": 70},
                },
            ],
        },
        export_result={
            "written": True,
            "target_path": str(tmp_path / "navidrome" / "weekly-exploration.m3u"),
            "entry_count": 3,
            "playable_count": 2,
            "missing_count": 1,
        },
    )

    client = app.test_client()
    settings_response = client.get("/settings")
    stats_response = client.get("/stats")

    assert settings_response.status_code == 200
    assert b"Tracked playlists" in settings_response.data
    assert b"Weekly Exploration" in settings_response.data
    assert b"Send to Navidrome now" in settings_response.data

    assert stats_response.status_code == 200
    assert b"Playlist stats" in stats_response.data
    assert b"Total playlists tracked" in stats_response.data
    assert b"Massive Attack" in stats_response.data


def test_reexport_playlist_from_history_to_navidrome_dir(tmp_path) -> None:
    app = create_app()
    app.config.update(
        TESTING=True,
        SETTINGS_FILE=str(tmp_path / "settings.json"),
        PLAYLIST_DB_PATH=str(tmp_path / "playlist_history.db"),
        NAVIDROME_PLAYLISTS_DIR=str(tmp_path / "navidrome"),
    )
    init_playlist_history(app.config["PLAYLIST_DB_PATH"])
    run_id = record_playlist_run(
        app.config["PLAYLIST_DB_PATH"],
        playlist_name="Weekly Jams for geekdadkevin, week of 2026-04-06 Mon",
        source_kind="remote-jspf",
        original_name="listenbrainz.jspf",
        remote_url="https://listenbrainz.org/playlist/22222222-2222-2222-2222-222222222222/export/jspf",
        saved_path=str(tmp_path / "uploads" / "listenbrainz-2222.jspf"),
        sync_result={
            "summary": {
                "requested": 1,
                "processed": 1,
                "downloaded": 1,
                "already_available": 0,
                "failed": 0,
                "preview": 0,
                "low_confidence": 0,
                "not_found": 0,
            },
            "results": [
                {
                    "index": 1,
                    "status": "downloaded",
                    "track": {
                        "title": "Windowlicker",
                        "artist": "Aphex Twin",
                        "album": "Windowlicker",
                        "source": "Aphex Twin/Windowlicker/Windowlicker.flac",
                    },
                    "resolved_match": {"path": "Aphex Twin/Windowlicker/Windowlicker.flac"},
                    "match": {"id": "ext-deezer-track-4", "score": 97},
                }
            ],
        },
        export_result={
            "written": True,
            "target_path": str(tmp_path / "navidrome" / "Weekly Jams.m3u"),
            "entry_count": 1,
            "playable_count": 1,
            "missing_count": 0,
        },
    )

    client = app.test_client()
    response = client.post(
        "/history/export",
        data={"run_id": str(run_id)},
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b"Navidrome playlist updated" in response.data
    assert (tmp_path / "navidrome" / "Weekly Jams.m3u").exists()


def test_playlist_history_reuses_saved_local_path_for_reexport(tmp_path) -> None:
    db_path = tmp_path / "playlist_history.db"
    playlist_dir = tmp_path / "navidrome"
    init_playlist_history(db_path)

    record_playlist_run(
        db_path,
        playlist_name="Library cache",
        source_kind="manual",
        original_name="library-cache.m3u",
        saved_path="memory://library-cache",
        sync_result={
            "summary": {"requested": 1, "processed": 1, "already_available": 1},
            "results": [
                {
                    "status": "already_available",
                    "track": {
                        "title": "Hail to the King",
                        "artist": "Avenged Sevenfold",
                        "album": "Hail to the King",
                        "source": (
                            "/app/downloads/Avenged Sevenfold/"
                            "Hail to the King/02 - Hail to the King.flac"
                        ),
                    },
                    "resolved_match": {
                        "path": (
                            "/app/downloads/Avenged Sevenfold/"
                            "Hail to the King/02 - Hail to the King.flac"
                        )
                    },
                    "match": {"id": "local-song-1", "provider": "navidrome", "score": 99},
                }
            ],
        },
        export_result={
            "written": True,
            "target_path": str(playlist_dir / "library-cache.m3u"),
            "entry_count": 1,
            "playable_count": 1,
            "missing_count": 0,
        },
    )

    run_id = record_playlist_run(
        db_path,
        playlist_name="Weekly Exploration for geekdadkevin, week of 2026-04-06 Mon",
        source_kind="remote-jspf",
        original_name="listenbrainz.jspf",
        remote_url="https://listenbrainz.org/playlist/11111111-1111-1111-1111-111111111111/export/jspf",
        saved_path="memory://weekly-exploration",
        sync_result={
            "summary": {"requested": 1, "processed": 1, "downloaded": 1},
            "results": [
                {
                    "status": "downloaded",
                    "track": {
                        "title": "Hail to the King",
                        "artist": "Avenged Sevenfold",
                        "album": "Hail to the King",
                        "source": "https://musicbrainz.org/recording/hail-to-the-king",
                    },
                    "match": {"id": "ext-deezer-song-12345", "provider": "deezer", "score": 98},
                }
            ],
        },
        export_result={
            "written": True,
            "target_path": str(playlist_dir / "Weekly Exploration.m3u"),
            "entry_count": 1,
            "playable_count": 1,
            "missing_count": 0,
        },
    )

    export_result = export_playlist_from_history(
        db_path,
        run_id=run_id,
        playlist_dir=playlist_dir,
    )

    assert export_result["written"] is True
    assert export_result["missing_count"] == 0

    written = (playlist_dir / "Weekly Exploration.m3u").read_text(encoding="utf-8")
    assert "../Avenged Sevenfold/Hail to the King/02 - Hail to the King.flac" in written
