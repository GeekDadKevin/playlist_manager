from __future__ import annotations

import json

import httpx
from app import create_app
from app.services.playlist_history import record_playlist_run
from app.services.scheduled_imports import run_scheduled_playlists
from app.services.settings_store import cron_expression, load_settings, save_settings


def test_settings_page_persists_theme_and_schedule(tmp_path) -> None:
    app = create_app()
    app.config.update(
        TESTING=True,
        DATA_DIR=str(tmp_path),
        SETTINGS_FILE=str(tmp_path / "settings.json"),
    )
    client = app.test_client()

    response = client.post(
        "/settings",
        data={
            "theme": "light",
            "automation_enabled": "on",
            "schedule_day": "friday",
            "schedule_time": "07:30",
            "playlist_targets": "Weekly Exploration, Weekly Jams",
            "sync_with_downloads": "on",
            "action": "save",
        },
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b"Settings saved" in response.data
    assert b"Settings" in response.data
    assert b'data-theme="light"' in response.data

    saved = load_settings(app.config["SETTINGS_FILE"])
    assert saved["theme"] == "light"
    assert saved["automation_enabled"] is True
    assert saved["schedule_day"] == "friday"
    assert saved["schedule_time"] == "07:30"
    assert saved["playlist_targets"] == ["weekly exploration", "weekly jams"]
    assert saved["sync_with_downloads"] is True
    assert cron_expression(saved) == "30 7 * * 5"


def test_settings_page_uses_config_defaults_for_fallback_toggles(tmp_path) -> None:
    app = create_app()
    app.config.update(
        TESTING=True,
        DATA_DIR=str(tmp_path),
        SETTINGS_FILE=str(tmp_path / "settings.json"),
        SOUNDCLOUD_FALLBACK_ENABLED="0",
        YOUTUBE_FALLBACK_ENABLED="1",
        DOWNLOAD_THREADS=3,
    )
    (tmp_path / "settings.json").write_text(json.dumps({"theme": "dark"}), encoding="utf-8")

    client = app.test_client()
    response = client.get("/settings")

    assert response.status_code == 200
    assert b'name="soundcloud_fallback" checked' not in response.data
    assert b'name="youtube_fallback" checked' in response.data
    assert b'name="download_threads" min="1" max="8" value="3"' in response.data


def test_settings_page_can_uncheck_fallback_toggles(tmp_path) -> None:
    app = create_app()
    app.config.update(
        TESTING=True,
        DATA_DIR=str(tmp_path),
        SETTINGS_FILE=str(tmp_path / "settings.json"),
    )
    save_settings(
        app.config["SETTINGS_FILE"],
        {
            "soundcloud_fallback": True,
            "youtube_fallback": True,
            "automation_enabled": True,
            "sync_with_downloads": True,
        },
    )

    client = app.test_client()
    response = client.post(
        "/settings",
        data={
            "theme": "dark",
            "schedule_day": "monday",
            "schedule_time": "06:00",
            "playlist_targets": "Weekly Exploration, Weekly Jams",
            "download_threads": "1",
            "action": "save",
        },
        follow_redirects=True,
    )

    assert response.status_code == 200
    saved = load_settings(app.config["SETTINGS_FILE"])
    assert saved["soundcloud_fallback"] is False
    assert saved["youtube_fallback"] is False
    assert saved["automation_enabled"] is False
    assert saved["sync_with_downloads"] is False


def test_index_keeps_weekly_targets_visible_even_if_previously_imported(
    tmp_path,
    monkeypatch,
) -> None:
    app = create_app()
    upload_folder = tmp_path / "uploads"
    app.config.update(
        TESTING=True,
        UPLOAD_FOLDER=str(upload_folder),
        SETTINGS_FILE=str(tmp_path / "settings.json"),
        PLAYLIST_DB_PATH=str(tmp_path / "playlist_history.db"),
        LISTENBRAINZ_USERNAME="demo",
    )
    save_settings(
        app.config["SETTINGS_FILE"],
        {"playlist_targets": "Weekly Exploration, Weekly Jams"},
    )

    record_playlist_run(
        app.config["PLAYLIST_DB_PATH"],
        playlist_name=(
            "Weekly Exploration for geekdadkevin, "
            "week of 2026-04-06 Mon"
        ),
        source_kind="remote-jspf",
        original_name="listenbrainz.jspf",
        remote_url=(
            "https://listenbrainz.org/playlist/"
            "11111111-1111-1111-1111-111111111111/export/jspf"
        ),
        saved_path="memory://weekly-exploration",
        sync_result={"summary": {"requested": 1, "processed": 1}, "results": []},
        export_result={
            "written": True,
            "target_path": str(tmp_path / "weekly-exploration.m3u"),
        },
    )
    record_playlist_run(
        app.config["PLAYLIST_DB_PATH"],
        playlist_name="Daily Mix for geekdadkevin, day of 2026-04-06 Mon",
        source_kind="remote-jspf",
        original_name="listenbrainz.jspf",
        remote_url=(
            "https://listenbrainz.org/playlist/"
            "33333333-3333-3333-3333-333333333333/export/jspf"
        ),
        saved_path="memory://daily-mix",
        sync_result={"summary": {"requested": 1, "processed": 1}, "results": []},
        export_result={
            "written": True,
            "target_path": str(tmp_path / "daily-mix.m3u"),
        },
    )

    monkeypatch.setattr(
        "app.routes.web.ListenBrainzService.list_playlists",
        lambda self, exclude_playlist_ids=None: [
            {
                "title": "Weekly Exploration for geekdadkevin, week of 2026-04-06 Mon",
                "source": "createdfor",
                "source_label": "Created For You",
                "playlist_id": "11111111-1111-1111-1111-111111111111",
                "jspf_url": "https://listenbrainz.org/playlist/11111111-1111-1111-1111-111111111111/export/jspf",
                "selected": False,
            },
            {
                "title": "Weekly Jams for geekdadkevin, week of 2026-04-06 Mon",
                "source": "createdfor",
                "source_label": "Created For You",
                "playlist_id": "22222222-2222-2222-2222-222222222222",
                "jspf_url": "https://listenbrainz.org/playlist/22222222-2222-2222-2222-222222222222/export/jspf",
                "selected": False,
            },
            {
                "title": "Daily Mix for geekdadkevin, day of 2026-04-06 Mon",
                "source": "createdfor",
                "source_label": "Created For You",
                "playlist_id": "33333333-3333-3333-3333-333333333333",
                "jspf_url": "https://listenbrainz.org/playlist/33333333-3333-3333-3333-333333333333/export/jspf",
                "selected": False,
            },
        ],
    )

    client = app.test_client()
    response = client.get("/")

    assert response.status_code == 200
    assert b"Weekly Exploration" in response.data
    assert b"Weekly Jams" in response.data
    assert b"Daily Mix" in response.data


def test_run_scheduled_playlists_filters_to_weekly_targets(tmp_path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/1/user/demo/playlists/createdfor":
            return httpx.Response(
                200,
                json={
                    "payload": {
                        "playlists": [
                            {
                                "title": (
                                    "Weekly Exploration for geekdadkevin, week of 2026-04-06 Mon"
                                ),
                                "identifier": (
                                    "https://listenbrainz.org/playlist/"
                                    "11111111-1111-1111-1111-111111111111"
                                ),
                            },
                            {
                                "title": "Weekly Jams for geekdadkevin, week of 2026-04-06 Mon",
                                "identifier": (
                                    "https://listenbrainz.org/playlist/"
                                    "22222222-2222-2222-2222-222222222222"
                                ),
                            },
                            {
                                "title": "Daily Mix for geekdadkevin, day of 2026-04-06 Mon",
                                "identifier": (
                                    "https://listenbrainz.org/playlist/"
                                    "33333333-3333-3333-3333-333333333333"
                                ),
                            },
                        ]
                    }
                },
            )
        if request.url.path == "/1/user/demo/playlists":
            return httpx.Response(200, json={"payload": {"playlists": []}})
        if request.url.path == "/1/playlist/11111111-1111-1111-1111-111111111111":
            return httpx.Response(
                200,
                json={
                    "playlist": {
                        "title": "Weekly Exploration for geekdadkevin, week of 2026-04-06 Mon",
                        "track": [
                            {
                                "title": "Teardrop",
                                "creator": "Massive Attack",
                                "identifier": "Massive Attack/Mezzanine/Teardrop.flac",
                            }
                        ],
                    }
                },
            )
        if request.url.path == "/1/playlist/22222222-2222-2222-2222-222222222222":
            return httpx.Response(
                200,
                json={
                    "playlist": {
                        "title": "Weekly Jams for geekdadkevin, week of 2026-04-06 Mon",
                        "track": [
                            {
                                "title": "Windowlicker",
                                "creator": "Aphex Twin",
                                "identifier": "Aphex Twin/Windowlicker/Windowlicker.flac",
                            }
                        ],
                    }
                },
            )
        raise AssertionError(f"Unexpected request: {request.url}")

    app = create_app()
    app.config.update(
        TESTING=True,
        LISTENBRAINZ_USERNAME="demo",
        NAVIDROME_PLAYLISTS_DIR=str(tmp_path / "navidrome"),
        UPLOAD_FOLDER=str(tmp_path / "uploads"),
        SETTINGS_FILE=str(tmp_path / "settings.json"),
    )

    with app.app_context():
        result = run_scheduled_playlists(
            app.config,
            transport=httpx.MockTransport(handler),
        )

    assert result["playlist_count"] == 2
    assert [item["playlist_name"] for item in result["results"]] == [
        "Weekly Exploration for geekdadkevin, week of 2026-04-06 Mon",
        "Weekly Jams for geekdadkevin, week of 2026-04-06 Mon",
    ]
    assert (tmp_path / "navidrome" / "Weekly Exploration.m3u").exists()
    assert (tmp_path / "navidrome" / "Weekly Jams.m3u").exists()
    assert not (tmp_path / "navidrome" / "daily-mix.m3u").exists()
