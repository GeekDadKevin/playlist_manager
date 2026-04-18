from __future__ import annotations

import os
from threading import Event, Thread
from typing import Any

import httpx
from flask import Flask

from app.services.deezer_download import DeezerDownloadService
from app.services.ingest import fetch_remote_jspf
from app.services.listenbrainz import ListenBrainzService
from app.services.navidrome_playlists import export_navidrome_playlist
from app.services.playlist_history import record_playlist_run
from app.services.settings_store import (
    current_schedule_key,
    load_settings,
    matches_playlist_target,
    record_run_result,
    settings_defaults_from_config,
    should_run_now,
)


def start_playlist_scheduler(app: Flask) -> None:
    if app.config.get("TESTING") or os.environ.get("PYTEST_CURRENT_TEST"):
        return
    if app.extensions.get("playlist_scheduler_started"):
        return

    stop_event = Event()
    worker = Thread(
        target=_scheduler_loop,
        args=(app, stop_event),
        daemon=True,
        name="playlist-auto-scheduler",
    )
    app.extensions["playlist_scheduler_started"] = True
    app.extensions["playlist_scheduler_stop_event"] = stop_event
    app.extensions["playlist_scheduler_thread"] = worker
    worker.start()


def run_scheduled_playlists(
    config: dict[str, Any],
    *,
    settings: dict[str, Any] | None = None,
    transport: httpx.BaseTransport | None = None,
) -> dict[str, Any]:
    settings_path = str(config.get("SETTINGS_FILE", "")).strip()
    active_settings = settings or load_settings(
        settings_path,
        default_overrides=settings_defaults_from_config(config),
    )
    run_key = current_schedule_key(active_settings)

    try:
        listenbrainz = ListenBrainzService(
            base_url=str(config.get("LISTENBRAINZ_API_BASE_URL", "https://api.listenbrainz.org")),
            username=str(config.get("LISTENBRAINZ_USERNAME", "")),
            auth_token=str(config.get("LISTENBRAINZ_AUTH_TOKEN", "")),
            playlist_type="createdfor",
            transport=transport,
        )
        if not listenbrainz.username:
            raise ValueError(
                "Set `LISTENBRAINZ_USERNAME` to enable scheduled weekly playlist imports."
            )

        title_terms = [
            str(item).strip().lower()
            for item in active_settings.get("playlist_targets", [])
            if str(item).strip()
        ]
        playlists = listenbrainz.list_playlists(count=100)
        selected_playlists = [
            playlist
            for playlist in playlists
            if playlist.get("source") == "createdfor"
            and matches_playlist_target(str(playlist.get("title", "")), title_terms)
        ]
        if not selected_playlists:
            raise ValueError(
                "No matching ListenBrainz weekly playlists were found for the configured targets."
            )

        downloader = DeezerDownloadService.from_config(config)
        navidrome_dir = str(
            config.get("NAVIDROME_PLAYLISTS_DIR") or config.get("NAVIDROME_PLAYLIST_DIR", "")
        ).strip()
        upload_folder = str(config.get("UPLOAD_FOLDER", "")).strip()
        max_tracks = int(config.get("SYNC_MAX_TRACKS", 100))
        sync_with_downloads = bool(active_settings.get("sync_with_downloads"))

        playlist_db_path = str(config.get("PLAYLIST_DB_PATH", "")).strip()
        run_results: list[dict[str, Any]] = []
        for playlist in selected_playlists:
            upload = fetch_remote_jspf(
                upload_folder,
                str(playlist.get("playlist_id") or playlist.get("jspf_url") or ""),
                listenbrainz=listenbrainz,
            )
            upload.playlist_name = str(
                playlist.get("title") or upload.playlist_name or upload.original_name
            )

            sync_mode = "export-only"
            sync_results = [{"track": track.to_dict(), "match": {}} for track in upload.tracks]
            sync_snapshot: dict[str, Any] = {"summary": {}, "results": sync_results}

            if sync_with_downloads and downloader.is_configured():
                download_result = downloader.sync_tracks(upload.tracks, max_tracks=max_tracks)
                sync_results = download_result.get("results", sync_results)
                sync_snapshot = download_result
                sync_mode = "download-sync"

            if navidrome_dir:
                low_confidence_count = int(
                    sync_snapshot.get("summary", {}).get("low_confidence", 0)
                )
                if low_confidence_count > 0:
                    export_result = {
                        "configured": True,
                        "written": False,
                        "pending_review": True,
                        "playlist_name": upload.playlist_name or upload.original_name,
                        "target_path": navidrome_dir,
                        "entry_count": int(sync_snapshot.get("summary", {}).get("processed", 0)),
                        "playable_count": int(sync_snapshot.get("summary", {}).get("downloaded", 0))
                        + int(sync_snapshot.get("summary", {}).get("already_available", 0)),
                        "missing_count": int(sync_snapshot.get("summary", {}).get("not_found", 0))
                        + int(sync_snapshot.get("summary", {}).get("failed", 0))
                        + low_confidence_count,
                        "reason": (
                            "Low-confidence tracks need manual review in the web UI "
                            "before this playlist can be committed to Navidrome."
                        ),
                    }
                else:
                    export_result = export_navidrome_playlist(
                        playlist_dir=navidrome_dir,
                        playlist_name=upload.playlist_name or upload.original_name,
                        sync_results=sync_results,
                    )
            else:
                export_result = {
                    "configured": False,
                    "written": False,
                    "playlist_name": upload.playlist_name or upload.original_name,
                    "reason": "Set `NAVIDROME_PLAYLIST_DIR` to enable Navidrome playlist export.",
                }

            history_run_id = (
                record_playlist_run(
                    playlist_db_path,
                    playlist_name=upload.playlist_name or upload.original_name,
                    source_kind=upload.source_kind,
                    original_name=upload.original_name,
                    remote_url=upload.remote_url,
                    saved_path=upload.saved_path,
                    sync_result=sync_snapshot,
                    export_result=export_result,
                )
                if playlist_db_path
                else 0
            )

            run_results.append(
                {
                    "playlist_name": upload.playlist_name or upload.original_name,
                    "playlist_id": str(playlist.get("playlist_id", "")),
                    "saved_path": upload.saved_path,
                    "track_count": upload.count,
                    "sync_mode": sync_mode,
                    "history_run_id": history_run_id,
                    "export": export_result,
                }
            )

        message = (
            f"Updated {len(run_results)} playlist(s): "
            + ", ".join(item["playlist_name"] for item in run_results)
            + "."
        )
        if settings_path:
            record_run_result(
                settings_path,
                status="success",
                message=message,
                run_key=run_key,
                results=run_results,
            )

        return {
            "playlist_count": len(run_results),
            "results": run_results,
            "message": message,
        }
    except Exception as exc:
        if settings_path:
            record_run_result(
                settings_path,
                status="failed",
                message=str(exc),
                run_key=run_key,
                results=[],
            )
        raise


def _scheduler_loop(app: Flask, stop_event: Event) -> None:
    while not stop_event.wait(30.0):
        with app.app_context():
            settings = load_settings(
                app.config["SETTINGS_FILE"],
                default_overrides=settings_defaults_from_config(app.config),
            )
            if not should_run_now(settings):
                continue

            try:
                run_scheduled_playlists(app.config, settings=settings)
            except Exception:
                app.logger.exception("Scheduled weekly playlist automation failed.")
