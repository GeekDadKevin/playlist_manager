from __future__ import annotations

import logging
import sqlite3
import sys
import time
from pathlib import Path

from flask import (
    Blueprint,
    current_app,
    flash,
    make_response,
    redirect,
    render_template,
    request,
    send_from_directory,
    session,
    url_for,
)
from flask.typing import ResponseReturnValue

from app.matching import build_search_queries
from app.services.audio_health import find_ffmpeg_executable
from app.services.audio_identification import (
    AcoustIdService,
    find_fpcalc_executable,
    lookup_musicbrainz_metadata_match,
)
from app.services.audio_identification_review import (
    apply_identification_metadata,
    build_review_item,
    fingerprint_guardrail_assessment,
    review_item_to_details,
)
from app.services.deezer_download import DeezerDownloadService
from app.services.ingest import (
    fetch_remote_jspf,
    find_imported_listenbrainz_playlist_ids,
    load_saved_playlist,
    save_uploaded_playlist,
)
from app.services.library_catalog import (
    CATALOG_FILTERS,
    CATALOG_PAGE_SIZE,
    catalog_batch_action_label,
    catalog_batch_actions,
    catalog_filter_counts,
    list_catalog_tracks,
    load_last_catalog_batch_result,
    run_catalog_batch_action,
)
from app.services.library_index import (
    get_library_report_counts,
    list_library_report_items,
    load_latest_library_tool_run,
    load_library_tool_run,
    record_musicbrainz_verification,
    refresh_library_index,
    refresh_library_index_for_paths,
    update_identify_audio_review_status,
    update_library_tool_run_result,
)
from app.services.listenbrainz import ListenBrainzService
from app.services.musicbrainz import MusicBrainzService
from app.services.navidrome_playlists import export_navidrome_playlist
from app.services.playlist_history import (
    export_playlist_from_history,
    get_playlist_stats,
    record_playlist_run,
)
from app.services.scheduled_imports import run_scheduled_playlists
from app.services.settings_store import (
    DAY_NAMES,
    cron_expression,
    load_settings,
    matches_playlist_target,
    settings_defaults_from_config,
)
from app.services.settings_store import (
    save_settings as save_app_settings,
)
from app.services.sync_jobs import (
    download_selected_low_confidence_candidates,
    export_sync_job_playlist,
    get_sync_job,
    resolve_low_confidence_candidate,
    search_low_confidence_candidates,
    skip_all_low_confidence_candidates,
    skip_low_confidence_candidate,
    start_sync_job,
)

web_bp = Blueprint("web", __name__)
log = logging.getLogger(__name__)


@web_bp.get("/favicon.ico")
def favicon() -> ResponseReturnValue:
    return send_from_directory(current_app.static_folder, "favicon.svg", mimetype="image/svg+xml")


@web_bp.app_context_processor
def inject_app_navigation() -> dict[str, object]:
    active_review_saved_path = str(session.get("active_review_saved_path", "")).strip()
    active_sync_job_id = str(session.get("active_sync_job_id", "")).strip()
    active_sync_job = get_sync_job(active_sync_job_id) if active_sync_job_id else None
    app_settings = load_settings(current_app.config["SETTINGS_FILE"])

    if active_sync_job_id and active_sync_job is None:
        session.pop("active_sync_job_id", None)
        active_sync_job_id = ""

    return {
        "active_review_saved_path": active_review_saved_path,
        "active_review_url": (
            url_for("web.review_page", saved_path=active_review_saved_path)
            if active_review_saved_path
            else ""
        ),
        "active_sync_job_id": active_sync_job_id,
        "active_sync_job": active_sync_job,
        "active_sync_job_url": (
            url_for("web.sync_status_page", job_id=active_sync_job_id) if active_sync_job_id else ""
        ),
        "active_sync_status": active_sync_job.get("status", "") if active_sync_job else "",
        "app_theme": app_settings.get("theme", "dark"),
    }


@web_bp.get("/")
def index() -> str:
    listenbrainz = ListenBrainzService.from_config(current_app.config)
    listenbrainz_playlists: list[dict[str, str]] = []
    listenbrainz_error = ""
    show_imported = request.args.get("show_imported", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    app_settings = load_settings(current_app.config["SETTINGS_FILE"])
    playlist_targets = list(app_settings.get("playlist_targets", []))
    imported_listenbrainz_ids = find_imported_listenbrainz_playlist_ids(
        current_app.config["UPLOAD_FOLDER"],
        current_app.config.get("PLAYLIST_DB_PATH"),
    )
    hidden_imported_count = 0

    if listenbrainz.is_configured():
        try:
            fetched_playlists = listenbrainz.list_playlists(exclude_playlist_ids=None)
            if show_imported:
                listenbrainz_playlists = fetched_playlists
            else:
                for playlist in fetched_playlists:
                    playlist_id = str(playlist.get("playlist_id", "")).lower()
                    playlist_title = str(playlist.get("title", ""))
                    playlist_source = str(playlist.get("source", "")).strip().lower()
                    is_imported = bool(playlist_id and playlist_id in imported_listenbrainz_ids)
                    keep_visible = playlist_source == "createdfor" or matches_playlist_target(
                        playlist_title,
                        playlist_targets,
                    )
                    if is_imported and not keep_visible:
                        hidden_imported_count += 1
                        continue
                    listenbrainz_playlists.append(playlist)
        except Exception as exc:  # pragma: no cover - network dependent
            listenbrainz_error = str(exc)

    return render_template(
        "index.html",
        configured_jspf_url="",
        listenbrainz_ready=listenbrainz.is_configured(),
        listenbrainz_playlists=listenbrainz_playlists,
        listenbrainz_error=listenbrainz_error,
        listenbrainz_hidden_count=hidden_imported_count,
        listenbrainz_show_imported=show_imported,
        listenbrainz_username=current_app.config.get("LISTENBRAINZ_USERNAME", ""),
        selected_listenbrainz_playlist="",
    )


@web_bp.route("/settings", methods=["GET", "POST"])
def settings_page() -> ResponseReturnValue:
    settings_defaults = settings_defaults_from_config(current_app.config)
    if request.method == "POST":
        existing_settings = load_settings(
            current_app.config["SETTINGS_FILE"],
            default_overrides=settings_defaults,
        )
        updated_settings = save_app_settings(
            current_app.config["SETTINGS_FILE"],
            {
                **existing_settings,
                "theme": request.form.get("theme", existing_settings.get("theme", "dark")),
                "automation_enabled": "automation_enabled" in request.form,
                "schedule_day": request.form.get(
                    "schedule_day", existing_settings.get("schedule_day", "monday")
                ),
                "schedule_time": request.form.get(
                    "schedule_time", existing_settings.get("schedule_time", "06:00")
                ),
                "playlist_targets": request.form.get(
                    "playlist_targets",
                    ", ".join(existing_settings.get("playlist_targets", [])),
                ),
                "sync_with_downloads": "sync_with_downloads" in request.form,
                "soundcloud_fallback": "soundcloud_fallback" in request.form,
                "youtube_fallback": "youtube_fallback" in request.form,
                "download_threads": request.form.get(
                    "download_threads",
                    existing_settings.get("download_threads", 1),
                ),
            },
            default_overrides=settings_defaults,
        )

        action = request.form.get("action", "save")
        if action == "run_now":
            try:
                result = run_scheduled_playlists(current_app.config, settings=updated_settings)
            except Exception as exc:  # pragma: no cover - network/runtime dependent
                flash(f"Scheduled weekly import failed: {exc}", "error")
            else:
                flash(result["message"], "success")
        else:
            flash("Settings saved.", "success")

        return redirect(url_for("web.settings_page"))

    settings_path = Path(current_app.config["SETTINGS_FILE"])
    settings = load_settings(settings_path, default_overrides=settings_defaults)
    history = get_playlist_stats(current_app.config["PLAYLIST_DB_PATH"])
    return render_template(
        "settings.html",
        settings=settings,
        theme_options=[("dark", "Dark"), ("light", "Light"), ("system", "System")],
        schedule_days=[(day, day.title()) for day in DAY_NAMES],
        cron_preview=cron_expression(settings),
        tracked_playlists=history["tracked_playlists"],
        stats_summary=history["summary"],
    )


@web_bp.post("/history/export")
def export_history_playlist() -> ResponseReturnValue:
    run_id = request.form.get("run_id", type=int)
    if not run_id:
        flash("Choose a tracked playlist to export first.", "error")
        return redirect(url_for("web.settings_page"))

    playlist_dir = str(
        current_app.config.get("NAVIDROME_PLAYLISTS_DIR")
        or current_app.config.get("NAVIDROME_PLAYLIST_DIR", "")
    ).strip()
    if not playlist_dir:
        flash("Set `NAVIDROME_PLAYLIST_DIR` to enable Navidrome playlist export.", "error")
        return redirect(url_for("web.settings_page"))

    try:
        export_result = export_playlist_from_history(
            current_app.config["PLAYLIST_DB_PATH"],
            run_id=run_id,
            playlist_dir=playlist_dir,
        )
    except Exception as exc:  # pragma: no cover - runtime/filesystem dependent
        flash(f"Could not update Navidrome playlist: {exc}", "error")
    else:
        flash(
            f"Navidrome playlist updated: {export_result['filename']} "
            f"({export_result['entry_count']} track(s)).",
            "success",
        )

    return redirect(url_for("web.settings_page"))


@web_bp.get("/stats")
def stats_page() -> str:
    history = get_playlist_stats(current_app.config["PLAYLIST_DB_PATH"])
    return render_template(
        "stats.html",
        history=history,
        summary=history["summary"],
    )


@web_bp.get("/review")
def review_page() -> ResponseReturnValue:
    saved_path = (
        request.args.get("saved_path", "").strip()
        or str(session.get("active_review_saved_path", "")).strip()
    )
    if not saved_path:
        flash("Import a playlist first to review it.", "error")
        return redirect(url_for("web.index"))

    try:
        upload = load_saved_playlist(current_app.config["UPLOAD_FOLDER"], saved_path)
    except Exception as exc:  # pragma: no cover - user-facing recovery
        if session.get("active_review_saved_path") == saved_path:
            session.pop("active_review_saved_path", None)
        flash(f"Could not load saved playlist: {exc}", "error")
        return redirect(url_for("web.index"))

    session["active_review_saved_path"] = upload.saved_path
    return _render_review_page(upload)


@web_bp.post("/review")
def review() -> ResponseReturnValue:
    uploaded = request.files.get("playlist_file")
    manual_jspf_url = request.form.get("jspf_url", "").strip()
    selected_playlist = request.form.get("listenbrainz_playlist_id", "").strip()
    listenbrainz = ListenBrainzService.from_config(current_app.config)
    jspf_url = manual_jspf_url or selected_playlist

    try:
        if uploaded and uploaded.filename:
            result = save_uploaded_playlist(
                current_app.config["UPLOAD_FOLDER"],
                uploaded.filename,
                uploaded.read(),
            )
        elif jspf_url or listenbrainz.is_configured():
            result = fetch_remote_jspf(
                current_app.config["UPLOAD_FOLDER"],
                jspf_url,
                listenbrainz=listenbrainz,
            )
        else:
            flash(
                "Upload an M3U, JSPF, or Navidrome missing-files CSV, "
                "or configure ListenBrainz in `.env`.",
                "error",
            )
            return redirect(url_for("web.index"))
    except Exception as exc:  # pragma: no cover - surfaced to UI
        flash(f"Could not parse playlist: {exc}", "error")
        return redirect(url_for("web.index"))

    session["active_review_saved_path"] = result.saved_path

    try:
        job_id = _start_sync_for_upload(result, max_tracks=current_app.config["SYNC_MAX_TRACKS"])
    except Exception as exc:  # pragma: no cover - runtime and network dependent
        flash(f"Playlist imported, but live sync could not start: {exc}", "error")
        return redirect(url_for("web.review_page", saved_path=result.saved_path))

    flash("Playlist imported. Live sync started automatically.", "success")
    return redirect(url_for("web.sync_status_page", job_id=job_id))


def _render_review_page(upload) -> str:
    deezer = DeezerDownloadService.from_config(current_app.config)
    preview_rows = []
    for track in upload.tracks[:50]:
        preview_rows.append(
            {
                **track.to_dict(),
                "queries": build_search_queries(track),
            }
        )

    return render_template(
        "review.html",
        upload=upload.to_dict(),
        tracks=preview_rows,
        total_tracks=upload.count,
        truncated=max(0, upload.count - len(preview_rows)),
        download_configured=deezer.is_configured(),
        sync_max_tracks=current_app.config["SYNC_MAX_TRACKS"],
    )


def _build_export_only_snapshot(upload) -> dict[str, object]:
    summary = {
        "requested": 0,
        "processed": 0,
        "preview": 0,
        "downloaded": 0,
        "already_available": 0,
        "low_confidence": 0,
        "not_found": 0,
        "failed": 0,
    }
    results: list[dict[str, object]] = []

    for index, track in enumerate(upload.tracks, start=1):
        track_dict = track.to_dict()
        source = str(track_dict.get("source", "")).replace("\\", "/").strip()
        is_local_path = bool(source) and not source.lower().startswith(("http://", "https://"))
        status = "already_available" if is_local_path else "not_found"
        item = {
            "index": index,
            "status": status,
            "track": track_dict,
            "match": {},
        }
        results.append(item)
        summary[status] += 1

    summary["requested"] = len(results)
    summary["processed"] = len(results)
    return {"summary": summary, "results": results}


@web_bp.post("/navidrome/export")
def export_playlist() -> ResponseReturnValue:
    saved_path = request.form.get("saved_path", "").strip()
    playlist_name = request.form.get("playlist_name", "").strip()

    try:
        upload = load_saved_playlist(current_app.config["UPLOAD_FOLDER"], saved_path)
        if playlist_name:
            upload.playlist_name = playlist_name

        session["active_review_saved_path"] = upload.saved_path
        playlist_dir = str(
            current_app.config.get("NAVIDROME_PLAYLISTS_DIR")
            or current_app.config.get("NAVIDROME_PLAYLIST_DIR", "")
        ).strip()
        if not playlist_dir:
            raise ValueError("Set `NAVIDROME_PLAYLIST_DIR` to enable Navidrome playlist export.")

        sync_snapshot = _build_export_only_snapshot(upload)
        export_result = export_navidrome_playlist(
            playlist_dir=playlist_dir,
            playlist_name=upload.playlist_name or upload.original_name,
            sync_results=sync_snapshot["results"],
        )
        record_playlist_run(
            current_app.config["PLAYLIST_DB_PATH"],
            playlist_name=upload.playlist_name or upload.original_name,
            source_kind=upload.source_kind,
            original_name=upload.original_name,
            remote_url=upload.remote_url,
            saved_path=upload.saved_path,
            sync_result=sync_snapshot,
            export_result=export_result,
        )
    except Exception as exc:  # pragma: no cover - runtime/filesystem dependent
        flash(f"Could not update Navidrome playlist: {exc}", "error")
        return redirect(url_for("web.review_page", saved_path=saved_path))

    if export_result.get("written"):
        message = (
            f"Navidrome playlist updated: {export_result['filename']} "
            f"({export_result['entry_count']} track(s))."
        )
        if export_result.get("missing_count"):
            message += f" {export_result['missing_count']} still pending download."
        flash(message, "success")
    else:
        flash(export_result.get("reason", "Navidrome playlist was not written."), "error")

    return redirect(url_for("web.review_page", saved_path=upload.saved_path))


@web_bp.post("/sync")
def sync_upload() -> ResponseReturnValue:
    saved_path = request.form.get("saved_path", "")
    playlist_name = request.form.get("playlist_name", "").strip()
    max_tracks = request.form.get("max_tracks", type=int) or current_app.config["SYNC_MAX_TRACKS"]

    try:
        upload = load_saved_playlist(current_app.config["UPLOAD_FOLDER"], saved_path)
        if playlist_name:
            upload.playlist_name = playlist_name
        session["active_review_saved_path"] = upload.saved_path
        job_id = _start_sync_for_upload(upload, max_tracks=max_tracks)
    except Exception as exc:  # pragma: no cover - runtime and network dependent
        flash(f"Sync failed: {exc}", "error")
        return redirect(url_for("web.index"))

    return redirect(url_for("web.sync_status_page", job_id=job_id))


def _start_sync_for_upload(upload, *, max_tracks: int) -> str:
    settings_path = Path(current_app.config["SETTINGS_FILE"])
    app_settings = load_settings(
        settings_path,
        default_overrides=settings_defaults_from_config(current_app.config),
    )
    sync_config = {
        **current_app.config,
        "SOUNDCLOUD_FALLBACK_ENABLED": app_settings.get("soundcloud_fallback", True),
        "YOUTUBE_FALLBACK_ENABLED": app_settings.get("youtube_fallback", False),
        "DOWNLOAD_THREADS": app_settings.get("download_threads", 1),
    }
    service = DeezerDownloadService.from_config(sync_config)
    if not service.is_configured():
        raise ValueError(
            "Configure `DEEZER_ARL` and `NAVIDROME_MUSIC_ROOT` to enable live downloads. "
            "SoundCloud and YouTube are only used during low-confidence review."
        )

    job_id = start_sync_job(
        upload,
        service,
        max_tracks=max_tracks,
        navidrome_playlists_dir=str(
            current_app.config.get("NAVIDROME_PLAYLISTS_DIR")
            or current_app.config.get("NAVIDROME_PLAYLIST_DIR", "")
        ).strip(),
        playlist_db_path=str(current_app.config.get("PLAYLIST_DB_PATH", "")).strip(),
    )
    session["active_sync_job_id"] = job_id
    return job_id


def _wants_json_response() -> bool:
    if request.headers.get("X-Requested-With", "").strip().lower() == "xmlhttprequest":
        return True
    best_match = request.accept_mimetypes.best_match(["application/json", "text/html"])
    return best_match == "application/json"


def _bool_config(value: object) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


@web_bp.get("/sync/<job_id>")
def sync_status_page(job_id: str) -> ResponseReturnValue:
    job = get_sync_job(job_id)
    if job is None:
        if session.get("active_sync_job_id") == job_id:
            session.pop("active_sync_job_id", None)
        flash("That sync job could not be found.", "error")
        return redirect(url_for("web.index"))

    session["active_sync_job_id"] = job_id
    return render_template("sync.html", job=job)


@web_bp.get("/sync/<job_id>/status")
def sync_status(job_id: str) -> ResponseReturnValue:
    job = get_sync_job(job_id)
    if job is None:
        return {"error": "Sync job not found."}, 404
    return job, 200


@web_bp.post("/sync/<job_id>/review/bulk")
def sync_review_bulk_action(job_id: str) -> ResponseReturnValue:
    action = request.form.get("action", "").strip().lower()

    try:
        if action == "skip_all":
            job = skip_all_low_confidence_candidates(job_id)
            skipped = int(job.get("bulk_summary", {}).get("skipped", 0))
            message = f"Accepted {skipped} low-confidence track(s) as missing."
        elif action == "download_selected":
            item_indexes = request.form.getlist("selected_item_index")
            candidate_ids = request.form.getlist("selected_candidate_id")
            selections = [
                (int(item_index), str(candidate_id).strip())
                for item_index, candidate_id in zip(item_indexes, candidate_ids, strict=False)
                if str(item_index).strip() and str(candidate_id).strip()
            ]
            job = download_selected_low_confidence_candidates(job_id, selections)
            summary = job.get("bulk_summary", {})
            message = (
                f"Downloaded {int(summary.get('downloaded', 0))} selected match(es); "
                f"{int(summary.get('remaining', 0))} still need review."
            )
        else:
            raise ValueError("Unknown bulk review action.")
    except Exception as exc:  # pragma: no cover - runtime/network dependent
        log.exception("Review bulk action failed for job %s", job_id)
        error_message = f"Could not update the low-confidence tracks: {exc}"
        if _wants_json_response():
            return {"ok": False, "error": error_message}, 400
        flash(error_message, "error")
        return redirect(url_for("web.sync_status_page", job_id=job_id))

    review_complete = job.get("sync", {}).get("summary", {}).get("low_confidence", 0) == 0
    if _wants_json_response():
        return {
            "ok": True,
            "message": message,
            "review_complete": review_complete,
            "redirect_url": url_for("web.sync_status_page", job_id=job_id),
            "job": job,
        }, 200

    flash(message, "success")
    if review_complete:
        flash(
            "Low-confidence review is complete. Commit the playlist to Navidrome "
            "when you're ready.",
            "success",
        )

    return redirect(url_for("web.sync_status_page", job_id=job_id))


@web_bp.post("/sync/<job_id>/review")
def sync_review_action(job_id: str) -> ResponseReturnValue:
    action = request.form.get("action", "search").strip().lower()
    item_index = request.form.get("item_index", type=int) or 0
    title = request.form.get("title", "").strip()
    artist = request.form.get("artist", "").strip()
    album = request.form.get("album", "").strip()

    try:
        if action == "search":
            job = search_low_confidence_candidates(
                job_id,
                item_index,
                title=title,
                artist=artist,
                album=album,
            )
            message = "Updated the candidate matches for that low-confidence track."
        elif action == "download":
            candidate_id = request.form.get(
                "candidate_id", request.form.get("deezer_id", "")
            ).strip()
            if not candidate_id:
                raise ValueError("Choose a candidate first.")
            job = resolve_low_confidence_candidate(
                job_id,
                item_index,
                deezer_id=candidate_id,
                title=title,
                artist=artist,
                album=album,
            )
            message = "Track resolved from the selected match."
        elif action == "skip":
            job = skip_low_confidence_candidate(
                job_id,
                item_index,
                title=title,
                artist=artist,
                album=album,
            )
            message = "Track marked as missing so you can keep going."
        else:
            raise ValueError("Unknown review action.")
    except Exception as exc:  # pragma: no cover - runtime/network dependent
        log.exception("Review action failed for job %s", job_id)
        error_message = f"Could not update the low-confidence track: {exc}"
        if _wants_json_response():
            return {"ok": False, "error": error_message}, 400
        flash(error_message, "error")
        return redirect(url_for("web.sync_status_page", job_id=job_id))

    review_complete = job.get("sync", {}).get("summary", {}).get("low_confidence", 0) == 0
    if _wants_json_response():
        return {
            "ok": True,
            "message": message,
            "review_complete": review_complete,
            "redirect_url": url_for("web.sync_status_page", job_id=job_id),
            "job": job,
        }, 200

    flash(message, "success")
    if review_complete:
        flash(
            (
                "Low-confidence review is complete. Commit the playlist to "
                "Navidrome when you're ready."
            ),
            "success",
        )

    return redirect(url_for("web.sync_status_page", job_id=job_id))


@web_bp.post("/sync/<job_id>/export")
def sync_export_playlist(job_id: str) -> ResponseReturnValue:
    try:
        log.info("Exporting playlist for job %s", job_id)
        job = export_sync_job_playlist(job_id)
        export_result = job.get("sync", {}).get("playlist_export", {})
    except Exception as exc:  # pragma: no cover - runtime/filesystem dependent
        log.exception("Playlist export failed for job %s", job_id)
        flash(f"Could not commit the playlist to Navidrome: {exc}", "error")
    else:
        log.info(
            "Playlist export complete for job %s: %s",
            job_id,
            export_result.get("target_path", ""),
        )
        flash(
            f"Navidrome playlist updated: {export_result.get('filename', 'playlist.m3u')} "
            f"({export_result.get('entry_count', 0)} track(s)).",
            "success",
        )

    return redirect(url_for("web.sync_status_page", job_id=job_id))


@web_bp.get("/logs")
def logs_page() -> str:
    return render_template("logs.html")


@web_bp.get("/catalog")
def catalog_page() -> str:
    music_root = current_app.config.get("NAVIDROME_MUSIC_ROOT", "").strip()
    issue_filter = request.args.get("issue_filter", "any-anomaly").strip().lower()
    search = request.args.get("search", "").strip()
    sort_by = request.args.get("sort_by", "path").strip().lower()
    sort_dir = request.args.get("sort_dir", "asc").strip().lower()
    page = request.args.get("page", 1, type=int)
    per_page = request.args.get("per_page", 25, type=int)
    counts: dict[str, int] = {filter_id: 0 for filter_id, _label in CATALOG_FILTERS}
    listing: dict[str, object] = {
        "items": [],
        "page": 1,
        "pages": 1,
        "per_page": per_page,
        "total": 0,
        "sort_by": sort_by,
        "sort_dir": sort_dir,
        "filter": issue_filter,
        "search": search,
    }
    last_batch = load_last_catalog_batch_result(current_app.config["DATA_DIR"])

    if music_root:
        root = Path(music_root)
        db_path = current_app.config["LIBRARY_INDEX_DB_PATH"]
        try:
            if root.is_dir():
                counts = catalog_filter_counts(db_path, root)
                listing = list_catalog_tracks(
                    db_path,
                    root,
                    issue_filter=issue_filter,
                    search=search,
                    sort_by=sort_by,
                    sort_dir=sort_dir,
                    page=page,
                    per_page=per_page,
                )
            else:
                flash("NAVIDROME_MUSIC_ROOT is not a directory.", "error")
        except sqlite3.OperationalError as exc:
            log.warning("Library catalog is busy while loading /catalog: %s", exc)
            flash(
                "Library catalog is busy right now. Try the Catalog page again in a moment.",
                "error",
            )

    return render_template(
        "catalog.html",
        music_root=music_root,
        filter_counts=counts,
        filter_options=CATALOG_FILTERS,
        batch_actions=catalog_batch_actions(),
        listing=listing,
        last_batch=last_batch,
        issue_filter=issue_filter,
        search=search,
        sort_by=sort_by,
        sort_dir=sort_dir,
        per_page=per_page,
    )


@web_bp.post("/catalog/batch")
def catalog_batch_action() -> ResponseReturnValue:
    action = request.form.get("action", "").strip().lower()
    dry_run = "dry_run" in request.form
    selected_paths = request.form.getlist("selected_paths")
    return_query = request.form.get("return_query", "").strip().lstrip("?")
    redirect_url = url_for("web.catalog_page")
    if return_query:
        redirect_url = f"{redirect_url}?{return_query}"

    try:
        result = run_catalog_batch_action(
            dict(current_app.config),
            action=action,
            relative_paths=selected_paths,
            dry_run=dry_run,
        )
    except Exception as exc:
        flash(f"Batch action failed: {exc}", "error")
        return redirect(redirect_url)

    flash(
        f"{result['label']}: {result['summary_line'] or 'Completed.'}",
        "success" if int(result.get("exit_code", 0) or 0) == 0 else "error",
    )
    return redirect(redirect_url)


@web_bp.post("/catalog/batch/start")
def catalog_batch_start() -> ResponseReturnValue:
    action = request.form.get("action", "").strip().lower()
    dry_run = "dry_run" in request.form
    selected_paths = request.form.getlist("selected_paths")
    if len(selected_paths) > CATALOG_PAGE_SIZE:
        return {
            "ok": False,
            "error": (
                f"Batch actions are limited to {CATALOG_PAGE_SIZE} tracks "
                "at a time right now."
            ),
        }, 400

    music_root = current_app.config.get("NAVIDROME_MUSIC_ROOT", "").strip()
    if not music_root:
        return {"ok": False, "error": "NAVIDROME_MUSIC_ROOT is not configured."}, 400

    script_path = Path(current_app.root_path).parent / "scripts" / "run_catalog_batch.py"
    cmd = [
        sys.executable,
        str(script_path),
        music_root,
        "--action",
        action,
    ]
    for relative_path in selected_paths:
        cmd.extend(["--relative-path", relative_path])
    if dry_run:
        cmd.append("--dry-run")

    snapshot = start_process_job(
        "catalog-batch",
        label=f"Catalog Batch · {catalog_batch_action_label(action)}",
        description="Running a selected-track catalog maintenance batch.",
        cmd=cmd,
        dry_run=dry_run,
        metadata={
            "action": action,
            "selected_count": len(selected_paths),
        },
        env_overrides={
            "NAVIDROME_MUSIC_ROOT": music_root,
            "LIBRARY_INDEX_DB_PATH": str(current_app.config["LIBRARY_INDEX_DB_PATH"]),
            "DATA_DIR": str(current_app.config["DATA_DIR"]),
        },
    )
    if snapshot is None:
        return {"ok": False, "error": "A catalog batch is already running."}, 409

    response = make_response(
        {
            "ok": True,
            "primary": snapshot,
            "status_url": url_for("web.catalog_batch_status"),
            "stop_url": url_for("web.catalog_batch_stop"),
            "stream_url": url_for("web.catalog_batch_stream"),
        },
        202,
    )
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


@web_bp.get("/catalog/batch/status")
def catalog_batch_status() -> ResponseReturnValue:
    snapshot = get_tool_status("catalog-batch", line_limit=400)
    response = make_response(
        {
            "active": bool(snapshot and snapshot.get("status") in {"running", "stopping"}),
            "primary": snapshot,
        },
        200,
    )
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


@web_bp.get("/catalog/batch/stream")
def catalog_batch_stream() -> ResponseReturnValue:
    def generate():
        snapshot = get_tool_status("catalog-batch", line_limit=400)
        if snapshot is None:
            yield "data: __EXIT__1\n\n"
            return

        lines = list(snapshot.get("lines", []))
        for line in lines:
            yield f"data: {line.replace(chr(10), ' ')}\n\n"
        sent_count = len(lines)

        while True:
            latest = get_tool_status("catalog-batch", line_limit=400)
            if latest is None:
                yield "data: __EXIT__1\n\n"
                return

            current_lines = list(latest.get("lines", []))
            if len(current_lines) < sent_count:
                yield "data: __RESET__\n\n"
                sent_count = 0

            for line in current_lines[sent_count:]:
                yield f"data: {line.replace(chr(10), ' ')}\n\n"
            sent_count = len(current_lines)

            status = str(latest.get("status") or "")
            if status not in {"running", "stopping"}:
                yield f"data: __EXIT__{int(latest.get('exit_code', 0) or 0)}\n\n"
                return

            time.sleep(0.5)

    return current_app.response_class(
        generate(),
        mimetype="text/event-stream",
        headers={"X-Accel-Buffering": "no", "Cache-Control": "no-cache"},
    )


@web_bp.post("/catalog/batch/stop")
def catalog_batch_stop() -> ResponseReturnValue:
    snapshot = stop_tool("catalog-batch")
    if snapshot is None:
        return {"ok": False, "error": "No catalog batch is running."}, 404
    return {"ok": True, "primary": snapshot}, 200


@web_bp.get("/logs/data")
def logs_data() -> ResponseReturnValue:
    log_path = Path(current_app.config["DATA_DIR"]) / "app.log"
    if not log_path.exists():
        return {"lines": [], "size": 0}, 200
    limit = request.args.get("limit", 500, type=int)
    with open(log_path, encoding="utf-8", errors="replace") as fh:
        all_lines = fh.readlines()
    lines = [line.rstrip("\n") for line in all_lines[-limit:]]
    return {"lines": lines, "total": len(all_lines), "showing": len(lines)}, 200


@web_bp.post("/logs/clear")
def logs_clear() -> ResponseReturnValue:
    log_path = Path(current_app.config["DATA_DIR"]) / "app.log"
    if log_path.exists():
        log_path.write_text("", encoding="utf-8")
    return redirect(url_for("web.logs_page"))


# ---------------------------------------------------------------------------
# Library tools
# ---------------------------------------------------------------------------

from app.services.library_tools import (  # noqa: E402
    TOOLS,
    get_tool_status,
    get_tool_status_snapshot,
    start_process_job,
    stop_tool,
    stream_tool,
)


@web_bp.get("/tools")
def tools_page() -> str:
    music_root = current_app.config.get("NAVIDROME_MUSIC_ROOT", "").strip()
    ffmpeg_available = bool(find_ffmpeg_executable())
    identify_audio_available = bool(find_fpcalc_executable(current_app.config)) and bool(
        str(current_app.config.get("ACOUSTID_API_KEY", "")).strip()
    )
    identify_audio_last_run: dict[str, object] | None = None
    report_filter = request.args.get("report_filter", "missing-xml").strip().lower()
    report_limit = request.args.get("report_limit", 50, type=int)
    refresh_catalog = request.args.get("refresh_catalog", "0") == "1"
    report_counts: dict[str, int] = {}
    report_items: list[dict[str, str]] = []
    report_filters = [
        ("musicbrainz-pending", "Needs MB Verify"),
        ("accepted-as-is", "Accepted As Is"),
        ("missing-xml", "Missing XML"),
        ("incomplete-xml", "Incomplete XML"),
        ("corrupted-audio", "Corrupted Audio"),
        ("non-deezer-source", "Non-Deezer Source"),
        ("orphaned-xml", "Orphaned XML"),
    ]

    if music_root:
        root = Path(music_root)
        db_path = current_app.config["LIBRARY_INDEX_DB_PATH"]
        try:
            if root.is_dir():
                if refresh_catalog:
                    refresh_library_index(db_path, root)
                report_counts = get_library_report_counts(db_path, root)
                report_items = list_library_report_items(
                    db_path,
                    root,
                    report_filter=report_filter,
                    limit=max(report_limit, 1),
                )
                identify_audio_last_run = load_latest_library_tool_run(
                    db_path,
                    tool_name="identify-audio",
                    root=root,
                )
            elif refresh_catalog:
                flash("NAVIDROME_MUSIC_ROOT is not a directory.", "error")
        except sqlite3.OperationalError as exc:
            log.warning("Library catalog is busy while loading /tools: %s", exc)
            flash(
                "Library catalog is busy right now. Try the Tools page again in a moment.",
                "error",
            )

    response = make_response(
        render_template(
            "tools.html",
            tools=TOOLS,
            ffmpeg_available=ffmpeg_available,
            identify_audio_available=identify_audio_available,
            identify_audio_last_run=identify_audio_last_run,
            music_root=music_root,
            report_counts=report_counts,
            report_items=report_items,
            report_filter=report_filter,
            report_filters=report_filters,
        )
    )
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


def _tools_page_redirect(
    report_filter: str = "missing-xml",
    *,
    open_identify_review: bool = False,
    open_identify_retry_result: bool = False,
    identify_retry_path: str | None = None,
) -> ResponseReturnValue:
    location = url_for(
        'web.tools_page',
        report_filter=report_filter,
        open_identify_review='1' if open_identify_review else None,
        open_identify_retry_result='1' if open_identify_retry_result else None,
        identify_retry_path=identify_retry_path or None,
    )
    return redirect(f"{location}#card-identify-audio")


def _identify_audio_review_context(
    run_id: int,
    relative_path: str,
) -> tuple[dict[str, object], Path, dict[str, object], str]:
    music_root = (
        Path(str(current_app.config.get("NAVIDROME_MUSIC_ROOT", "")).strip())
        .expanduser()
        .resolve()
    )
    if not music_root.is_dir():
        raise ValueError("NAVIDROME_MUSIC_ROOT is not configured correctly.")

    run = load_library_tool_run(current_app.config["LIBRARY_INDEX_DB_PATH"], run_id)
    if run is None or run.get("tool_name") != "identify-audio":
        raise ValueError("That Identify Tracks By Audio review snapshot could not be found.")
    if str(run.get("root_path") or "") != str(music_root):
        raise ValueError("That review snapshot belongs to a different music root.")

    result = run.get("result") if isinstance(run.get("result"), dict) else {}
    review = result.get("review") if isinstance(result.get("review"), dict) else {}
    normalized_path = str(relative_path or "").replace("\\", "/").strip()
    for group_name in ("low_confidence_items", "no_match_items"):
        items = review.get(group_name)
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            if str(item.get("relative_path") or "").replace("\\", "/").strip() == normalized_path:
                return run, music_root, item, group_name
    raise ValueError("That fingerprint review item is no longer available.")


def _write_identify_audio_review(run: dict[str, object], review: dict[str, object]) -> None:
    low_confidence_items = review.get("low_confidence_items")
    no_match_items = review.get("no_match_items")
    low_confidence_list = low_confidence_items if isinstance(low_confidence_items, list) else []
    no_match_list = no_match_items if isinstance(no_match_items, list) else []
    review["low_confidence_items"] = low_confidence_list
    review["no_match_items"] = no_match_list
    review["low_confidence_count"] = len(low_confidence_list)
    review["no_match_count"] = len(no_match_list)
    review["recorded_count"] = len(low_confidence_list) + len(no_match_list)
    review["truncated_count"] = max(int(review.get("truncated_count") or 0), 0)

    result = run.get("result") if isinstance(run.get("result"), dict) else {}
    result["review"] = review
    update_library_tool_run_result(
        current_app.config["LIBRARY_INDEX_DB_PATH"],
        run_id=int(run["id"]),
        result=result,
    )


@web_bp.post("/tools/identify-audio/review/accept")
def identify_audio_review_accept() -> ResponseReturnValue:
    run_id = request.form.get("run_id", type=int) or 0
    relative_path = request.form.get("relative_path", "")
    report_filter = request.form.get("report_filter", "missing-xml").strip().lower()

    try:
        run, music_root, item, group_name = _identify_audio_review_context(run_id, relative_path)
        if group_name != "low_confidence_items":
            raise ValueError("Only low-confidence fingerprint matches can be accepted.")
        details = review_item_to_details(item)
        if not details.get("recording_mbid"):
            raise ValueError("That review item does not contain a usable MusicBrainz recording.")

        audio_path = (music_root / str(item.get("relative_path") or "")).resolve()
        update_identify_audio_review_status(
            current_app.config["LIBRARY_INDEX_DB_PATH"],
            audio_path,
            status="",
            root=music_root,
        )
        apply_identification_metadata(audio_path, details)
        record_musicbrainz_verification(
            current_app.config["LIBRARY_INDEX_DB_PATH"],
            audio_path,
            root=music_root,
        )
        refresh_library_index_for_paths(
            current_app.config["LIBRARY_INDEX_DB_PATH"],
            music_root,
            [audio_path],
            scan_xml_sidecars=True,
        )

        result = run.get("result") if isinstance(run.get("result"), dict) else {}
        review = result.get("review") if isinstance(result.get("review"), dict) else {}
        items = review.get(group_name)
        review[group_name] = [
            candidate
            for candidate in items
            if isinstance(candidate, dict)
            and str(candidate.get("relative_path") or "").replace("\\", "/").strip()
            != str(item.get("relative_path") or "").replace("\\", "/").strip()
        ]
        _write_identify_audio_review(run, review)
        flash(f"Accepted fingerprint match for {item['relative_path']}.", "success")
    except Exception as exc:
        flash(f"Could not accept the fingerprint match: {exc}", "error")

    return _tools_page_redirect(report_filter, open_identify_review=True)


@web_bp.post("/tools/identify-audio/review/keep")
def identify_audio_review_keep() -> ResponseReturnValue:
    run_id = request.form.get("run_id", type=int) or 0
    relative_path = request.form.get("relative_path", "")
    report_filter = request.form.get("report_filter", "missing-xml").strip().lower()

    try:
        run, music_root, item, group_name = _identify_audio_review_context(run_id, relative_path)
        audio_path = (music_root / str(item.get("relative_path") or "")).resolve()
        update_identify_audio_review_status(
            current_app.config["LIBRARY_INDEX_DB_PATH"],
            audio_path,
            status="accepted-as-is",
            root=music_root,
        )

        result = run.get("result") if isinstance(run.get("result"), dict) else {}
        review = result.get("review") if isinstance(result.get("review"), dict) else {}
        items = review.get(group_name)
        review[group_name] = [
            candidate
            for candidate in items
            if isinstance(candidate, dict)
            and str(candidate.get("relative_path") or "").replace("\\", "/").strip()
            != str(item.get("relative_path") or "").replace("\\", "/").strip()
        ]
        _write_identify_audio_review(run, review)
        flash(
            f"Kept {item['relative_path']} as-is and removed it from fingerprint review.",
            "success",
        )
    except Exception as exc:
        flash(f"Could not keep the file as-is: {exc}", "error")

    return _tools_page_redirect(report_filter, open_identify_review=True)


@web_bp.post("/tools/identify-audio/review/retry")
def identify_audio_review_retry() -> ResponseReturnValue:
    run_id = request.form.get("run_id", type=int) or 0
    relative_path = request.form.get("relative_path", "")
    report_filter = request.form.get("report_filter", "missing-xml").strip().lower()

    try:
        run, music_root, item, _group_name = _identify_audio_review_context(run_id, relative_path)
        audio_path = (music_root / str(item.get("relative_path") or "")).resolve()
        update_identify_audio_review_status(
            current_app.config["LIBRARY_INDEX_DB_PATH"],
            audio_path,
            status="",
            root=music_root,
        )
        service = AcoustIdService.from_config(current_app.config)
        musicbrainz = MusicBrainzService.from_config(current_app.config)
        identified = service.identify_track(audio_path, musicbrainz_service=musicbrainz)

        result = run.get("result") if isinstance(run.get("result"), dict) else {}
        review = result.get("review") if isinstance(result.get("review"), dict) else {}
        normalized_path = str(item.get("relative_path") or "").replace("\\", "/").strip()
        low_confidence_items = [
            candidate
            for candidate in review.get("low_confidence_items", [])
            if isinstance(candidate, dict)
            and str(candidate.get("relative_path") or "").replace("\\", "/").strip()
            != normalized_path
        ]
        no_match_items = [
            candidate
            for candidate in review.get("no_match_items", [])
            if isinstance(candidate, dict)
            and str(candidate.get("relative_path") or "").replace("\\", "/").strip()
            != normalized_path
        ]

        match = identified.get("match") if isinstance(identified, dict) else {}
        used_metadata_fallback = False
        if not isinstance(match, dict) or not match.get("recording_mbid"):
            match = lookup_musicbrainz_metadata_match(
                audio_path,
                musicbrainz_service=musicbrainz,
                root=music_root,
            )
            used_metadata_fallback = bool(match.get("recording_mbid"))
        if not isinstance(match, dict) or not match.get("recording_mbid"):
            no_match_items.append(
                {
                    "relative_path": normalized_path,
                    "reason": "no_match",
                    "reason_label": "No match",
                    "message": (
                        "No AcoustID or MusicBrainz metadata match was returned "
                        "for this file."
                    ),
                }
            )
            review["low_confidence_items"] = low_confidence_items
            review["no_match_items"] = no_match_items
            _write_identify_audio_review(run, review)
            flash(
                f"Retry did not find a usable AcoustID or MusicBrainz match for {relative_path}.",
                "error",
            )
            return _tools_page_redirect(
                report_filter,
                open_identify_review=True,
                open_identify_retry_result=True,
                identify_retry_path=normalized_path,
            )

        guardrail = fingerprint_guardrail_assessment(audio_path, match)
        if used_metadata_fallback or (identified.get("accepted") and guardrail["accepted"]):
            apply_identification_metadata(audio_path, match)
            record_musicbrainz_verification(
                current_app.config["LIBRARY_INDEX_DB_PATH"],
                audio_path,
                root=music_root,
            )
            refresh_library_index_for_paths(
                current_app.config["LIBRARY_INDEX_DB_PATH"],
                music_root,
                [audio_path],
                scan_xml_sidecars=True,
            )
            review["low_confidence_items"] = low_confidence_items
            review["no_match_items"] = no_match_items
            _write_identify_audio_review(run, review)
            resolved_with = (
                "MusicBrainz metadata"
                if used_metadata_fallback
                else "high-confidence fingerprint"
            )
            flash(
                f"Retry resolved {relative_path} with a {resolved_with} match.",
                "success",
            )
            return _tools_page_redirect(
                report_filter,
                open_identify_review=True,
                open_identify_retry_result=True,
                identify_retry_path=normalized_path,
            )

        if identified.get("accepted"):
            low_confidence_items.append(
                build_review_item(
                    normalized_path,
                    match,
                    reason="guardrail",
                    reason_label="Similarity check",
                    message=(
                        "Retry still needs manual review before metadata can be trusted. "
                        f"{guardrail['reason'] or 'Similarity check failed'}."
                    ),
                )
            )
        else:
            low_confidence_items.append(
                build_review_item(
                    normalized_path,
                    match,
                    reason="low_confidence",
                    reason_label="Low confidence",
                    message=(
                        "Retry still needs manual review before metadata can be trusted. "
                        f"Score {float(match.get('acoustid_score') or 0.0):.2f}."
                    ),
                )
            )
        review["low_confidence_items"] = low_confidence_items
        review["no_match_items"] = no_match_items
        _write_identify_audio_review(run, review)
        flash(
            f"Retry updated the fingerprint candidate for {relative_path}, "
            "but it still needs review.",
            "success",
        )
    except Exception as exc:
        flash(f"Could not retry the fingerprint lookup: {exc}", "error")

    return _tools_page_redirect(
        report_filter,
        open_identify_review=True,
        open_identify_retry_result=True,
        identify_retry_path=relative_path,
    )


@web_bp.get("/tools/stream/<tool>")
def tools_stream(tool: str) -> ResponseReturnValue:
    if tool not in TOOLS:
        return {"error": f"Unknown tool: {tool!r}"}, 400

    music_root = current_app.config.get("NAVIDROME_MUSIC_ROOT", "").strip()
    if not music_root:
        def _no_root():
            yield "data: ERROR: NAVIDROME_MUSIC_ROOT is not set.\n\n"
            yield "data: __EXIT__1\n\n"
        return current_app.response_class(
            _no_root(),
            mimetype="text/event-stream",
            headers={"X-Accel-Buffering": "no", "Cache-Control": "no-cache"},
        )

    dry_run = request.args.get("dry_run", "0") == "1"
    full_scan = request.args.get("full_scan", "0") == "1"
    limit_raw = request.args.get("limit", "").strip()
    limit = int(limit_raw) if limit_raw.isdigit() else None
    root = Path(music_root)

    def generate():
        for line in stream_tool(
            tool,
            root,
            dry_run=dry_run,
            limit=limit,
            full_scan=full_scan,
        ):
            # Escape newlines inside a single SSE data field.
            safe = line.replace("\n", " ")
            yield f"data: {safe}\n\n"

    return current_app.response_class(
        generate(),
        mimetype="text/event-stream",
        headers={"X-Accel-Buffering": "no", "Cache-Control": "no-cache"},
    )


@web_bp.get("/tools/status")
def tools_status() -> ResponseReturnValue:
    line_limit = request.args.get("line_limit", 120, type=int)
    tool = request.args.get("tool", "").strip()
    if tool:
        if tool not in TOOLS:
            return {"error": f"Unknown tool: {tool!r}"}, 400
        snapshot = get_tool_status(tool, line_limit=max(10, min(line_limit, 120)))
        response = make_response(
            {
                "active": bool(snapshot and snapshot.get("status") == "running"),
                "primary": snapshot,
                "tools": [snapshot] if snapshot is not None else [],
            },
            200,
        )
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        return response
    snapshot = get_tool_status_snapshot(line_limit=max(10, min(line_limit, 400)))
    response = make_response(snapshot, 200)
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response

@web_bp.post("/tools/stop/<tool>")
def tools_stop(tool: str) -> ResponseReturnValue:
    if tool not in TOOLS:
        return {"error": f"Unknown tool: {tool!r}"}, 400

    snapshot = stop_tool(tool)
    if snapshot is None:
        return {"error": f"Unknown tool: {tool!r}"}, 404

    response = make_response(
        {
            "ok": True,
            "tool": tool,
            "primary": snapshot,
        },
        200,
    )
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response
