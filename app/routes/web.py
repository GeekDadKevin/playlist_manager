from __future__ import annotations

from pathlib import Path

from flask import (
    Blueprint,
    current_app,
    flash,
    redirect,
    render_template,
    request,
    send_from_directory,
    session,
    url_for,
)
from flask.typing import ResponseReturnValue

from app.matching import build_search_queries
from app.services.deezer_download import DeezerDownloadService
from app.services.ingest import (
    fetch_remote_jspf,
    find_imported_listenbrainz_playlist_ids,
    load_saved_playlist,
    save_uploaded_playlist,
)
from app.services.listenbrainz import ListenBrainzService
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
)
from app.services.settings_store import (
    save_settings as save_app_settings,
)
from app.services.sync_jobs import (
    export_sync_job_playlist,
    get_sync_job,
    resolve_low_confidence_candidate,
    search_low_confidence_candidates,
    skip_low_confidence_candidate,
    start_sync_job,
)

web_bp = Blueprint("web", __name__)


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
                    is_imported = bool(playlist_id and playlist_id in imported_listenbrainz_ids)
                    keep_visible = matches_playlist_target(playlist_title, playlist_targets)
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
    if request.method == "POST":
        existing_settings = load_settings(current_app.config["SETTINGS_FILE"])
        updated_settings = save_app_settings(
            current_app.config["SETTINGS_FILE"],
            {
                **existing_settings,
                "theme": request.form.get("theme", existing_settings.get("theme", "dark")),
                "automation_enabled": request.form.get("automation_enabled", ""),
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
                "sync_with_downloads": request.form.get("sync_with_downloads", ""),
            },
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

    settings = load_settings(current_app.config["SETTINGS_FILE"])
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
    service = DeezerDownloadService.from_config(current_app.config)
    if not service.is_configured():
        raise ValueError(
            "Configure `DEEZER_ARL` and `NAVIDROME_MUSIC_ROOT` to enable live downloads. "
            "SoundCloud is only used during low-confidence review."
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
            flash("Updated the candidate matches for that low-confidence track.", "success")
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
            flash("Track resolved from the selected match.", "success")
        elif action == "skip":
            job = skip_low_confidence_candidate(
                job_id,
                item_index,
                title=title,
                artist=artist,
                album=album,
            )
            flash("Track marked as missing so you can keep going.", "success")
        else:
            raise ValueError("Unknown review action.")
    except Exception as exc:  # pragma: no cover - runtime/network dependent
        flash(f"Could not update the low-confidence track: {exc}", "error")
        return redirect(url_for("web.sync_status_page", job_id=job_id))

    if job.get("sync", {}).get("summary", {}).get("low_confidence", 0) == 0:
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
        job = export_sync_job_playlist(job_id)
        export_result = job.get("sync", {}).get("playlist_export", {})
    except Exception as exc:  # pragma: no cover - runtime/filesystem dependent
        flash(f"Could not commit the playlist to Navidrome: {exc}", "error")
    else:
        flash(
            f"Navidrome playlist updated: {export_result.get('filename', 'playlist.m3u')} "
            f"({export_result.get('entry_count', 0)} track(s)).",
            "success",
        )

    return redirect(url_for("web.sync_status_page", job_id=job_id))


@web_bp.get("/logs")
def logs_page() -> str:
    return render_template("logs.html")


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
