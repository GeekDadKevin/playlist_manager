from __future__ import annotations

from flask import Blueprint, current_app, flash, redirect, render_template, request, url_for
from flask.typing import ResponseReturnValue

from app.matching import build_search_queries
from app.services.ingest import fetch_remote_jspf, load_saved_playlist, save_uploaded_playlist
from app.services.octo_fiesta import OctoFiestaService
from app.services.sync_jobs import get_sync_job, start_sync_job

web_bp = Blueprint("web", __name__)


@web_bp.get("/")
def index() -> str:
    return render_template("index.html")


@web_bp.post("/review")
def review() -> ResponseReturnValue:
    uploaded = request.files.get("playlist_file")
    jspf_url = request.form.get("jspf_url", "").strip()

    try:
        if uploaded and uploaded.filename:
            result = save_uploaded_playlist(
                current_app.config["UPLOAD_FOLDER"],
                uploaded.filename,
                uploaded.read(),
            )
        elif jspf_url:
            result = fetch_remote_jspf(current_app.config["UPLOAD_FOLDER"], jspf_url)
        else:
            flash("Upload an M3U/JSPF file or provide a ListenBrainz-compatible JSPF URL.", "error")
            return redirect(url_for("web.index"))
    except Exception as exc:  # pragma: no cover - surfaced to UI
        flash(f"Could not parse playlist: {exc}", "error")
        return redirect(url_for("web.index"))

    octo = OctoFiestaService.from_config(current_app.config)
    preview_rows = []
    for track in result.tracks[:50]:
        preview_rows.append(
            {
                **track.to_dict(),
                "queries": build_search_queries(track),
                "octo_preview": octo.build_handoff_payload(track),
            }
        )

    return render_template(
        "review.html",
        upload=result.to_dict(),
        tracks=preview_rows,
        total_tracks=result.count,
        truncated=max(0, result.count - len(preview_rows)),
        octo_configured=octo.is_configured(),
        sync_max_tracks=current_app.config["SYNC_MAX_TRACKS"],
    )


@web_bp.post("/sync")
def sync_upload() -> ResponseReturnValue:
    saved_path = request.form.get("saved_path", "")
    max_tracks = request.form.get("max_tracks", type=int) or current_app.config["SYNC_MAX_TRACKS"]

    try:
        upload = load_saved_playlist(current_app.config["UPLOAD_FOLDER"], saved_path)
        octo = OctoFiestaService.from_config(current_app.config)
        if not octo.is_configured():
            raise ValueError(
                "Set `OCTO_FIESTA_BASE_URL`, `OCTO_FIESTA_USERNAME`, "
                "and either a password or token auth."
            )
        job_id = start_sync_job(upload, octo, max_tracks=max_tracks)
    except Exception as exc:  # pragma: no cover - runtime and network dependent
        flash(f"Sync failed: {exc}", "error")
        return redirect(url_for("web.index"))

    return redirect(url_for("web.sync_status_page", job_id=job_id))


@web_bp.get("/sync/<job_id>")
def sync_status_page(job_id: str) -> ResponseReturnValue:
    job = get_sync_job(job_id)
    if job is None:
        flash("That sync job could not be found.", "error")
        return redirect(url_for("web.index"))

    return render_template("sync.html", job=job)


@web_bp.get("/sync/<job_id>/status")
def sync_status(job_id: str) -> ResponseReturnValue:
    job = get_sync_job(job_id)
    if job is None:
        return {"error": "Sync job not found."}, 404
    return job, 200
