from __future__ import annotations

from flask import Blueprint, current_app, request
from flask.typing import ResponseReturnValue

from app.matching import build_search_queries, rank_candidates
from app.models import PlaylistTrack
from app.services.deezer_download import DeezerDownloadService
from app.services.ingest import load_saved_playlist, parse_uploaded_playlist, save_uploaded_playlist
from app.services.navidrome_playlists import export_navidrome_playlist

api_bp = Blueprint("api", __name__)


@api_bp.get("/health")
def health() -> ResponseReturnValue:
    return {"service": "jspf-converter", "status": "ok"}, 200


@api_bp.post("/upload")
def upload_playlist() -> ResponseReturnValue:
    uploaded = request.files.get("file")
    if uploaded is None or not uploaded.filename:
        return {"error": "Provide a multipart file in the 'file' field."}, 400

    result = save_uploaded_playlist(
        current_app.config["UPLOAD_FOLDER"],
        uploaded.filename,
        uploaded.read(),
    )
    return result.to_dict(), 201


@api_bp.post("/parse")
def parse_playlist() -> ResponseReturnValue:
    response: dict = {}

    if "file" in request.files and request.files["file"].filename:
        uploaded = request.files["file"]
        filename = uploaded.filename or "playlist.m3u"
        upload_result = save_uploaded_playlist(
            current_app.config["UPLOAD_FOLDER"],
            filename,
            uploaded.read(),
        )
        tracks = upload_result.tracks
        response["upload"] = upload_result.to_dict()
    else:
        payload = request.get_json(silent=True) or {}
        filename = payload.get("filename", "playlist.jspf")
        content = payload.get("content", "")
        if not content:
            return {"error": "Provide an uploaded file or JSON with filename/content."}, 400
        tracks = parse_uploaded_playlist(filename, content.encode("utf-8"))

    response["count"] = len(tracks)
    response["tracks"] = [track.to_dict() for track in tracks]
    return response, 200


@api_bp.post("/sync")
def sync_playlist() -> ResponseReturnValue:
    payload = request.get_json(silent=True) or {}
    upload_result = None

    if "file" in request.files and request.files["file"].filename:
        uploaded = request.files["file"]
        filename = uploaded.filename or "playlist.m3u"
        upload_result = save_uploaded_playlist(
            current_app.config["UPLOAD_FOLDER"],
            filename,
            uploaded.read(),
        )
        tracks = upload_result.tracks
    elif payload.get("saved_path"):
        upload_result = load_saved_playlist(
            current_app.config["UPLOAD_FOLDER"], payload["saved_path"]
        )
        tracks = upload_result.tracks
    elif payload.get("tracks"):
        tracks = [
            PlaylistTrack(
                title=item.get("title", ""),
                artist=item.get("artist", ""),
                album=item.get("album", ""),
                duration_seconds=item.get("duration_seconds"),
                source=item.get("source", ""),
                extra=item.get("extra", {}),
            )
            for item in payload.get("tracks", [])
        ]
    else:
        return {"error": "Provide a saved path, uploaded file, or a list of tracks to sync."}, 400

    downloader = DeezerDownloadService.from_config(current_app.config)
    try:
        sync_result = downloader.sync_tracks(tracks, max_tracks=payload.get("max_tracks"))
    except Exception as exc:
        return {"error": str(exc)}, 400

    response = dict(sync_result)
    playlist_name = payload.get("playlist_name", "playlist")
    if upload_result is not None:
        playlist_name = upload_result.playlist_name or upload_result.original_name
        response["upload"] = upload_result.to_dict()

    navidrome_playlists_dir = str(
        current_app.config.get("NAVIDROME_PLAYLISTS_DIR")
        or current_app.config.get("NAVIDROME_PLAYLIST_DIR", "")
    ).strip()
    if navidrome_playlists_dir:
        summary = response.get("summary", {}) if isinstance(response, dict) else {}
        if int(summary.get("low_confidence", 0)) > 0:
            response["playlist_export"] = {
                "configured": True,
                "written": False,
                "pending_review": True,
                "playlist_name": playlist_name,
                "target_path": navidrome_playlists_dir,
                "entry_count": int(summary.get("processed", 0)),
                "playable_count": int(summary.get("downloaded", 0))
                + int(summary.get("already_available", 0)),
                "missing_count": int(summary.get("not_found", 0))
                + int(summary.get("failed", 0))
                + int(summary.get("low_confidence", 0)),
                "reason": (
                    "Resolve the low-confidence tracks in the web UI before "
                    "exporting this playlist to Navidrome."
                ),
            }
        else:
            try:
                response["playlist_export"] = export_navidrome_playlist(
                    playlist_dir=navidrome_playlists_dir,
                    playlist_name=playlist_name,
                    sync_results=response.get("results", []),
                )
            except Exception as exc:
                response["playlist_export"] = {
                    "configured": True,
                    "written": False,
                    "playlist_name": playlist_name,
                    "reason": str(exc),
                }
    return response, 200


@api_bp.post("/match/preview")
def match_preview() -> ResponseReturnValue:
    payload = request.get_json(silent=True) or {}
    track = PlaylistTrack(
        title=payload.get("title", ""),
        artist=payload.get("artist", ""),
        album=payload.get("album", ""),
        duration_seconds=payload.get("duration_seconds"),
    )
    candidates = payload.get("candidates", [])

    if candidates:
        ranked = rank_candidates(track, candidates)
    else:
        downloader = DeezerDownloadService.from_config(current_app.config)
        try:
            ranked = downloader.search_track(track)
        except Exception as exc:
            return {"error": str(exc), "queries": build_search_queries(track), "ranked": []}, 400

    return {
        "queries": build_search_queries(track),
        "ranked": ranked,
    }, 200
