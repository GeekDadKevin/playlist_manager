from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from threading import Lock, Thread
from typing import Any
from uuid import uuid4

from app.models import PlaylistTrack, PlaylistUpload
from app.services.deezer_download import DeezerDownloadService, _utc_timestamp
from app.services.navidrome_playlists import export_navidrome_playlist
from app.services.playlist_history import record_playlist_run

SyncService = DeezerDownloadService

_JOBS: dict[str, dict[str, Any]] = {}
_JOB_CONTEXT: dict[str, dict[str, Any]] = {}
_JOBS_LOCK = Lock()


def create_sync_job(upload: PlaylistUpload, max_tracks: int) -> str:
    track_count = min(upload.count, max_tracks) if max_tracks else upload.count
    job_id = uuid4().hex

    job = {
        "job_id": job_id,
        "status": "queued",
        "created_at": _utc_timestamp(),
        "started_at": "",
        "completed_at": "",
        "progress_percent": 0,
        "error": "",
        "upload": upload.to_dict(),
        "sync": {
            "mode": "pending",
            "provider": "",
            "threshold": 0,
            "processing_mode": "sequential",
            "started_at": "",
            "completed_at": "",
            "summary": {
                "requested": track_count,
                "processed": 0,
                "preview": 0,
                "downloaded": 0,
                "already_available": 0,
                "low_confidence": 0,
                "not_found": 0,
                "failed": 0,
            },
            "results": [],
        },
    }

    with _JOBS_LOCK:
        _JOBS[job_id] = job

    return job_id


def start_sync_job(
    upload: PlaylistUpload,
    service: SyncService,
    max_tracks: int,
    navidrome_playlists_dir: str = "",
    playlist_db_path: str = "",
) -> str:
    job_id = create_sync_job(upload, max_tracks)
    with _JOBS_LOCK:
        _JOB_CONTEXT[job_id] = {
            "service": service,
            "navidrome_playlists_dir": navidrome_playlists_dir,
            "playlist_db_path": playlist_db_path,
        }

    worker = Thread(
        target=_run_sync_job,
        args=(job_id, upload, service, max_tracks, navidrome_playlists_dir, playlist_db_path),
        daemon=True,
        name=f"sync-job-{job_id[:8]}",
    )
    worker.start()
    return job_id


def get_sync_job(job_id: str) -> dict[str, Any] | None:
    with _JOBS_LOCK:
        job = _JOBS.get(job_id)
        return deepcopy(job) if job is not None else None


def search_low_confidence_candidates(
    job_id: str,
    item_index: int,
    *,
    title: str = "",
    artist: str = "",
    album: str = "",
) -> dict[str, Any]:
    item, service = _get_review_item(job_id, item_index)
    track = _track_from_item(item, title=title, artist=artist, album=album)
    candidates = service.search_track(track, limit=8, include_soundcloud=True)

    with _JOBS_LOCK:
        job = _require_job_unlocked(job_id)
        results = job["sync"].setdefault("results", [])
        target = results[item_index - 1]
        target["track"] = track.to_dict()
        target["queries"] = (
            list(candidates[0].get("queries", [])) if candidates else target.get("queries", [])
        )
        target["candidates"] = candidates[:8]
        target["match"] = candidates[0] if candidates else {}
        target["status"] = "low_confidence"
        target["message"] = (
            "Pick one of the candidate matches below, or keep refining the search."
            if candidates
            else "No Deezer or SoundCloud candidates were found for the revised search."
        )
        target["completed_at"] = _utc_timestamp()
        _refresh_job_state_unlocked(job_id)
        return deepcopy(job)


def resolve_low_confidence_candidate(
    job_id: str,
    item_index: int,
    *,
    deezer_id: str,
    title: str = "",
    artist: str = "",
    album: str = "",
) -> dict[str, Any]:
    item, service = _get_review_item(job_id, item_index)
    track = _track_from_item(item, title=title, artist=artist, album=album)
    candidates = list(item.get("candidates", [])) if isinstance(item, dict) else []
    if not candidates:
        candidates = service.search_track(track, limit=8, include_soundcloud=True)
    match = next(
        (
            candidate
            for candidate in candidates
            if str(candidate.get("deezer_id") or candidate.get("id") or "") == str(deezer_id)
        ),
        None,
    )
    if match is None:
        raise ValueError("Choose one of the available candidates first.")

    resolved = service.resolve_track_selection(track, match)
    resolved["index"] = item_index
    resolved["completed_at"] = _utc_timestamp()
    resolved["candidates"] = candidates[:8]

    with _JOBS_LOCK:
        job = _require_job_unlocked(job_id)
        job["sync"].setdefault("results", [])[item_index - 1] = resolved
        _refresh_job_state_unlocked(job_id)
        return deepcopy(job)


def skip_low_confidence_candidate(
    job_id: str,
    item_index: int,
    *,
    title: str = "",
    artist: str = "",
    album: str = "",
) -> dict[str, Any]:
    item, _service = _get_review_item(job_id, item_index)
    track = _track_from_item(item, title=title, artist=artist, album=album)

    with _JOBS_LOCK:
        job = _require_job_unlocked(job_id)
        target = job["sync"].setdefault("results", [])[item_index - 1]
        target["track"] = track.to_dict()
        target["status"] = "not_found"
        target["message"] = "Skipped during manual review. The track will stay missing for now."
        target["completed_at"] = _utc_timestamp()
        _refresh_job_state_unlocked(job_id)
        return deepcopy(job)


def export_sync_job_playlist(job_id: str) -> dict[str, Any]:
    with _JOBS_LOCK:
        job = _require_job_unlocked(job_id)
        context = _JOB_CONTEXT.get(job_id, {})
        navidrome_playlists_dir = str(context.get("navidrome_playlists_dir", "")).strip()
        if not navidrome_playlists_dir:
            raise ValueError("Set `NAVIDROME_PLAYLIST_DIR` to enable Navidrome playlist export.")
        if str(job.get("status", "")) != "completed":
            raise ValueError("Wait for the sync job to finish before exporting the playlist.")

        results = list(job.get("sync", {}).get("results", []))
        summary = _summarize_results(
            results, requested=job.get("sync", {}).get("summary", {}).get("requested", 0)
        )
        if summary.get("low_confidence", 0):
            raise ValueError("Resolve the low-confidence tracks before exporting this playlist.")

        playlist_name = str(
            job.get("upload", {}).get("playlist_name")
            or job.get("upload", {}).get("original_name")
            or "playlist"
        )

    export_result = export_navidrome_playlist(
        playlist_dir=navidrome_playlists_dir,
        playlist_name=playlist_name,
        sync_results=results,
    )

    with _JOBS_LOCK:
        job = _require_job_unlocked(job_id)
        job["sync"]["summary"] = _summarize_results(
            job["sync"].get("results", []),
            requested=job["sync"].get("summary", {}).get("requested", 0),
        )
        job["sync"]["playlist_export"] = export_result
        return deepcopy(job)


def _run_sync_job(
    job_id: str,
    upload: PlaylistUpload,
    service: SyncService,
    max_tracks: int,
    navidrome_playlists_dir: str,
    playlist_db_path: str,
) -> None:
    _update_job(
        job_id,
        {
            "status": "running",
            "started_at": _utc_timestamp(),
        },
    )

    try:
        final_result = service.sync_tracks(
            upload.tracks,
            max_tracks=max_tracks,
            progress_callback=lambda snapshot: _update_progress(job_id, snapshot),
        )
    except Exception as exc:  # pragma: no cover - runtime and network dependent
        _update_job(
            job_id,
            {
                "status": "failed",
                "error": str(exc),
                "completed_at": _utc_timestamp(),
            },
        )
        return

    final_result["summary"] = _summarize_results(
        final_result.get("results", []),
        requested=final_result.get("summary", {}).get("requested", 0),
    )

    if navidrome_playlists_dir:
        final_result["playlist_export"] = _prepare_playlist_export(
            upload,
            final_result,
            navidrome_playlists_dir,
        )

    if playlist_db_path:
        record_playlist_run(
            playlist_db_path,
            playlist_name=upload.playlist_name or upload.original_name,
            source_kind=upload.source_kind,
            original_name=upload.original_name,
            remote_url=upload.remote_url,
            saved_path=upload.saved_path,
            sync_result=final_result,
            export_result=final_result.get("playlist_export", {}),
        )

    _update_job(
        job_id,
        {
            "status": "completed",
            "completed_at": final_result.get("completed_at", _utc_timestamp()),
            "progress_percent": 100,
            "sync": final_result,
        },
    )


def _update_progress(job_id: str, snapshot: dict[str, Any]) -> None:
    summary = snapshot.get("summary", {})
    requested = int(summary.get("requested", 0))
    processed = int(summary.get("processed", 0))
    progress_percent = 100 if requested == 0 else int((processed / requested) * 100)

    _update_job(
        job_id,
        {
            "status": "running",
            "started_at": snapshot.get("started_at", ""),
            "completed_at": snapshot.get("completed_at", ""),
            "progress_percent": progress_percent,
            "sync": snapshot,
        },
    )


def _update_job(job_id: str, values: dict[str, Any]) -> None:
    with _JOBS_LOCK:
        if job_id not in _JOBS:
            return
        _JOBS[job_id].update(values)


def _get_review_item(job_id: str, item_index: int) -> tuple[dict[str, Any], SyncService]:
    if item_index <= 0:
        raise ValueError("Choose a valid track to review.")

    with _JOBS_LOCK:
        job = _require_job_unlocked(job_id)
        context = _JOB_CONTEXT.get(job_id, {})
        results = list(job.get("sync", {}).get("results", []))
        if item_index > len(results):
            raise ValueError("That track could not be found in the sync results.")
        item = deepcopy(results[item_index - 1])
        service = context.get("service")

    if service is None:
        raise ValueError("This sync job no longer has an active Deezer session for review.")
    return item, service


def _track_from_item(
    item: dict[str, Any],
    *,
    title: str = "",
    artist: str = "",
    album: str = "",
) -> PlaylistTrack:
    track = item.get("track", {}) if isinstance(item, dict) else {}
    return PlaylistTrack(
        title=title.strip() or str(track.get("title", "")).strip(),
        artist=artist.strip() or str(track.get("artist", "")).strip(),
        album=album.strip() or str(track.get("album", "")).strip(),
        duration_seconds=track.get("duration_seconds"),
        source=str(track.get("source", "")).strip(),
        extra=dict(track.get("extra", {})) if isinstance(track.get("extra"), dict) else {},
    )


def _require_job_unlocked(job_id: str) -> dict[str, Any]:
    job = _JOBS.get(job_id)
    if job is None:
        raise ValueError("Sync job not found.")
    return job


def _summarize_results(results: list[dict[str, Any]], requested: Any = 0) -> dict[str, int]:
    summary = {
        "requested": int(requested or len(results)),
        "processed": len(results),
        "preview": 0,
        "downloaded": 0,
        "already_available": 0,
        "low_confidence": 0,
        "not_found": 0,
        "failed": 0,
    }
    for item in results:
        status = str(item.get("status", "")).strip()
        if status in summary:
            summary[status] += 1
    return summary


def _refresh_job_state_unlocked(job_id: str) -> None:
    job = _require_job_unlocked(job_id)
    results = list(job.get("sync", {}).get("results", []))
    summary = _summarize_results(
        results, requested=job.get("sync", {}).get("summary", {}).get("requested", 0)
    )
    job["sync"]["summary"] = summary

    context = _JOB_CONTEXT.get(job_id, {})
    navidrome_playlists_dir = str(context.get("navidrome_playlists_dir", "")).strip()
    if not navidrome_playlists_dir:
        return

    playlist_name = str(
        job.get("upload", {}).get("playlist_name")
        or job.get("upload", {}).get("original_name")
        or "playlist"
    )
    job["sync"]["playlist_export"] = _pending_or_ready_export_result(
        playlist_name,
        navidrome_playlists_dir,
        summary,
    )


def _prepare_playlist_export(
    upload: PlaylistUpload,
    final_result: dict[str, Any],
    navidrome_playlists_dir: str,
) -> dict[str, Any]:
    summary = final_result.get("summary", {}) if isinstance(final_result, dict) else {}
    if int(summary.get("low_confidence", 0)) > 0:
        return _pending_or_ready_export_result(
            upload.playlist_name or upload.original_name,
            navidrome_playlists_dir,
            summary,
        )

    try:
        return export_navidrome_playlist(
            playlist_dir=navidrome_playlists_dir,
            playlist_name=upload.playlist_name or upload.original_name,
            sync_results=final_result.get("results", []),
        )
    except Exception as exc:  # pragma: no cover - filesystem dependent
        return {
            "configured": True,
            "written": False,
            "playlist_name": upload.playlist_name or upload.original_name,
            "reason": str(exc),
        }


def _pending_or_ready_export_result(
    playlist_name: str,
    navidrome_playlists_dir: str,
    summary: dict[str, Any],
) -> dict[str, Any]:
    pending_review = int(summary.get("low_confidence", 0)) > 0
    playable_count = int(summary.get("downloaded", 0)) + int(summary.get("already_available", 0))
    missing_count = (
        int(summary.get("not_found", 0))
        + int(summary.get("failed", 0))
        + int(summary.get("low_confidence", 0))
    )
    return {
        "configured": True,
        "written": False,
        "pending_review": pending_review,
        "ready": not pending_review,
        "playlist_name": playlist_name,
        "target_path": str(Path(navidrome_playlists_dir)),
        "entry_count": int(summary.get("processed", 0)),
        "playable_count": playable_count,
        "missing_count": missing_count,
        "reason": (
            "Resolve the low-confidence tracks below before exporting this playlist to Navidrome."
            if pending_review
            else "Manual review is complete. Commit the playlist to Navidrome when you're ready."
        ),
    }
