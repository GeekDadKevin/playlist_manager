from __future__ import annotations

from copy import deepcopy
from threading import Lock, Thread
from typing import Any
from uuid import uuid4

from app.models import PlaylistUpload
from app.services.octo_fiesta import OctoFiestaService, _utc_timestamp

_JOBS: dict[str, dict[str, Any]] = {}
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


def start_sync_job(upload: PlaylistUpload, octo: OctoFiestaService, max_tracks: int) -> str:
    job_id = create_sync_job(upload, max_tracks)
    worker = Thread(
        target=_run_sync_job,
        args=(job_id, upload, octo, max_tracks),
        daemon=True,
        name=f"sync-job-{job_id[:8]}",
    )
    worker.start()
    return job_id


def get_sync_job(job_id: str) -> dict[str, Any] | None:
    with _JOBS_LOCK:
        job = _JOBS.get(job_id)
        return deepcopy(job) if job is not None else None


def _run_sync_job(
    job_id: str,
    upload: PlaylistUpload,
    octo: OctoFiestaService,
    max_tracks: int,
) -> None:
    _update_job(
        job_id,
        {
            "status": "running",
            "started_at": _utc_timestamp(),
        },
    )

    try:
        final_result = octo.sync_tracks(
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
