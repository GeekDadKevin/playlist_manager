from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
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
            "review_preparing": False,
            "review_search_status": {"completed": 0, "total": 0, "current_track": ""},
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
        target["review_candidates_ready"] = True
        target["message"] = (
            "Choose a Deezer or SoundCloud match below, or keep refining the search."
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
    resolved["review_candidates_ready"] = True

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
        target["review_candidates_ready"] = True
        target["message"] = "Skipped during manual review. The track will stay missing for now."
        target["completed_at"] = _utc_timestamp()
        _refresh_job_state_unlocked(job_id)
        return deepcopy(job)


def skip_all_low_confidence_candidates(job_id: str) -> dict[str, Any]:
    with _JOBS_LOCK:
        job = _require_job_unlocked(job_id)
        results = job.get("sync", {}).setdefault("results", [])
        skipped = 0
        for item in results:
            if str(item.get("status", "")).strip() != "low_confidence":
                continue
            item["track"] = _track_from_item(item).to_dict()
            item["status"] = "not_found"
            item["message"] = "Accepted as missing during bulk review."
            item["completed_at"] = _utc_timestamp()
            skipped += 1
        _refresh_job_state_unlocked(job_id)
        snapshot = deepcopy(job)

    snapshot["bulk_summary"] = {"skipped": skipped}
    return snapshot


def download_selected_low_confidence_candidates(
    job_id: str,
    selections: list[tuple[int, str]],
) -> dict[str, Any]:
    summary = {
        "attempted": len(selections),
        "downloaded": 0,
        "remaining": 0,
        "failed": 0,
    }

    if not selections:
        raise ValueError("Choose at least one candidate before bulk downloading.")

    for item_index, candidate_id in selections:
        if item_index <= 0 or not candidate_id.strip():
            continue
        try:
            resolve_low_confidence_candidate(job_id, item_index, deezer_id=candidate_id)
        except Exception as exc:
            summary["failed"] += 1
            with _JOBS_LOCK:
                job = _require_job_unlocked(job_id)
                target = job.get("sync", {}).setdefault("results", [])[item_index - 1]
                target["review_status"] = "failed"
                target["message"] = f"Selected download failed: {exc}"
                target["completed_at"] = _utc_timestamp()
                _refresh_job_state_unlocked(job_id)
        else:
            summary["downloaded"] += 1

    with _JOBS_LOCK:
        snapshot = deepcopy(_require_job_unlocked(job_id))

    summary["remaining"] = int(snapshot.get("sync", {}).get("summary", {}).get("low_confidence", 0))
    snapshot["bulk_summary"] = summary
    return snapshot


def _should_prepare_review_candidates(service: SyncService, sync_result: dict[str, Any]) -> bool:
    soundcloud_service = getattr(service, "soundcloud_service", None)
    if soundcloud_service is None or not soundcloud_service.is_configured():
        return False
    return any(
        str(item.get("status", "")).strip() == "low_confidence"
        for item in sync_result.get("results", [])
    )


def _dedupe_review_candidates(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()

    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue

        key = str(
            candidate.get("deezer_id")
            or candidate.get("soundcloud_id")
            or candidate.get("id")
            or candidate.get("link")
            or (
                f"{candidate.get('provider', '')}:"
                f"{candidate.get('artist', '')}:"
                f"{candidate.get('title', '')}"
            )
        ).strip()
        if not key:
            continue

        normalized_key = key.lower()
        if normalized_key in seen:
            continue

        seen.add(normalized_key)
        deduped.append(candidate)

    return deduped


def _merge_review_candidates(
    existing_candidates: list[dict[str, Any]],
    additional_candidates: list[dict[str, Any]],
    *,
    limit: int = 8,
) -> list[dict[str, Any]]:
    primary = _dedupe_review_candidates(existing_candidates)
    secondary = _dedupe_review_candidates(additional_candidates)

    if not secondary:
        return primary[:limit]
    if not primary:
        return secondary[:limit]

    secondary_quota = min(len(secondary), max(1, limit // 2))
    primary_quota = min(len(primary), max(1, limit - secondary_quota))

    merged = [*primary[:primary_quota], *secondary[:secondary_quota]]
    leftovers = [*primary[primary_quota:], *secondary[secondary_quota:]]
    for candidate in leftovers:
        if len(merged) >= limit:
            break
        merged.append(candidate)

    return merged[:limit]


def _prepare_review_candidates_for_item(
    service: SyncService,
    item_index: int,
    item: dict[str, Any],
) -> dict[str, Any]:
    track = _track_from_item(item)
    track_label = f"{track.artist or 'Unknown artist'} — {track.title or 'Unknown title'}"
    existing_candidates = [
        candidate for candidate in list(item.get("candidates", [])) if isinstance(candidate, dict)
    ]
    if not existing_candidates and isinstance(item.get("match"), dict) and item["match"]:
        existing_candidates = [dict(item["match"])]

    if not existing_candidates:
        try:
            existing_candidates = service.search_track(track, limit=4, include_soundcloud=False)
        except Exception:
            existing_candidates = []

    soundcloud_service = getattr(service, "soundcloud_service", None)
    if soundcloud_service is None or not soundcloud_service.is_configured():
        soundcloud_candidates: list[dict[str, Any]] = []
    else:
        soundcloud_candidates = soundcloud_service.search_track(track, limit=4, max_queries=1)
        if not soundcloud_candidates:
            soundcloud_candidates = soundcloud_service.search_track(track, limit=4, max_queries=3)

    candidates = _merge_review_candidates(existing_candidates, soundcloud_candidates)
    return {
        "item_index": item_index,
        "track": track.to_dict(),
        "track_label": track_label,
        "candidates": candidates[:8],
        "match": candidates[0] if candidates else item.get("match", {}),
        "message": (
            "Choose a Deezer or SoundCloud match below, or keep the track missing."
            if candidates
            else "No Deezer or SoundCloud candidates were found for this track."
        ),
    }


def _prepare_low_confidence_review_candidates(
    job_id: str,
    sync_result: dict[str, Any],
    service: SyncService,
) -> dict[str, Any]:
    results = sync_result.setdefault("results", [])
    pending_items = [
        (index, deepcopy(item))
        for index, item in enumerate(results, start=1)
        if str(item.get("status", "")).strip() == "low_confidence"
    ]
    total = len(pending_items)
    if total == 0:
        sync_result["review_preparing"] = False
        sync_result["review_search_status"] = {"completed": 0, "total": 0, "current_track": ""}
        return sync_result

    sync_result["review_preparing"] = True
    sync_result["review_search_status"] = {
        "completed": 0,
        "total": total,
        "current_track": "Starting parallel SoundCloud lookups…",
    }
    _update_progress(job_id, sync_result)

    max_workers = min(4, total)
    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="review-preload") as pool:
        future_map = {
            pool.submit(_prepare_review_candidates_for_item, service, item_index, item): item_index
            for item_index, item in pending_items
        }
        completed = 0
        for future in as_completed(future_map):
            item_index = future_map[future]
            target = results[item_index - 1]
            try:
                prepared = future.result()
            except Exception as exc:
                track = _track_from_item(target)
                track_label = (
                    f"{track.artist or 'Unknown artist'} — {track.title or 'Unknown title'}"
                )
                target["track"] = track.to_dict()
                target["review_candidates_ready"] = True
                target["message"] = f"Could not pre-load SoundCloud matches: {exc}"
                target["completed_at"] = _utc_timestamp()
            else:
                track_label = str(prepared.get("track_label", "")).strip()
                target["track"] = prepared.get("track", target.get("track", {}))
                target["candidates"] = prepared.get("candidates", [])
                target["match"] = prepared.get("match", target.get("match", {}))
                target["review_candidates_ready"] = True
                target["message"] = prepared.get("message", target.get("message", ""))
                target["completed_at"] = _utc_timestamp()

            completed += 1
            sync_result["review_search_status"] = {
                "completed": completed,
                "total": total,
                "current_track": track_label,
            }
            _update_progress(job_id, sync_result)

    sync_result["review_preparing"] = False
    sync_result["review_search_status"] = {
        "completed": total,
        "total": total,
        "current_track": "",
    }
    return sync_result


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

    if _should_prepare_review_candidates(service, final_result):
        final_result = _prepare_low_confidence_review_candidates(job_id, final_result, service)
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
        track_number=track.get("track_number"),
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
