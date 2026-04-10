from __future__ import annotations

import json
from pathlib import Path
from uuid import uuid4

import httpx
from werkzeug.utils import secure_filename

from app.models import PlaylistTrack, PlaylistUpload
from app.parsers import parse_jspf, parse_m3u, parse_navidrome_missing_csv
from app.services.listenbrainz import (
    build_listenbrainz_api_url,
    extract_listenbrainz_playlist_id,
    normalize_listenbrainz_url,
)
from app.services.upload_cache import cache_upload, get_cached_upload

SUPPORTED_EXTENSIONS = {".m3u", ".m3u8", ".jspf", ".json", ".csv"}


def parse_uploaded_playlist(filename: str, payload: bytes) -> list[PlaylistTrack]:
    suffix = Path(filename).suffix.lower()
    if suffix not in SUPPORTED_EXTENSIONS:
        raise ValueError(f"Unsupported playlist type: {suffix or 'unknown'}")

    text = payload.decode("utf-8-sig", errors="ignore")
    if suffix in {".m3u", ".m3u8"}:
        return parse_m3u(text)
    if suffix == ".csv":
        return parse_navidrome_missing_csv(text)
    return parse_jspf(text)


def save_uploaded_playlist(
    upload_folder: str | Path, filename: str, payload: bytes
) -> PlaylistUpload:
    _ = upload_folder
    tracks = parse_uploaded_playlist(filename, payload)
    if not tracks:
        raise ValueError("The uploaded playlist did not contain any readable tracks.")
    playlist_name = _detect_playlist_name(filename, payload)

    stored_name = _build_stored_filename(filename)
    upload = PlaylistUpload(
        source_kind="upload",
        original_name=filename,
        playlist_name=playlist_name,
        stored_name=stored_name,
        saved_path=f"memory://{stored_name}",
        tracks=tracks,
    )
    return cache_upload(upload)


def load_saved_playlist(upload_folder: str | Path, saved_path: str | Path) -> PlaylistUpload:
    saved_path_text = str(saved_path).strip()
    cached_upload = get_cached_upload(saved_path_text)
    if cached_upload is not None:
        return cached_upload
    if saved_path_text.startswith("memory://"):
        raise ValueError(
            "This imported playlist is no longer in memory. Re-import it or use the tracked "
            "playlist tools in Settings to export it again."
        )

    base_folder = Path(upload_folder).resolve()
    target_path = Path(saved_path).resolve()

    target_path.relative_to(base_folder)
    payload = target_path.read_bytes()
    tracks = parse_uploaded_playlist(target_path.name, payload)
    if not tracks:
        raise ValueError("The saved playlist did not contain any readable tracks.")
    return PlaylistUpload(
        source_kind="saved-upload",
        original_name=target_path.name,
        playlist_name=_detect_playlist_name(target_path.name, payload),
        stored_name=target_path.name,
        saved_path=str(target_path),
        tracks=tracks,
    )


def parse_jspf_from_url(url: str) -> list[PlaylistTrack]:
    fetch_url = build_listenbrainz_api_url(url) or normalize_listenbrainz_url(url)
    with httpx.Client(follow_redirects=True, timeout=15.0) as client:
        response = client.get(fetch_url)
        response.raise_for_status()

    if "json" in response.headers.get("content-type", "").lower():
        return parse_jspf(response.json())
    return parse_jspf(response.text)


def fetch_remote_jspf(
    upload_folder: str | Path,
    url: str,
    listenbrainz: object | None = None,
) -> PlaylistUpload:
    raw_url = url.strip()
    payload: str | dict
    fetch_jspf_document = getattr(listenbrainz, "fetch_jspf_document", None)
    resolve_fetch_url = getattr(listenbrainz, "resolve_fetch_url", None)

    if callable(fetch_jspf_document):
        fetched_payload = fetch_jspf_document(raw_url)
        if not isinstance(fetched_payload, str | dict):
            raise ValueError("ListenBrainz returned an unsupported playlist payload.")

        payload = fetched_payload
        text = json.dumps(payload, ensure_ascii=False, indent=2)
        tracks = parse_jspf(payload)
        if callable(resolve_fetch_url):
            remote_url = str(resolve_fetch_url(raw_url))
        elif raw_url:
            remote_url = build_listenbrainz_api_url(raw_url) or normalize_listenbrainz_url(raw_url)
        else:
            remote_url = ""
    else:
        if not raw_url:
            raise ValueError(
                "Provide a ListenBrainz playlist URL, playlist ID, or JSPF export URL."
            )

        normalized_url = build_listenbrainz_api_url(raw_url) or normalize_listenbrainz_url(raw_url)
        with httpx.Client(follow_redirects=True, timeout=15.0) as client:
            response = client.get(normalized_url)
            response.raise_for_status()

        if "json" in response.headers.get("content-type", "").lower():
            payload = response.json()
            text = json.dumps(payload, ensure_ascii=False, indent=2)
            tracks = parse_jspf(payload)
        else:
            text = response.text
            payload = text
            tracks = parse_jspf(text)
        remote_url = normalized_url

    if not tracks:
        raise ValueError("The ListenBrainz playlist did not contain any readable tracks.")

    _ = upload_folder
    stored_name = _build_stored_filename("listenbrainz.jspf")
    upload = PlaylistUpload(
        source_kind="remote-jspf",
        original_name="listenbrainz.jspf",
        playlist_name=_detect_playlist_name("listenbrainz.jspf", payload),
        stored_name=stored_name,
        saved_path=f"memory://{stored_name}",
        remote_url=remote_url,
        tracks=tracks,
    )
    return cache_upload(upload)


def find_imported_listenbrainz_playlist_ids(
    upload_folder: str | Path,
    playlist_db_path: str | Path | None = None,
) -> set[str]:
    imported_ids: set[str] = set()

    if playlist_db_path:
        try:
            from app.services.playlist_history import find_recorded_listenbrainz_playlist_ids

            imported_ids.update(find_recorded_listenbrainz_playlist_ids(playlist_db_path))
        except Exception:
            pass

    folder = Path(upload_folder)
    if not folder.exists():
        return imported_ids

    for pattern in ("*.jspf", "*.json"):
        for candidate in folder.glob(pattern):
            try:
                payload = json.loads(candidate.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue

            playlist = payload.get("playlist", {})
            if not isinstance(playlist, dict):
                continue

            playlist_id = extract_listenbrainz_playlist_id(str(playlist.get("identifier", "")))
            if playlist_id:
                imported_ids.add(playlist_id.lower())

    return imported_ids


def _build_stored_filename(filename: str) -> str:
    path = Path(filename)
    stem = secure_filename(path.stem) or "playlist"
    suffix = path.suffix.lower() or ".txt"
    return f"{stem}-{uuid4().hex[:8]}{suffix}"


def _detect_playlist_name(filename: str, payload: bytes | str | dict) -> str:
    fallback = Path(filename).stem.strip() or "playlist"

    if isinstance(payload, bytes):
        text = payload.decode("utf-8", errors="ignore")
    elif isinstance(payload, str):
        text = payload
    else:
        text = ""

    try:
        parsed = payload if isinstance(payload, dict) else json.loads(text)
    except (TypeError, json.JSONDecodeError):
        return fallback

    if isinstance(parsed, dict):
        playlist = parsed.get("playlist") if isinstance(parsed.get("playlist"), dict) else parsed
    else:
        playlist = {}

    title = str(playlist.get("title", "")).strip() if isinstance(playlist, dict) else ""
    return title or fallback
