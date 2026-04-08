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

SUPPORTED_EXTENSIONS = {".m3u", ".m3u8", ".jspf", ".json", ".csv"}


def parse_uploaded_playlist(filename: str, payload: bytes) -> list[PlaylistTrack]:
    suffix = Path(filename).suffix.lower()
    if suffix not in SUPPORTED_EXTENSIONS:
        raise ValueError(f"Unsupported playlist type: {suffix or 'unknown'}")

    text = payload.decode("utf-8", errors="ignore")
    if suffix in {".m3u", ".m3u8"}:
        return parse_m3u(text)
    if suffix == ".csv":
        return parse_navidrome_missing_csv(text)
    return parse_jspf(text)


def save_uploaded_playlist(
    upload_folder: str | Path, filename: str, payload: bytes
) -> PlaylistUpload:
    tracks = parse_uploaded_playlist(filename, payload)
    playlist_name = _detect_playlist_name(filename, payload)

    folder = Path(upload_folder)
    folder.mkdir(parents=True, exist_ok=True)

    stored_name = _build_stored_filename(filename)
    saved_path = folder / stored_name
    saved_path.write_bytes(payload)

    return PlaylistUpload(
        source_kind="upload",
        original_name=filename,
        playlist_name=playlist_name,
        stored_name=stored_name,
        saved_path=str(saved_path),
        tracks=tracks,
    )


def load_saved_playlist(upload_folder: str | Path, saved_path: str | Path) -> PlaylistUpload:
    base_folder = Path(upload_folder).resolve()
    target_path = Path(saved_path).resolve()

    target_path.relative_to(base_folder)
    payload = target_path.read_bytes()
    tracks = parse_uploaded_playlist(target_path.name, payload)

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

    folder = Path(upload_folder)
    folder.mkdir(parents=True, exist_ok=True)

    stored_name = _build_stored_filename("listenbrainz.jspf")
    saved_path = folder / stored_name
    saved_path.write_text(text, encoding="utf-8")

    return PlaylistUpload(
        source_kind="remote-jspf",
        original_name="listenbrainz.jspf",
        playlist_name=_detect_playlist_name("listenbrainz.jspf", payload),
        stored_name=stored_name,
        saved_path=str(saved_path),
        remote_url=remote_url,
        tracks=tracks,
    )


def find_imported_listenbrainz_playlist_ids(upload_folder: str | Path) -> set[str]:
    folder = Path(upload_folder)
    if not folder.exists():
        return set()

    imported_ids: set[str] = set()
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

    playlist = parsed.get("playlist", {}) if isinstance(parsed, dict) else {}
    title = str(playlist.get("title", "")).strip() if isinstance(playlist, dict) else ""
    return title or fallback
