from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import httpx
from werkzeug.utils import secure_filename

from app.models import PlaylistTrack, PlaylistUpload
from app.parsers import parse_jspf, parse_m3u

SUPPORTED_EXTENSIONS = {".m3u", ".m3u8", ".jspf", ".json"}


def parse_uploaded_playlist(filename: str, payload: bytes) -> list[PlaylistTrack]:
    suffix = Path(filename).suffix.lower()
    if suffix not in SUPPORTED_EXTENSIONS:
        raise ValueError(f"Unsupported playlist type: {suffix or 'unknown'}")

    text = payload.decode("utf-8", errors="ignore")
    if suffix in {".m3u", ".m3u8"}:
        return parse_m3u(text)
    return parse_jspf(text)


def save_uploaded_playlist(
    upload_folder: str | Path, filename: str, payload: bytes
) -> PlaylistUpload:
    tracks = parse_uploaded_playlist(filename, payload)

    folder = Path(upload_folder)
    folder.mkdir(parents=True, exist_ok=True)

    stored_name = _build_stored_filename(filename)
    saved_path = folder / stored_name
    saved_path.write_bytes(payload)

    return PlaylistUpload(
        source_kind="upload",
        original_name=filename,
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
        stored_name=target_path.name,
        saved_path=str(target_path),
        tracks=tracks,
    )


def parse_jspf_from_url(url: str) -> list[PlaylistTrack]:
    with httpx.Client(follow_redirects=True, timeout=15.0) as client:
        response = client.get(url)
        response.raise_for_status()
    return parse_jspf(response.text)


def fetch_remote_jspf(upload_folder: str | Path, url: str) -> PlaylistUpload:
    with httpx.Client(follow_redirects=True, timeout=15.0) as client:
        response = client.get(url)
        response.raise_for_status()

    text = response.text
    tracks = parse_jspf(text)

    folder = Path(upload_folder)
    folder.mkdir(parents=True, exist_ok=True)

    stored_name = _build_stored_filename("listenbrainz.jspf")
    saved_path = folder / stored_name
    saved_path.write_text(text, encoding="utf-8")

    return PlaylistUpload(
        source_kind="remote-jspf",
        original_name="listenbrainz.jspf",
        stored_name=stored_name,
        saved_path=str(saved_path),
        remote_url=url,
        tracks=tracks,
    )


def _build_stored_filename(filename: str) -> str:
    path = Path(filename)
    stem = secure_filename(path.stem) or "playlist"
    suffix = path.suffix.lower() or ".txt"
    return f"{stem}-{uuid4().hex[:8]}{suffix}"
