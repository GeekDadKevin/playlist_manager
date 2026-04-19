from __future__ import annotations

from copy import deepcopy
from threading import Lock
from uuid import uuid4

from app.models import PlaylistUpload

_CACHE: dict[str, PlaylistUpload] = {}
_CACHE_LOCK = Lock()


def cache_upload(upload: PlaylistUpload) -> PlaylistUpload:
    saved_path = upload.saved_path.strip() or f"memory://{uuid4().hex}"
    upload.saved_path = saved_path
    if not upload.stored_name:
        upload.stored_name = (
            saved_path.removeprefix("memory://") or f"upload-{uuid4().hex[:8]}"
        )

    with _CACHE_LOCK:
        _CACHE[saved_path] = deepcopy(upload)

    return upload


def get_cached_upload(saved_path: str) -> PlaylistUpload | None:
    key = saved_path.strip()
    if not key:
        return None

    with _CACHE_LOCK:
        upload = _CACHE.get(key)
        return deepcopy(upload) if upload is not None else None
