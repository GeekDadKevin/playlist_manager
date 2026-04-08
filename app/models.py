from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(slots=True)
class PlaylistTrack:
    title: str
    artist: str = ""
    album: str = ""
    duration_seconds: int | None = None
    source: str = ""
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class PlaylistUpload:
    source_kind: str
    original_name: str
    playlist_name: str = ""
    stored_name: str = ""
    saved_path: str = ""
    remote_url: str = ""
    tracks: list[PlaylistTrack] = field(default_factory=list)

    @property
    def count(self) -> int:
        return len(self.tracks)

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_kind": self.source_kind,
            "original_name": self.original_name,
            "playlist_name": self.playlist_name,
            "stored_name": self.stored_name,
            "saved_path": self.saved_path,
            "remote_url": self.remote_url,
            "count": self.count,
            "tracks": [track.to_dict() for track in self.tracks],
        }
