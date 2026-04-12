from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

from mutagen.flac import FLAC, FLACNoHeaderError

AUDIO_EXTENSIONS = {
    ".aac",
    ".aif",
    ".aiff",
    ".alac",
    ".ape",
    ".flac",
    ".m4a",
    ".mp3",
    ".ogg",
    ".oga",
    ".opus",
    ".wav",
    ".wma",
}
_MUSICBRAINZ_RECORDING_RE = re.compile(r"/recording/([0-9a-fA-F-]{36})", re.IGNORECASE)
_UUID_RE = re.compile(r"^[0-9a-fA-F-]{36}$")


def guess_track_metadata(audio_path: str | Path) -> dict[str, str]:
    path = Path(audio_path)
    stem = path.stem.strip()
    album = path.parent.name.strip() if path.parent.name else ""
    artist = path.parent.parent.name.strip() if path.parent.parent.name else ""
    title = stem

    parts = [part.strip() for part in stem.split(" - ") if part.strip()]
    if len(parts) >= 3:
        artist = parts[0]
        album = " - ".join(parts[1:-1])
        title = parts[-1]
    elif len(parts) == 2:
        artist = parts[0]
        title = parts[1]

    return {
        "title": title,
        "artist": artist,
        "album": album,
    }


def load_song_metadata_xml(metadata_path: str | Path) -> dict[str, str]:
    path = Path(metadata_path)
    if not path.exists() or not path.is_file():
        return {}

    try:
        root = ET.parse(path).getroot()
    except (ET.ParseError, OSError):
        return {}

    return {
        str(child.tag).strip().lower(): (child.text or "").strip()
        for child in root
        if child.tag is not None
    }


def extract_musicbrainz_track_id(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if _UUID_RE.fullmatch(text):
        return text

    match = _MUSICBRAINZ_RECORDING_RE.search(text)
    return match.group(1) if match else ""


def write_song_metadata_xml(
    audio_path: str | Path,
    *,
    title: str,
    artist: str = "",
    album: str = "",
    track_number: int | str | None = None,
    duration_seconds: int | str | None = None,
    provider: str = "",
    deezer_id: int | str | None = None,
    deezer_artist_id: int | str | None = None,
    deezer_album_id: int | str | None = None,
    deezer_link: str = "",
    quality: str = "",
    source: str = "",
    annotation: str = "",
    musicbrainz_track_id: str = "",
    timestamp: str = "",
    overwrite: bool = True,
) -> Path:
    path = Path(audio_path)
    metadata_path = path.with_suffix(".xml")
    if metadata_path.exists() and not overwrite:
        return metadata_path

    root = ET.Element("song")
    required_fields = {
        "title": title or path.stem,
        "performingartist": artist,
        "albumtitle": album,
        "albumartist": artist,
        "durationseconds": "" if duration_seconds in {None, ""} else str(duration_seconds),
    }
    optional_fields = {
        "provider": provider,
        "deezerid": "" if deezer_id in {None, ""} else str(deezer_id),
        "deezerartistid": "" if deezer_artist_id in {None, ""} else str(deezer_artist_id),
        "deezeralbumid": "" if deezer_album_id in {None, ""} else str(deezer_album_id),
        "deezerlink": deezer_link,
        "tracknumber": "" if track_number in {None, ""} else str(track_number),
        "musicbrainztrackid": musicbrainz_track_id or extract_musicbrainz_track_id(source),
        "description": annotation,
        "quality": quality,
        "audiofile": path.name,
        "audiopath": str(path),
        "source": source,
        "downloadedat": timestamp,
    }

    for key, value in required_fields.items():
        child = ET.SubElement(root, key)
        child.text = value

    for key, value in optional_fields.items():
        if value == "":
            continue
        child = ET.SubElement(root, key)
        child.text = value

    tree = ET.ElementTree(root)
    ET.indent(tree, space="  ")
    tree.write(metadata_path, encoding="utf-8", xml_declaration=True)
    return metadata_path


def write_flac_tags(
    audio_path: str | Path,
    *,
    title: str,
    artist: str = "",
    album: str = "",
    album_artist: str = "",
    track_number: int | str | None = None,
    duration_seconds: int | str | None = None,
    provider: str = "",
    deezer_id: int | str | None = None,
    deezer_artist_id: int | str | None = None,
    deezer_album_id: int | str | None = None,
    deezer_link: str = "",
    source: str = "",
    annotation: str = "",
    musicbrainz_track_id: str = "",
    quality: str = "",
    overwrite: bool = True,
) -> Path:
    path = Path(audio_path)
    if path.suffix.lower() != ".flac":
        return path

    try:
        audio = FLAC(path)
    except FLACNoHeaderError as exc:
        raise ValueError(f"File is not a valid FLAC file: {path}") from exc

    tags = {
        "TITLE": title or path.stem,
        "ARTIST": artist,
        "ALBUM": album,
        "ALBUMARTIST": album_artist or artist,
        "TRACKNUMBER": "" if track_number in {None, ""} else str(track_number),
        "DURATIONSECONDS": "" if duration_seconds in {None, ""} else str(duration_seconds),
        "PROVIDER": provider,
        "DEEZER_TRACK_ID": "" if deezer_id in {None, ""} else str(deezer_id),
        "DEEZER_ARTIST_ID": "" if deezer_artist_id in {None, ""} else str(deezer_artist_id),
        "DEEZER_ALBUM_ID": "" if deezer_album_id in {None, ""} else str(deezer_album_id),
        "DEEZER_LINK": deezer_link,
        "MUSICBRAINZ_TRACKID": musicbrainz_track_id or extract_musicbrainz_track_id(source),
        "SOURCE": source,
        "QUALITY": quality,
        "COMMENT": annotation,
        "DESCRIPTION": annotation,
    }

    for key, value in tags.items():
        _set_flac_tag(audio, key, value, overwrite=overwrite)

    audio.save()
    return path


def backfill_missing_song_xml(
    root: str | Path,
    *,
    overwrite: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    root_path = Path(root).expanduser()
    if not root_path.exists() or not root_path.is_dir():
        raise ValueError(f"Music library root does not exist or is not a directory: {root_path}")

    summary: dict[str, Any] = {
        "root": str(root_path),
        "scanned": 0,
        "created": 0,
        "tagged_flac": 0,
        "skipped_existing": 0,
        "failed": 0,
        "written": [],
    }

    for audio_path in sorted(root_path.rglob("*")):
        if not audio_path.is_file() or audio_path.suffix.lower() not in AUDIO_EXTENSIONS:
            continue

        summary["scanned"] += 1
        metadata_path = audio_path.with_suffix(".xml")
        guessed = guess_track_metadata(audio_path)
        existing = load_song_metadata_xml(metadata_path)

        title = _coalesce_text(existing.get("title"), guessed["title"])
        artist = _coalesce_text(
            existing.get("performingartist"),
            existing.get("artist"),
            guessed["artist"],
        )
        album = _coalesce_text(existing.get("albumtitle"), existing.get("album"), guessed["album"])
        duration_seconds = _coalesce_text(existing.get("durationseconds"))
        provider = _coalesce_text(existing.get("provider"), "library")
        deezer_id = _coalesce_text(existing.get("deezerid"))
        deezer_artist_id = _coalesce_text(existing.get("deezerartistid"))
        deezer_album_id = _coalesce_text(existing.get("deezeralbumid"))
        deezer_link = _coalesce_text(existing.get("deezerlink"))
        source = _coalesce_text(existing.get("source"), str(audio_path))
        annotation = _coalesce_text(existing.get("description"), existing.get("comment"))
        musicbrainz_track_id = _coalesce_text(existing.get("musicbrainztrackid"))

        try:
            if not metadata_path.exists() or overwrite:
                if not dry_run:
                    write_song_metadata_xml(
                        audio_path,
                        title=title,
                        artist=artist,
                        album=album,
                        duration_seconds=duration_seconds,
                        provider=provider,
                        deezer_id=deezer_id,
                        deezer_artist_id=deezer_artist_id,
                        deezer_album_id=deezer_album_id,
                        deezer_link=deezer_link,
                        source=source,
                        annotation=annotation,
                        musicbrainz_track_id=musicbrainz_track_id,
                        overwrite=overwrite,
                    )
                summary["created"] += 1
                summary["written"].append(str(metadata_path))
            else:
                summary["skipped_existing"] += 1

            if audio_path.suffix.lower() == ".flac":
                if not dry_run:
                    write_flac_tags(
                        audio_path,
                        title=title,
                        artist=artist,
                        album=album,
                        duration_seconds=duration_seconds,
                        provider=provider,
                        deezer_id=deezer_id,
                        deezer_artist_id=deezer_artist_id,
                        deezer_album_id=deezer_album_id,
                        deezer_link=deezer_link,
                        source=source,
                        annotation=annotation,
                        musicbrainz_track_id=musicbrainz_track_id,
                        overwrite=overwrite,
                    )
                summary["tagged_flac"] += 1
        except Exception:
            summary["failed"] += 1

    return summary


def _coalesce_text(*values: object) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _set_flac_tag(audio: FLAC, key: str, value: object, *, overwrite: bool) -> None:
    text = str(value or "").strip()
    if not text:
        return

    existing_value = audio.get(key)
    has_existing = isinstance(existing_value, list) and bool(existing_value)
    if overwrite or key not in audio or not has_existing:
        audio[key] = [text]
