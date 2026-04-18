from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any, cast

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
_XML_ID_FIELD_MAP = {
    "musicbrainztrackid": "musicbrainz_track_id",
    "deezerid": "deezer_id",
    "deezerartistid": "deezer_artist_id",
    "deezeralbumid": "deezer_album_id",
    "deezerlink": "deezer_link",
}
_VA_FOLDER_NAMES = {
    "various artists",
    "various",
    "va",
    "v.a.",
    "v.a",
    "compilations",
    "compilation",
    "soundtracks",
    "soundtrack",
    "ost",
}


def guess_track_metadata(audio_path: str | Path) -> dict[str, str]:
    guessed = guess_preliminary_metadata(audio_path)
    return {
        "title": guessed["title"],
        "artist": guessed["artist"],
        "album": guessed["album"],
    }


def guess_preliminary_metadata(
    audio_path: str | Path,
    *,
    root: str | Path | None = None,
) -> dict[str, str]:
    path = Path(audio_path)
    stem = path.stem.strip()
    album = path.parent.name.strip() if path.parent.name else ""
    artist = path.parent.parent.name.strip() if path.parent.parent.name else ""
    album_artist = artist
    title = stem
    track_number = ""

    if root is not None:
        try:
            rel = path.resolve().relative_to(Path(root).expanduser().resolve())
        except ValueError:
            rel = None
        if rel is not None:
            if len(rel.parts) >= 2:
                artist = rel.parts[0].strip()
                album_artist = artist
            if len(rel.parts) >= 3:
                album = rel.parts[1].strip()

    parts = [part.strip() for part in stem.split(" - ") if part.strip()]

    if len(parts) >= 4 and parts[-2].isdigit():
        title = parts[-1]
        track_number = parts[-2].lstrip("0") or "0"
        if _is_va_folder_name(artist):
            artist = parts[0]
            album_artist = path.parent.parent.name.strip() if path.parent.parent.name else artist
            if len(parts) > 3:
                album = " - ".join(parts[1:-2]) or album
        else:
            artist = artist or parts[0]
            album_artist = album_artist or artist
            if len(parts) > 3:
                album = " - ".join(parts[1:-2]) or album
    elif len(parts) == 3 and parts[1].isdigit():
        if _is_va_folder_name(artist):
            artist = parts[0]
            album_artist = path.parent.parent.name.strip() if path.parent.parent.name else artist
        title = parts[-1]
        track_number = parts[1].lstrip("0") or "0"
    elif len(parts) >= 3:
        artist = parts[0]
        album_artist = path.parent.parent.name.strip() if path.parent.parent.name else artist
        album = " - ".join(parts[1:-1])
        title = parts[-1]
    elif len(parts) == 2:
        if parts[0].isdigit():
            track_number = parts[0].lstrip("0") or "0"
            title = parts[1]
        else:
            artist = parts[0]
            album_artist = path.parent.parent.name.strip() if path.parent.parent.name else artist
            title = parts[1]

    return {
        "title": title,
        "artist": artist,
        "album": album,
        "albumartist": album_artist or artist,
        "track_number": track_number,
    }


def _is_va_folder_name(name: str) -> bool:
    return str(name or "").strip().lower() in _VA_FOLDER_NAMES


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


def load_embedded_audio_metadata(audio_path: str | Path) -> dict[str, str]:
    path = Path(audio_path)

    try:
        easy_audio = _mutagen_file(path, easy=True)
    except Exception:
        easy_audio = None

    try:
        raw_audio = _mutagen_file(path)
    except Exception:
        raw_audio = None

    track_number = _first_easy_tag(easy_audio, "tracknumber")
    disc_number = _coalesce_text(
        _first_easy_tag(easy_audio, "discnumber"),
        _first_raw_tag(raw_audio, "discnumber"),
    )

    return {
        "title": _first_easy_tag(easy_audio, "title"),
        "artist": _first_easy_tag(easy_audio, "artist"),
        "album": _first_easy_tag(easy_audio, "album"),
        "albumartist": _first_easy_tag(easy_audio, "albumartist"),
        "artist_sort": _first_raw_tag(raw_audio, "artistsort", "artist_sort", "soar"),
        "albumartist_sort": _first_raw_tag(
            raw_audio,
            "albumartistsort",
            "albumartist_sort",
            "soaa",
        ),
        "track_number": _primary_number_tag(track_number),
        "track_total": _coalesce_text(
            _first_raw_tag(raw_audio, "tracktotal", "totaltracks"),
            _secondary_number_tag(track_number),
        ),
        "disc_number": _primary_number_tag(disc_number),
        "disc_total": _coalesce_text(
            _first_raw_tag(raw_audio, "disctotal", "totaldiscs"),
            _secondary_number_tag(disc_number),
        ),
        "date": _coalesce_text(
            _first_easy_tag(easy_audio, "date"),
            _first_raw_tag(raw_audio, "date"),
        ),
        "original_date": _first_raw_tag(raw_audio, "originaldate", "original_date"),
        "genre": _coalesce_text(
            _first_easy_tag(easy_audio, "genre"),
            _first_raw_tag(raw_audio, "genre"),
        ),
        "isrc": _first_raw_tag(raw_audio, "isrc", "tsrc"),
        "barcode": _first_raw_tag(raw_audio, "barcode"),
        "label": _first_raw_tag(raw_audio, "label", "publisher"),
        "catalog_number": _first_raw_tag(raw_audio, "catalognumber", "catalog_number"),
        "media_format": _first_raw_tag(raw_audio, "media", "mediaformat", "media_format"),
        "release_country": _first_raw_tag(raw_audio, "releasecountry", "release_country"),
        "release_status": _first_raw_tag(raw_audio, "releasestatus", "release_status"),
        "release_type": _first_raw_tag(raw_audio, "releasetype", "release_type"),
        "release_secondary_types": _first_raw_tag(
            raw_audio,
            "releasesecondarytypes",
            "release_secondary_types",
        ),
        "language": _first_raw_tag(raw_audio, "language"),
        "script": _first_raw_tag(raw_audio, "script"),
        "recording_disambiguation": _first_raw_tag(
            raw_audio,
            "recordingdisambiguation",
            "recording_disambiguation",
        ),
        "album_disambiguation": _first_raw_tag(
            raw_audio,
            "albumdisambiguation",
            "album_disambiguation",
        ),
        "musicbrainz_album_id": _first_raw_tag(
            raw_audio,
            "musicbrainz_albumid",
            "musicbrainz_album_id",
            "musicbrainzreleaseid",
            "musicbrainz_release_id",
            "releasembid",
            "release_mbid",
        ),
        "musicbrainz_track_id": _first_raw_tag(
            raw_audio,
            "musicbrainz_trackid",
            "musicbrainz_track_id",
            "musicbrainzrecordingid",
            "musicbrainz_recording_id",
            "musicbrainzrecordingmbid",
            "recordingmbid",
            "recording_mbid",
        ),
        "musicbrainz_artist_id": _first_raw_tag(
            raw_audio,
            "musicbrainz_artistid",
            "musicbrainz_artist_id",
            "artistmbid",
            "artist_mbid",
        ),
        "musicbrainz_albumartist_id": _first_raw_tag(
            raw_audio,
            "musicbrainz_albumartistid",
            "musicbrainz_albumartist_id",
            "musicbrainz_releaseartistid",
            "musicbrainz_releaseartist_id",
            "albumartistmbid",
            "albumartist_mbid",
        ),
        "musicbrainz_release_group_id": _first_raw_tag(
            raw_audio,
            "musicbrainz_releasegroupid",
            "musicbrainz_release_group_id",
            "releasegroupmbid",
            "release_group_mbid",
        ),
        "deezer_id": _first_raw_tag(raw_audio, "deezer_track_id", "deezertrackid", "deezerid"),
        "deezer_artist_id": _first_raw_tag(
            raw_audio,
            "deezer_artist_id",
            "deezerartistid",
            "deezerartist",
        ),
        "deezer_album_id": _first_raw_tag(
            raw_audio,
            "deezer_album_id",
            "deezeralbumid",
            "deezeralbum",
        ),
        "deezer_link": _first_raw_tag(raw_audio, "deezer_link", "deezerlink", "deezerurl"),
    }


def write_song_metadata_xml(
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
    quality: str = "",
    source: str = "",
    downloaded_from: str = "",
    annotation: str = "",
    musicbrainz_track_id: str = "",
    timestamp: str = "",
    extra_fields: Mapping[str, object] | None = None,
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
        "albumartist": album_artist or artist,
        "durationseconds": "" if duration_seconds in {None, ""} else str(duration_seconds),
    }
    optional_fields = {
        "provider": provider,
        "downloadedfrom": normalize_downloaded_from(
            downloaded_from,
            provider=provider,
            source=source,
        ),
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
        "audioextension": path.suffix,
        "source": source,
        "downloadedat": timestamp,
    }
    if extra_fields:
        for key, value in extra_fields.items():
            optional_fields[str(key).strip().lower()] = str(value or "").strip()

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
        "AUDIO_EXTENSION": path.suffix,
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
        embedded = load_embedded_audio_metadata(audio_path)

        title = _coalesce_text(existing.get("title"), embedded.get("title"), guessed["title"])
        artist = _coalesce_text(
            existing.get("performingartist"),
            existing.get("artist"),
            embedded.get("artist"),
            embedded.get("albumartist"),
            guessed["artist"],
        )
        album = _coalesce_text(
            existing.get("albumtitle"),
            existing.get("album"),
            embedded.get("album"),
            guessed["album"],
        )
        duration_seconds = _coalesce_text(existing.get("durationseconds"))
        provider = _coalesce_text(existing.get("provider"), "library")
        downloaded_from = normalize_downloaded_from(
            existing.get("downloadedfrom"),
            provider=provider,
            source=existing.get("source") or str(audio_path),
        )
        deezer_id = _coalesce_text(existing.get("deezerid"), embedded.get("deezer_id"))
        deezer_artist_id = _coalesce_text(
            existing.get("deezerartistid"),
            embedded.get("deezer_artist_id"),
        )
        deezer_album_id = _coalesce_text(
            existing.get("deezeralbumid"),
            embedded.get("deezer_album_id"),
        )
        deezer_link = _coalesce_text(existing.get("deezerlink"), embedded.get("deezer_link"))
        source = _coalesce_text(existing.get("source"), str(audio_path))
        annotation = _coalesce_text(existing.get("description"), existing.get("comment"))
        musicbrainz_track_id = _coalesce_text(
            existing.get("musicbrainztrackid"),
            embedded.get("musicbrainz_track_id"),
        )

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
                        downloaded_from=downloaded_from,
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


def repair_song_metadata_xml_ids(
    root: str | Path,
    *,
    dry_run: bool = False,
    limit: int | None = None,
    xml_paths: list[str | Path] | None = None,
    progress_callback: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    root_path = Path(root).expanduser()
    if not root_path.exists() or not root_path.is_dir():
        raise ValueError(f"Music library root does not exist or is not a directory: {root_path}")

    summary: dict[str, Any] = {
        "root": str(root_path),
        "scanned": 0,
        "updated": 0,
        "unchanged": 0,
        "unresolved": 0,
        "failed": 0,
        "written": [],
        "unresolved_items": [],
    }

    xml_candidates = (
        [Path(path) for path in xml_paths]
        if xml_paths is not None
        else sorted(root_path.rglob("*.xml"))
    )

    for xml_path in xml_candidates:
        if not xml_path.is_file():
            continue

        audio_path = next(
            (
                xml_path.with_suffix(ext)
                for ext in AUDIO_EXTENSIONS
                if xml_path.with_suffix(ext).exists()
            ),
            None,
        )
        if audio_path is None:
            continue

        if progress_callback is not None:
            try:
                relative_xml = xml_path.relative_to(root_path)
            except ValueError:
                relative_xml = xml_path
            progress_callback(f"INSPECT XML IDS: {relative_xml}")

        summary["scanned"] += 1
        if limit is not None and summary["scanned"] > limit:
            break

        try:
            existing = load_song_metadata_xml(xml_path)
            embedded = load_embedded_audio_metadata(audio_path)
            changed, missing_after = _repair_xml_id_fields(
                xml_path,
                existing=existing,
                embedded=embedded,
                dry_run=dry_run,
            )
        except Exception:
            summary["failed"] += 1
            continue

        if changed:
            summary["updated"] += 1
            summary["written"].append(str(xml_path))
        else:
            summary["unchanged"] += 1

        provider = str(existing.get("provider") or "").strip().lower()
        unresolved_fields = _primary_missing_id_fields(
            provider=provider,
            existing=existing,
            embedded=embedded,
            missing_after=missing_after,
        )
        if unresolved_fields:
            summary["unresolved"] += 1
            summary["unresolved_items"].append(
                {
                    "xml_path": str(xml_path),
                    "audio_path": str(audio_path),
                    "missing_fields": unresolved_fields,
                }
            )

    return summary


def _coalesce_text(*values: object) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _repair_xml_id_fields(
    xml_path: Path,
    *,
    existing: dict[str, str],
    embedded: dict[str, str],
    dry_run: bool,
) -> tuple[bool, list[str]]:
    tree = ET.parse(xml_path)
    root = tree.getroot()
    changed = False
    missing_after: list[str] = []

    for xml_field, embedded_field in _XML_ID_FIELD_MAP.items():
        embedded_value = str(embedded.get(embedded_field) or "").strip()
        elem = root.find(xml_field)
        current_value = (elem.text or "").strip() if elem is not None and elem.text else ""

        if embedded_value and current_value != embedded_value:
            changed = True
            if not dry_run:
                if elem is None:
                    elem = ET.SubElement(root, xml_field)
                elem.text = embedded_value

        final_value = embedded_value or current_value
        if not final_value:
            missing_after.append(xml_field)

    if changed and not dry_run:
        ET.indent(tree, space="  ")
        tree.write(xml_path, encoding="utf-8", xml_declaration=True)

    return changed, missing_after


def _primary_missing_id_fields(
    *,
    provider: str,
    existing: dict[str, str],
    embedded: dict[str, str],
    missing_after: list[str],
) -> list[str]:
    required_fields = ["musicbrainztrackid"]
    downloaded_from = normalize_downloaded_from(
        existing.get("downloadedfrom"),
        provider=provider,
        source=existing.get("source"),
    )
    if downloaded_from == "deezer" or existing.get("deezerid") or embedded.get("deezer_id"):
        required_fields.append("deezerid")

    return [field for field in required_fields if field in missing_after]


def normalize_downloaded_from(
    value: object,
    *,
    provider: object = "",
    source: object = "",
) -> str:
    text = _coalesce_text(value, provider)
    normalized = text.casefold()
    if normalized in {"deezer", "youtube", "soundcloud", "library", "manual"}:
        return normalized

    source_text = str(source or "").strip().casefold()
    if "deezer.com" in source_text:
        return "deezer"
    if "youtube.com" in source_text or "youtu.be" in source_text:
        return "youtube"
    if "soundcloud.com" in source_text:
        return "soundcloud"
    if source_text.startswith(("/", "file:", "g:\\", "c:\\", "d:\\")):
        return "library"
    return normalized or "unknown"


def _mutagen_file(path: Path, easy: bool = False) -> Any:
    from mutagen._file import File as MutagenFile

    return MutagenFile(path, easy=easy)


def _first_easy_tag(audio: Any, key: str) -> str:
    if audio is None:
        return ""
    try:
        value = audio.get(key)
    except Exception:
        return ""
    return _tag_value_to_text(value)


def _first_raw_tag(audio: Any, *candidate_keys: str) -> str:
    if audio is None:
        return ""

    normalized_candidates = {_normalize_tag_key(key) for key in candidate_keys}

    try:
        items = list(audio.items())
    except Exception:
        return ""

    for key, value in items:
        normalized_key = _normalize_tag_key(str(key))
        if normalized_key in normalized_candidates or any(
            normalized_key.endswith(candidate) for candidate in normalized_candidates
        ):
            text = _tag_value_to_text(value)
            if text:
                return text
    return ""


def _normalize_tag_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


def _tag_value_to_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (bytes, bytearray)):
        for encoding in ("utf-8", "utf-16", "latin-1"):
            try:
                return bytes(value).decode(encoding).strip("\x00 ").strip()
            except UnicodeDecodeError:
                continue
        return ""
    if isinstance(value, list):
        for item in value:
            text = _tag_value_to_text(item)
            if text:
                return text
        return ""
    text_attr = getattr(value, "text", None)
    if text_attr is not None:
        return _tag_value_to_text(text_attr)
    desc_attr = getattr(value, "desc", None)
    if desc_attr and hasattr(value, "text"):
        return _tag_value_to_text(cast(Any, value).text)
    return str(value or "").strip()


def _primary_number_tag(value: str) -> str:
    return str(value or "").split("/", 1)[0].strip()


def _secondary_number_tag(value: str) -> str:
    parts = str(value or "").split("/", 1)
    return parts[1].strip() if len(parts) == 2 else ""


def _set_flac_tag(audio: FLAC, key: str, value: object, *, overwrite: bool) -> None:
    text = str(value or "").strip()
    if not text:
        return

    existing_value = audio.get(key)
    has_existing = isinstance(existing_value, list) and bool(existing_value)
    if overwrite or key not in audio or not has_existing:
        audio[key] = [text]
