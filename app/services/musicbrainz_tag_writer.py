from __future__ import annotations

from pathlib import Path
from typing import Any

from mutagen import File as MutagenFile
from mutagen.flac import FLAC
from mutagen.id3 import (
    TALB,
    TCON,
    TDRC,
    TIT2,
    TPE1,
    TPE2,
    TPOS,
    TRCK,
    TSO2,
    TSOP,
    TSRC,
    TXXX,
)
from mutagen.mp4 import MP4, MP4FreeForm


def write_musicbrainz_tags(audio_path: str | Path, details: dict[str, Any]) -> None:
    path = Path(audio_path)
    normalized = normalize_musicbrainz_details(details)
    audio = MutagenFile(path)
    if audio is None:
        raise ValueError(f"Unsupported audio format: {path}")

    if isinstance(audio, FLAC) or audio.__class__.__module__.startswith("mutagen.ogg"):
        _set_common_mapping_tags(audio, normalized)
        audio.save()
        return

    if isinstance(audio, MP4):
        audio["\xa9nam"] = [normalized["title"]]
        audio["\xa9ART"] = [normalized["artist"]]
        audio["\xa9alb"] = [normalized["album"]]
        audio["aART"] = [normalized["albumartist"]]
        if normalized.get("artist_sort"):
            audio["soar"] = [normalized["artist_sort"]]
        if normalized.get("albumartist_sort"):
            audio["soaa"] = [normalized["albumartist_sort"]]
        if normalized.get("track_number"):
            audio["trkn"] = [
                (
                    int(normalized["track_number"]),
                    int(normalized.get("track_total") or 0),
                )
            ]
        if normalized.get("disc_number"):
            audio["disk"] = [
                (
                    int(normalized["disc_number"]),
                    int(normalized.get("disc_total") or 0),
                )
            ]
        if normalized.get("date"):
            audio["\xa9day"] = [normalized["date"]]
        if normalized.get("genre"):
            audio["\xa9gen"] = [normalized["genre"]]
        if normalized.get("recording_mbid"):
            audio["----:com.apple.iTunes:MUSICBRAINZ_TRACKID"] = [
                MP4FreeForm(normalized["recording_mbid"].encode("utf-8"))
            ]
        if normalized.get("release_mbid"):
            audio["----:com.apple.iTunes:MUSICBRAINZ_ALBUMID"] = [
                MP4FreeForm(normalized["release_mbid"].encode("utf-8"))
            ]
        if normalized.get("release_group_mbid"):
            audio["----:com.apple.iTunes:MUSICBRAINZ_RELEASEGROUPID"] = [
                MP4FreeForm(normalized["release_group_mbid"].encode("utf-8"))
            ]
        if normalized.get("artist_mbid"):
            audio["----:com.apple.iTunes:MUSICBRAINZ_ARTISTID"] = [
                MP4FreeForm(normalized["artist_mbid"].encode("utf-8"))
            ]
        if normalized.get("albumartist_mbid"):
            audio["----:com.apple.iTunes:MUSICBRAINZ_ALBUMARTISTID"] = [
                MP4FreeForm(normalized["albumartist_mbid"].encode("utf-8"))
            ]
        _set_mp4_freeform(audio, "ORIGINALDATE", normalized.get("original_date", ""))
        _set_mp4_freeform(audio, "ISRC", normalized.get("isrc", ""))
        _set_mp4_freeform(audio, "BARCODE", normalized.get("barcode", ""))
        _set_mp4_freeform(audio, "LABEL", normalized.get("label", ""))
        _set_mp4_freeform(audio, "CATALOGNUMBER", normalized.get("catalog_number", ""))
        _set_mp4_freeform(audio, "MEDIA", normalized.get("media_format", ""))
        _set_mp4_freeform(
            audio, "RELEASECOUNTRY", normalized.get("release_country", "")
        )
        _set_mp4_freeform(audio, "RELEASESTATUS", normalized.get("release_status", ""))
        _set_mp4_freeform(audio, "RELEASETYPE", normalized.get("release_type", ""))
        _set_mp4_freeform(
            audio,
            "RELEASESECONDARYTYPES",
            normalized.get("release_secondary_types", ""),
        )
        _set_mp4_freeform(audio, "LANGUAGE", normalized.get("language", ""))
        _set_mp4_freeform(audio, "SCRIPT", normalized.get("script", ""))
        _set_mp4_freeform(
            audio,
            "RECORDINGDISAMBIGUATION",
            normalized.get("recording_disambiguation", ""),
        )
        _set_mp4_freeform(
            audio,
            "ALBUMDISAMBIGUATION",
            normalized.get("album_disambiguation", ""),
        )
        audio.save()
        return

    if audio.__class__.__module__.startswith("mutagen.mp3"):
        if audio.tags is None:
            audio.add_tags()
        _set_mp3_tag_text(audio, TIT2, normalized["title"])
        _set_mp3_tag_text(audio, TPE1, normalized["artist"])
        _set_mp3_tag_text(audio, TALB, normalized["album"])
        _set_mp3_tag_text(audio, TPE2, normalized["albumartist"])
        _set_mp3_tag_text(audio, TSOP, normalized.get("artist_sort", ""))
        _set_mp3_tag_text(audio, TSO2, normalized.get("albumartist_sort", ""))
        _set_mp3_tag_text(audio, TCON, normalized.get("genre", ""))
        _set_mp3_tag_text(audio, TDRC, normalized.get("date", ""))
        _set_mp3_tag_text(audio, TSRC, normalized.get("isrc", ""))
        if normalized.get("track_number"):
            track_value = normalized["track_number"]
            if normalized.get("track_total"):
                track_value = f"{track_value}/{normalized['track_total']}"
            _set_mp3_tag_text(audio, TRCK, track_value)
        if normalized.get("disc_number"):
            disc_value = normalized["disc_number"]
            if normalized.get("disc_total"):
                disc_value = f"{disc_value}/{normalized['disc_total']}"
            _set_mp3_tag_text(audio, TPOS, disc_value)
        _set_txxx_text(
            audio, "MusicBrainz Track Id", normalized.get("recording_mbid", "")
        )
        _set_txxx_text(
            audio, "MusicBrainz Album Id", normalized.get("release_mbid", "")
        )
        _set_txxx_text(
            audio,
            "MusicBrainz Release Group Id",
            normalized.get("release_group_mbid", ""),
        )
        _set_txxx_text(
            audio, "MusicBrainz Artist Id", normalized.get("artist_mbid", "")
        )
        _set_txxx_text(
            audio,
            "MusicBrainz Album Artist Id",
            normalized.get("albumartist_mbid", ""),
        )
        _set_txxx_text(audio, "Original Date", normalized.get("original_date", ""))
        _set_txxx_text(audio, "Barcode", normalized.get("barcode", ""))
        _set_txxx_text(audio, "Label", normalized.get("label", ""))
        _set_txxx_text(audio, "Catalog Number", normalized.get("catalog_number", ""))
        _set_txxx_text(audio, "Media", normalized.get("media_format", ""))
        _set_txxx_text(audio, "Release Country", normalized.get("release_country", ""))
        _set_txxx_text(audio, "Release Status", normalized.get("release_status", ""))
        _set_txxx_text(audio, "Release Type", normalized.get("release_type", ""))
        _set_txxx_text(
            audio,
            "Release Secondary Types",
            normalized.get("release_secondary_types", ""),
        )
        _set_txxx_text(audio, "Language", normalized.get("language", ""))
        _set_txxx_text(audio, "Script", normalized.get("script", ""))
        _set_txxx_text(
            audio,
            "Recording Disambiguation",
            normalized.get("recording_disambiguation", ""),
        )
        _set_txxx_text(
            audio,
            "Album Disambiguation",
            normalized.get("album_disambiguation", ""),
        )
        audio.save()
        return

    easy_audio = MutagenFile(path, easy=True)
    if easy_audio is None:
        raise ValueError(f"Unsupported audio format: {path}")
    easy_audio["title"] = [normalized["title"]]
    easy_audio["artist"] = [normalized["artist"]]
    easy_audio["album"] = [normalized["album"]]
    easy_audio["albumartist"] = [normalized["albumartist"]]
    if normalized.get("track_number"):
        track_value = normalized["track_number"]
        if normalized.get("track_total"):
            track_value = f"{track_value}/{normalized['track_total']}"
        easy_audio["tracknumber"] = [track_value]
    if normalized.get("disc_number"):
        disc_value = normalized["disc_number"]
        if normalized.get("disc_total"):
            disc_value = f"{disc_value}/{normalized['disc_total']}"
        easy_audio["discnumber"] = [disc_value]
    if normalized.get("date"):
        easy_audio["date"] = [normalized["date"]]
    if normalized.get("genre"):
        easy_audio["genre"] = [normalized["genre"]]
    easy_audio.save()


def normalize_musicbrainz_details(details: dict[str, Any]) -> dict[str, str]:
    def _text(key: str) -> str:
        return str(details.get(key) or "").strip()

    def _number_text(key: str) -> str:
        value = details.get(key)
        if value in {None, "", 0, "0"}:
            return ""
        return str(value).strip()

    return {
        "title": _text("title"),
        "artist": _text("artist"),
        "artist_sort": _text("artist_sort"),
        "album": _text("album"),
        "albumartist": _text("albumartist") or _text("artist"),
        "albumartist_sort": _text("albumartist_sort"),
        "track_number": _number_text("track_number"),
        "track_total": _number_text("track_total"),
        "disc_number": _number_text("disc_number"),
        "disc_total": _number_text("disc_total"),
        "date": _text("date"),
        "original_date": _text("original_date"),
        "genre": _text("genre"),
        "isrc": _text("isrc"),
        "barcode": _text("barcode"),
        "label": _text("label"),
        "catalog_number": _text("catalog_number"),
        "media_format": _text("media_format"),
        "release_country": _text("release_country"),
        "release_status": _text("release_status"),
        "release_type": _text("release_type"),
        "release_secondary_types": _text("release_secondary_types"),
        "language": _text("language"),
        "script": _text("script"),
        "recording_disambiguation": _text("recording_disambiguation"),
        "album_disambiguation": _text("album_disambiguation"),
        "recording_mbid": _text("recording_mbid"),
        "release_mbid": _text("release_mbid"),
        "release_group_mbid": _text("release_group_mbid"),
        "artist_mbid": _text("artist_mbid"),
        "albumartist_mbid": _text("albumartist_mbid"),
    }


def musicbrainz_xml_fields(details: dict[str, Any]) -> dict[str, str]:
    normalized = normalize_musicbrainz_details(details)
    return {
        "musicbrainzalbumid": normalized["release_mbid"],
        "musicbrainzartistid": normalized["artist_mbid"],
        "musicbrainzalbumartistid": normalized["albumartist_mbid"],
        "musicbrainzreleasegroupid": normalized["release_group_mbid"],
        "artistsort": normalized["artist_sort"],
        "albumartistsort": normalized["albumartist_sort"],
        "tracktotal": normalized["track_total"],
        "discnumber": normalized["disc_number"],
        "disctotal": normalized["disc_total"],
        "date": normalized["date"],
        "originaldate": normalized["original_date"],
        "genre": normalized["genre"],
        "isrc": normalized["isrc"],
        "barcode": normalized["barcode"],
        "label": normalized["label"],
        "catalognumber": normalized["catalog_number"],
        "media": normalized["media_format"],
        "releasecountry": normalized["release_country"],
        "releasestatus": normalized["release_status"],
        "releasetype": normalized["release_type"],
        "releasesecondarytypes": normalized["release_secondary_types"],
        "language": normalized["language"],
        "script": normalized["script"],
        "recordingdisambiguation": normalized["recording_disambiguation"],
        "albumdisambiguation": normalized["album_disambiguation"],
    }


def _set_common_mapping_tags(audio: Any, details: dict[str, str]) -> None:
    audio["title"] = [details["title"]]
    audio["artist"] = [details["artist"]]
    audio["album"] = [details["album"]]
    audio["albumartist"] = [details["albumartist"]]
    _set_mapping_tag(audio, "artistsort", details.get("artist_sort", ""))
    _set_mapping_tag(audio, "albumartistsort", details.get("albumartist_sort", ""))
    _set_mapping_tag(audio, "tracknumber", details.get("track_number", ""))
    _set_mapping_tag(audio, "tracktotal", details.get("track_total", ""))
    _set_mapping_tag(audio, "discnumber", details.get("disc_number", ""))
    _set_mapping_tag(audio, "disctotal", details.get("disc_total", ""))
    _set_mapping_tag(audio, "date", details.get("date", ""))
    _set_mapping_tag(audio, "originaldate", details.get("original_date", ""))
    _set_mapping_tag(audio, "genre", details.get("genre", ""))
    _set_mapping_tag(audio, "isrc", details.get("isrc", ""))
    _set_mapping_tag(audio, "barcode", details.get("barcode", ""))
    _set_mapping_tag(audio, "label", details.get("label", ""))
    _set_mapping_tag(audio, "catalognumber", details.get("catalog_number", ""))
    _set_mapping_tag(audio, "media", details.get("media_format", ""))
    _set_mapping_tag(audio, "releasecountry", details.get("release_country", ""))
    _set_mapping_tag(audio, "releasestatus", details.get("release_status", ""))
    _set_mapping_tag(audio, "releasetype", details.get("release_type", ""))
    _set_mapping_tag(
        audio,
        "releasesecondarytypes",
        details.get("release_secondary_types", ""),
    )
    _set_mapping_tag(audio, "language", details.get("language", ""))
    _set_mapping_tag(audio, "script", details.get("script", ""))
    _set_mapping_tag(
        audio,
        "recordingdisambiguation",
        details.get("recording_disambiguation", ""),
    )
    _set_mapping_tag(
        audio,
        "albumdisambiguation",
        details.get("album_disambiguation", ""),
    )
    _set_mapping_tag(audio, "musicbrainz_trackid", details.get("recording_mbid", ""))
    _set_mapping_tag(audio, "musicbrainz_albumid", details.get("release_mbid", ""))
    _set_mapping_tag(
        audio,
        "musicbrainz_releasegroupid",
        details.get("release_group_mbid", ""),
    )
    _set_mapping_tag(audio, "musicbrainz_artistid", details.get("artist_mbid", ""))
    _set_mapping_tag(
        audio,
        "musicbrainz_albumartistid",
        details.get("albumartist_mbid", ""),
    )


def _set_mp3_tag_text(audio: Any, frame_cls: Any, value: str) -> None:
    if not value:
        return
    frame_name = frame_cls.__name__
    audio.tags.delall(frame_name)
    audio.tags.add(frame_cls(encoding=3, text=[value]))


def _set_txxx_text(audio: Any, desc: str, value: str) -> None:
    if not value:
        return
    audio.tags.delall(f"TXXX:{desc}")
    audio.tags.add(TXXX(encoding=3, desc=desc, text=[value]))


def _set_mp4_freeform(audio: MP4, key: str, value: str) -> None:
    if not value:
        return
    audio[f"----:com.apple.iTunes:{key}"] = [MP4FreeForm(value.encode("utf-8"))]


def _set_mapping_tag(audio: Any, key: str, value: str) -> None:
    if value:
        audio[key] = [value]
