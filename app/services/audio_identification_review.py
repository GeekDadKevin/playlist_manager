from __future__ import annotations

from pathlib import Path
from typing import Any

from rapidfuzz import fuzz

from app.matching.normalize import normalize_text
from app.services.musicbrainz_tag_writer import musicbrainz_xml_fields, write_musicbrainz_tags
from app.services.song_metadata import (
    load_embedded_audio_metadata,
    load_song_metadata_xml,
    normalize_downloaded_from,
    write_song_metadata_xml,
)

_TITLE_GUARDRAIL_MIN = 72.0
_ARTIST_GUARDRAIL_MIN = 55.0
_COMBINED_GUARDRAIL_MIN = 78.0
_REVIEW_DETAIL_FIELDS = (
    "release_group_mbid",
    "artist_sort",
    "albumartist_sort",
    "track_total",
    "disc_number",
    "disc_total",
    "date",
    "original_date",
    "genre",
    "isrc",
    "barcode",
    "label",
    "catalog_number",
    "media_format",
    "release_country",
    "release_status",
    "release_type",
    "release_secondary_types",
    "language",
    "script",
    "recording_disambiguation",
    "album_disambiguation",
)


def review_item_to_details(item: dict[str, Any]) -> dict[str, Any]:
    artist = str(item.get("match_artist") or item.get("artist") or "").strip()
    albumartist = str(item.get("match_albumartist") or artist).strip()
    details = {
        "recording_mbid": str(item.get("recording_mbid") or "").strip(),
        "release_mbid": str(item.get("release_mbid") or "").strip(),
        "title": str(item.get("match_title") or item.get("title") or "").strip(),
        "artist": artist,
        "album": str(item.get("match_album") or item.get("album") or "").strip(),
        "albumartist": albumartist,
        "artist_mbid": str(item.get("artist_mbid") or "").strip(),
        "albumartist_mbid": str(item.get("albumartist_mbid") or "").strip(),
        "track_number": item.get("track_number"),
        "acoustid_id": str(item.get("acoustid_id") or "").strip(),
        "acoustid_score": float(item.get("acoustid_score") or 0.0),
    }
    for field in _REVIEW_DETAIL_FIELDS:
        details[field] = item.get(field)
    return details


def apply_identification_metadata(audio_path: str | Path, details: dict[str, Any]) -> None:
    path = Path(audio_path)
    write_musicbrainz_tags(path, details)
    preserved = _preserved_xml_fields(path)
    source = preserved["source"] or f"https://musicbrainz.org/recording/{details['recording_mbid']}"
    write_song_metadata_xml(
        path,
        title=str(details.get("title") or path.stem).strip(),
        artist=str(details.get("artist") or "").strip(),
        album=str(details.get("album") or "").strip(),
        album_artist=str(details.get("albumartist") or details.get("artist") or "").strip(),
        track_number=details.get("track_number"),
        provider=preserved["provider"],
        downloaded_from=preserved["downloaded_from"],
        deezer_id=preserved["deezer_id"],
        deezer_artist_id=preserved["deezer_artist_id"],
        deezer_album_id=preserved["deezer_album_id"],
        deezer_link=preserved["deezer_link"],
        quality=preserved["quality"],
        source=source,
        annotation=preserved["annotation"],
        musicbrainz_track_id=str(details.get("recording_mbid") or "").strip(),
        extra_fields=musicbrainz_xml_fields(details),
        overwrite=True,
    )


def fingerprint_guardrail_assessment(
    audio_path: str | Path,
    details: dict[str, Any],
) -> dict[str, Any]:
    path = Path(audio_path)
    reference = _reference_metadata(path)
    title_score = _match_similarity(reference.get("title", ""), str(details.get("title") or ""))
    artist_score = _best_similarity(
        _match_similarity(reference.get("artist", ""), str(details.get("artist") or "")),
        _match_similarity(
            reference.get("albumartist", ""),
            str(details.get("albumartist") or details.get("artist") or ""),
        ),
    )
    album_score = _match_similarity(reference.get("album", ""), str(details.get("album") or ""))

    weighted: list[tuple[float, float]] = []
    if title_score is not None:
        weighted.append((title_score, 0.65))
    if artist_score is not None:
        weighted.append((artist_score, 0.25))
    if album_score is not None:
        weighted.append((album_score, 0.10))
    total_weight = sum(weight for _score, weight in weighted)
    combined_score = (
        sum(score * weight for score, weight in weighted) / total_weight
        if weighted
        else None
    )

    reasons: list[str] = []
    if title_score is not None and title_score < _TITLE_GUARDRAIL_MIN:
        reasons.append(f"title mismatch {title_score:.0f}")
    if artist_score is not None and artist_score < _ARTIST_GUARDRAIL_MIN:
        reasons.append(f"artist mismatch {artist_score:.0f}")
    if combined_score is not None and combined_score < _COMBINED_GUARDRAIL_MIN:
        reasons.append(f"combined similarity {combined_score:.0f}")

    return {
        "accepted": not reasons,
        "title_score": title_score,
        "artist_score": artist_score,
        "album_score": album_score,
        "combined_score": combined_score,
        "reference": reference,
        "reason": ", ".join(reasons),
    }


def build_review_item(
    audio_path: str | Path,
    details: dict[str, Any],
    *,
    reason: str,
    reason_label: str,
    message: str,
) -> dict[str, Any]:
    path = Path(audio_path)
    guardrail = fingerprint_guardrail_assessment(path, details)
    item = {
        "relative_path": str(path).replace("\\", "/"),
        "reason": reason,
        "reason_label": reason_label,
        "acoustid_id": str(details.get("acoustid_id") or "").strip(),
        "acoustid_score": float(details.get("acoustid_score") or 0.0),
        "recording_mbid": str(details.get("recording_mbid") or "").strip(),
        "release_mbid": str(details.get("release_mbid") or "").strip(),
        "match_title": str(details.get("title") or "").strip(),
        "match_artist": str(details.get("artist") or "").strip(),
        "match_album": str(details.get("album") or "").strip(),
        "match_albumartist": str(details.get("albumartist") or details.get("artist") or "").strip(),
        "artist_mbid": str(details.get("artist_mbid") or "").strip(),
        "albumartist_mbid": str(details.get("albumartist_mbid") or "").strip(),
        "track_number": details.get("track_number"),
        "title_score": guardrail["title_score"],
        "artist_score": guardrail["artist_score"],
        "album_score": guardrail["album_score"],
        "combined_score": guardrail["combined_score"],
        "reference_title": guardrail["reference"].get("title", ""),
        "reference_artist": guardrail["reference"].get("artist", ""),
        "reference_album": guardrail["reference"].get("album", ""),
        "message": message,
    }
    for field in _REVIEW_DETAIL_FIELDS:
        item[field] = details.get(field)
    return item


def _preserved_xml_fields(audio_path: Path) -> dict[str, str]:
    xml_data = load_song_metadata_xml(audio_path.with_suffix(".xml"))
    embedded = load_embedded_audio_metadata(audio_path)
    provider = str(xml_data.get("provider") or "library").strip() or "library"
    source = str(
        xml_data.get("source")
        or f"https://musicbrainz.org/recording/{xml_data.get('musicbrainztrackid', '').strip()}"
        or str(audio_path)
    ).strip()
    return {
        "provider": provider,
        "downloaded_from": normalize_downloaded_from(
            xml_data.get("downloadedfrom"),
            provider=provider,
            source=source or str(audio_path),
        ),
        "source": source,
        "quality": str(xml_data.get("quality") or "").strip(),
        "annotation": str(xml_data.get("description") or "").strip(),
        "deezer_id": str(xml_data.get("deezerid") or embedded.get("deezer_id") or "").strip(),
        "deezer_artist_id": str(
            xml_data.get("deezerartistid") or embedded.get("deezer_artist_id") or ""
        ).strip(),
        "deezer_album_id": str(
            xml_data.get("deezeralbumid") or embedded.get("deezer_album_id") or ""
        ).strip(),
        "deezer_link": str(
            xml_data.get("deezerlink") or embedded.get("deezer_link") or ""
        ).strip(),
    }


def _match_similarity(left: str, right: str) -> float | None:
    if not str(left or "").strip() or not str(right or "").strip():
        return None
    return float(fuzz.token_set_ratio(normalize_text(left), normalize_text(right)))


def _best_similarity(*scores: float | None) -> float | None:
    present = [score for score in scores if score is not None]
    return max(present) if present else None


def _reference_metadata(audio_path: Path) -> dict[str, str]:
    embedded = load_embedded_audio_metadata(audio_path)
    xml_data = load_song_metadata_xml(audio_path.with_suffix(".xml"))
    artist = str(embedded.get("artist") or "").strip()
    albumartist = str(embedded.get("albumartist") or "").strip()
    xml_artist = str(xml_data.get("performingartist") or xml_data.get("artist") or "").strip()
    xml_albumartist = str(xml_data.get("albumartist") or "").strip()
    return {
        "title": str(embedded.get("title") or xml_data.get("title") or "").strip(),
        "artist": artist or albumartist or xml_artist,
        "albumartist": albumartist or artist or xml_albumartist or xml_artist,
        "album": str(embedded.get("album") or xml_data.get("albumtitle") or "").strip(),
    }
