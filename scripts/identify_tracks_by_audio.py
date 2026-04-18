"""Identify badly tagged files by audio fingerprinting.

Scans candidate audio files, generates a Chromaprint fingerprint using fpcalc,
looks the fingerprint up via AcoustID, then resolves the best MusicBrainz
recording details for tag and XML repair.
"""
from __future__ import annotations

import argparse
import datetime
import os
import sys
from pathlib import Path
from typing import Any

from rapidfuzz import fuzz

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=False)

from app.matching.normalize import normalize_text  # noqa: E402
from app.services.audio_identification import (  # noqa: E402
    AcoustIdService,
    lookup_musicbrainz_metadata_match,
)
from app.services.library_index import (  # noqa: E402
    list_musicbrainz_tag_candidates,
    record_library_tool_run,
    record_musicbrainz_verification,
    refresh_library_index,
    refresh_library_index_for_paths,
)
from app.services.musicbrainz import MusicBrainzService  # noqa: E402
from app.services.musicbrainz_tag_writer import (  # noqa: E402
    musicbrainz_xml_fields,
    normalize_musicbrainz_details,
    write_musicbrainz_tags,
)
from app.services.song_metadata import (  # noqa: E402
    load_embedded_audio_metadata,
    load_song_metadata_xml,
    normalize_downloaded_from,
    write_song_metadata_xml,
)
from app.services.tool_output import emit_console_line  # noqa: E402

_MAX_RECORDED_REVIEW_ITEMS = 100
_TITLE_GUARDRAIL_MIN = 72.0
_ARTIST_GUARDRAIL_MIN = 55.0
_COMBINED_GUARDRAIL_MIN = 78.0


def _emit(line: str, lines: list[str]) -> None:
    emit_console_line(line)
    lines.append(line)


def _write_tags(audio_path: Path, details: dict[str, Any]) -> None:
    write_musicbrainz_tags(audio_path, details)


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


def _details_need_update(audio_path: Path, details: dict[str, Any]) -> bool:
    embedded = load_embedded_audio_metadata(audio_path)
    xml_data = load_song_metadata_xml(audio_path.with_suffix(".xml"))
    normalized = normalize_musicbrainz_details(details)
    track_number = normalized["track_number"]
    albumartist = normalized["albumartist"]

    required_embedded_checks = (
        (embedded.get("title", ""), normalized["title"]),
        (embedded.get("artist", ""), normalized["artist"]),
        (embedded.get("album", ""), normalized["album"]),
        (embedded.get("albumartist", ""), albumartist),
        (embedded.get("musicbrainz_track_id", ""), normalized["recording_mbid"]),
    )
    optional_embedded_checks = (
        (embedded.get("track_number", "").split("/", 1)[0].strip(), track_number),
        (embedded.get("musicbrainz_album_id", ""), normalized["release_mbid"]),
        (embedded.get("musicbrainz_artist_id", ""), normalized["artist_mbid"]),
        (embedded.get("musicbrainz_albumartist_id", ""), normalized["albumartist_mbid"]),
        (embedded.get("musicbrainz_release_group_id", ""), normalized["release_group_mbid"]),
        (embedded.get("artist_sort", ""), normalized["artist_sort"]),
        (embedded.get("albumartist_sort", ""), normalized["albumartist_sort"]),
        (embedded.get("track_total", ""), normalized["track_total"]),
        (embedded.get("disc_number", ""), normalized["disc_number"]),
        (embedded.get("disc_total", ""), normalized["disc_total"]),
        (embedded.get("date", ""), normalized["date"]),
        (embedded.get("original_date", ""), normalized["original_date"]),
        (embedded.get("genre", ""), normalized["genre"]),
        (embedded.get("isrc", ""), normalized["isrc"]),
        (embedded.get("barcode", ""), normalized["barcode"]),
        (embedded.get("label", ""), normalized["label"]),
        (embedded.get("catalog_number", ""), normalized["catalog_number"]),
        (embedded.get("media_format", ""), normalized["media_format"]),
        (embedded.get("release_country", ""), normalized["release_country"]),
        (embedded.get("release_status", ""), normalized["release_status"]),
        (embedded.get("release_type", ""), normalized["release_type"]),
        (embedded.get("release_secondary_types", ""), normalized["release_secondary_types"]),
        (embedded.get("language", ""), normalized["language"]),
        (embedded.get("script", ""), normalized["script"]),
        (
            embedded.get("recording_disambiguation", ""),
            normalized["recording_disambiguation"],
        ),
        (embedded.get("album_disambiguation", ""), normalized["album_disambiguation"]),
    )
    if any(
        str(current or "").strip() != str(expected or "").strip()
        for current, expected in required_embedded_checks
    ):
        return True
    if any(
        str(expected or "").strip()
        and str(current or "").strip() != str(expected or "").strip()
        for current, expected in optional_embedded_checks
    ):
        return True

    required_xml_checks = (
        (xml_data.get("title", ""), normalized["title"]),
        (xml_data.get("performingartist", ""), normalized["artist"]),
        (xml_data.get("albumtitle", ""), normalized["album"]),
        (xml_data.get("albumartist", ""), albumartist),
        (xml_data.get("musicbrainztrackid", ""), normalized["recording_mbid"]),
    )
    if any(
        str(current or "").strip() != str(expected or "").strip()
        for current, expected in required_xml_checks
    ):
        return True

    if any(
        expected and str(xml_data.get(field, "") or "").strip() != expected
        for field, expected in musicbrainz_xml_fields(details).items()
    ):
        return True

    return bool(track_number) and str(xml_data.get("tracknumber", "") or "").strip() != track_number


def _match_similarity(left: str, right: str) -> float | None:
    if not str(left or "").strip() or not str(right or "").strip():
        return None
    return float(fuzz.token_set_ratio(normalize_text(left), normalize_text(right)))


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


def _guardrail_assessment(audio_path: Path, match: dict[str, Any]) -> dict[str, Any]:
    reference = _reference_metadata(audio_path)
    title_score = _match_similarity(reference.get("title", ""), str(match.get("title") or ""))
    artist_score = max(
        score
        for score in (
            _match_similarity(reference.get("artist", ""), str(match.get("artist") or "")),
            _match_similarity(
                reference.get("albumartist", ""),
                str(match.get("albumartist") or match.get("artist") or ""),
            ),
        )
        if score is not None
    ) if any(
        score is not None
        for score in (
            _match_similarity(reference.get("artist", ""), str(match.get("artist") or "")),
            _match_similarity(
                reference.get("albumartist", ""),
                str(match.get("albumartist") or match.get("artist") or ""),
            ),
        )
    ) else None
    album_score = _match_similarity(reference.get("album", ""), str(match.get("album") or ""))

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


def identify_tracks_by_audio(
    root: Path,
    *,
    dry_run: bool = False,
    limit: int | None = None,
    full_scan: bool = False,
    db_path: str | Path | None = None,
    selected_paths: list[Path] | None = None,
    tool_name: str = "identify-audio",
    record_run: bool = True,
) -> tuple[list[str], int]:
    lines: list[str] = []
    started_at = datetime.datetime.now(datetime.UTC).isoformat()
    started = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    library_index_db = str(
        db_path
        or os.getenv(
            "LIBRARY_INDEX_DB_PATH",
            Path(__file__).resolve().parent.parent / "data" / "library_index.db",
        )
    )

    _emit(
        "identify_tracks_by_audio  "
        f"root={root}  dry_run={dry_run}  limit={limit}  full_scan={full_scan}  started={started}",
        lines,
    )
    _emit("=" * 72, lines)

    service = AcoustIdService.from_config(os.environ)
    if not service.api_key:
        _emit("ERROR: ACOUSTID_API_KEY is not configured.", lines)
        return lines, 1
    if not service.fpcalc_path:
        _emit("ERROR: fpcalc is not installed or FPCALC_BIN is not configured.", lines)
        return lines, 1

    if selected_paths is not None:
        inventory_summary = None
        candidates = selected_paths[:limit] if limit is not None else list(selected_paths)
        _emit(
            "  Using explicit selection of "
            f"{len(candidates)} audio file(s) for fingerprint lookup.",
            lines,
        )
    else:
        inventory_summary = refresh_library_index(
            library_index_db,
            root,
            progress_callback=lambda line: _emit(line, lines),
            limit=limit,
            scan_xml_sidecars=False,
        )
        candidates = list_musicbrainz_tag_candidates(
            library_index_db,
            root,
            force_full=full_scan,
            limit=limit,
        )
        _emit(
            "  Indexed "
            f"{inventory_summary['scanned']} audio file(s); "
            f"selected {len(candidates)} candidate file(s) for fingerprint lookup.",
            lines,
        )

    musicbrainz = MusicBrainzService.from_config(os.environ)
    updated = 0
    skipped = 0
    unresolved = 0
    failed = 0
    low_confidence_items: list[dict[str, Any]] = []
    no_match_items: list[dict[str, Any]] = []

    for index, audio_path in enumerate(candidates, start=1):
        relative_path = audio_path.relative_to(root)
        _emit(f"CHECK AUDIO ID: {index}/{len(candidates)}  {relative_path}", lines)

        try:
            identified = service.identify_track(audio_path, musicbrainz_service=musicbrainz)
        except Exception as exc:
            failed += 1
            _emit(f"ERROR: fingerprint lookup failed for {relative_path}  [{exc}]", lines)
            continue

        match = identified.get("match") if isinstance(identified, dict) else {}
        used_metadata_fallback = False
        if not isinstance(match, dict) or not match.get("recording_mbid"):
            match = lookup_musicbrainz_metadata_match(
                audio_path,
                musicbrainz_service=musicbrainz,
                root=root,
            )
            used_metadata_fallback = bool(match.get("recording_mbid"))
        if not isinstance(match, dict) or not match.get("recording_mbid"):
            unresolved += 1
            no_match_items.append(
                {
                    "relative_path": str(relative_path).replace("\\", "/"),
                    "reason": "no_match",
                    "reason_label": "No match",
                    "message": (
                        "No AcoustID or MusicBrainz metadata match was returned "
                        "for this file."
                    ),
                }
            )
            _emit(f"WARN: no fingerprint or metadata match found for {relative_path}", lines)
            continue

        score = float(match.get("acoustid_score") or 0.0)
        label = (
            f"{str(match.get('artist') or '').strip()} - {str(match.get('title') or '').strip()}"
        ).strip(" -")
        if not used_metadata_fallback and not identified.get("accepted"):
            unresolved += 1
            low_confidence_items.append(
                {
                    "relative_path": str(relative_path).replace("\\", "/"),
                    "reason": "low_confidence",
                    "reason_label": "Low confidence",
                    "acoustid_id": str(match.get("acoustid_id") or "").strip(),
                    "acoustid_score": score,
                    "recording_mbid": str(match.get("recording_mbid") or "").strip(),
                    "release_mbid": str(match.get("release_mbid") or "").strip(),
                    "match_title": str(match.get("title") or "").strip(),
                    "match_artist": str(match.get("artist") or "").strip(),
                    "match_album": str(match.get("album") or "").strip(),
                    "match_albumartist": str(
                        match.get("albumartist") or match.get("artist") or ""
                    ).strip(),
                    "artist_mbid": str(match.get("artist_mbid") or "").strip(),
                    "albumartist_mbid": str(match.get("albumartist_mbid") or "").strip(),
                    "track_number": match.get("track_number"),
                    "message": (
                        "Needs manual review before metadata can be trusted. "
                        f"Score {score:.2f}."
                    ),
                }
            )
            _emit(
                f"WARN: low-confidence fingerprint match for {relative_path}  "
                f"[score={score:.2f}  match={label or 'unknown'}]",
                lines,
            )
            continue

        guardrail = _guardrail_assessment(audio_path, match)
        if not used_metadata_fallback and not guardrail["accepted"]:
            unresolved += 1
            low_confidence_items.append(
                {
                    "relative_path": str(relative_path).replace("\\", "/"),
                    "reason": "guardrail",
                    "reason_label": "Similarity check",
                    "acoustid_id": str(match.get("acoustid_id") or "").strip(),
                    "acoustid_score": score,
                    "recording_mbid": str(match.get("recording_mbid") or "").strip(),
                    "release_mbid": str(match.get("release_mbid") or "").strip(),
                    "match_title": str(match.get("title") or "").strip(),
                    "match_artist": str(match.get("artist") or "").strip(),
                    "match_album": str(match.get("album") or "").strip(),
                    "match_albumartist": str(
                        match.get("albumartist") or match.get("artist") or ""
                    ).strip(),
                    "artist_mbid": str(match.get("artist_mbid") or "").strip(),
                    "albumartist_mbid": str(match.get("albumartist_mbid") or "").strip(),
                    "track_number": match.get("track_number"),
                    "title_score": guardrail["title_score"],
                    "artist_score": guardrail["artist_score"],
                    "album_score": guardrail["album_score"],
                    "combined_score": guardrail["combined_score"],
                    "reference_title": guardrail["reference"].get("title", ""),
                    "reference_artist": guardrail["reference"].get("artist", ""),
                    "reference_album": guardrail["reference"].get("album", ""),
                    "message": (
                        "Fingerprint match needs manual review: "
                        f"{guardrail['reason'] or 'similarity check failed'}."
                    ),
                }
            )
            _emit(
                f"WARN: fingerprint match needs manual review for {relative_path}  "
                f"[score={score:.2f}  similarity={guardrail['combined_score'] or 0.0:.0f}  "
                f"match={label or 'unknown'}  reason={guardrail['reason'] or 'similarity'}]",
                lines,
            )
            continue

        if not _details_need_update(audio_path, match):
            skipped += 1
            if not dry_run:
                record_musicbrainz_verification(library_index_db, audio_path, root=root)
            _emit(f"SKIP: already matches fingerprint result for {relative_path}", lines)
            continue

        if dry_run:
            updated += 1
            _emit(
                f"[DRY-RUN] would update {relative_path}  "
                f"[score={score:.2f}  match={label or 'unknown'}"
                f"  source={'musicbrainz-metadata' if used_metadata_fallback else 'acoustid'}]",
                lines,
            )
            continue

        try:
            _write_tags(audio_path, match)
            preserved = _preserved_xml_fields(audio_path)
            source = preserved["source"] or f"https://musicbrainz.org/recording/{match['recording_mbid']}"
            write_song_metadata_xml(
                audio_path,
                title=str(match.get("title") or audio_path.stem).strip(),
                artist=str(match.get("artist") or "").strip(),
                album=str(match.get("album") or "").strip(),
                album_artist=str(match.get("albumartist") or match.get("artist") or "").strip(),
                track_number=match.get("track_number"),
                provider=preserved["provider"],
                downloaded_from=preserved["downloaded_from"],
                deezer_id=preserved["deezer_id"],
                deezer_artist_id=preserved["deezer_artist_id"],
                deezer_album_id=preserved["deezer_album_id"],
                deezer_link=preserved["deezer_link"],
                quality=preserved["quality"],
                source=source,
                annotation=preserved["annotation"],
                musicbrainz_track_id=str(match.get("recording_mbid") or "").strip(),
                extra_fields=musicbrainz_xml_fields(match),
                overwrite=True,
            )
            record_musicbrainz_verification(library_index_db, audio_path, root=root)
        except Exception as exc:
            failed += 1
            _emit(f"ERROR: could not write updated metadata for {relative_path}  [{exc}]", lines)
            continue

        updated += 1
        _emit(
            f"UPDATED: {relative_path}  [score={score:.2f}  match={label or 'unknown'}"
            f"  source={'musicbrainz-metadata' if used_metadata_fallback else 'acoustid'}]",
            lines,
        )

    _emit("", lines)
    _emit("=" * 72, lines)
    _emit(
        f"SUMMARY  scanned={len(candidates)}  updated={updated}  skipped={skipped}  "
        f"unresolved={unresolved}  failed={failed}  dry_run={dry_run}  full_scan={full_scan}",
        lines,
    )

    if not dry_run:
        refresh_library_index_for_paths(
            library_index_db,
            root,
            candidates,
            scan_xml_sidecars=True,
        )

    if record_run:
        review_items = low_confidence_items + no_match_items
        recorded_review_items = review_items[:_MAX_RECORDED_REVIEW_ITEMS]
        record_library_tool_run(
            library_index_db,
            tool_name=tool_name,
            root=root,
            run_mode="full" if full_scan else "incremental",
            started_at=started_at,
            completed_at=datetime.datetime.now(datetime.UTC).isoformat(),
            scanned_count=len(candidates),
            changed_count=updated,
            error_count=failed + unresolved,
            result={
                "inventory": inventory_summary,
                "updated": updated,
                "skipped": skipped,
                "unresolved": unresolved,
                "failed": failed,
                "dry_run": dry_run,
                "full_scan": full_scan,
                "review": {
                    "low_confidence_count": len(low_confidence_items),
                    "no_match_count": len(no_match_items),
                    "recorded_count": len(recorded_review_items),
                    "truncated_count": max(0, len(review_items) - len(recorded_review_items)),
                    "low_confidence_items": recorded_review_items[: len(low_confidence_items)],
                    "no_match_items": recorded_review_items[len(low_confidence_items) :],
                },
            },
        )

    return lines, 0 if failed == 0 else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "root",
        nargs="?",
        default=os.getenv("NAVIDROME_MUSIC_ROOT", ""),
        help="Music library root. Defaults to NAVIDROME_MUSIC_ROOT from .env.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be updated without writing any tags or XML files.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Only inspect the first N candidate audio files after sorting by path.",
    )
    parser.add_argument(
        "--full-scan",
        action="store_true",
        help="Recheck all indexed candidate files instead of only the incremental subset.",
    )
    args = parser.parse_args()

    if not args.root:
        print(
            "ERROR: no music root provided. "
            "Pass it as an argument or set NAVIDROME_MUSIC_ROOT in .env.",
            file=sys.stderr,
        )
        return 1

    root = Path(args.root).expanduser().resolve()
    if not root.is_dir():
        print(f"ERROR: {root} is not a directory.", file=sys.stderr)
        return 1

    log_lines, exit_code = identify_tracks_by_audio(
        root,
        dry_run=args.dry_run,
        limit=args.limit,
        full_scan=args.full_scan,
    )

    log_ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_name = f"identify_tracks_by_audio_{log_ts}.log"
    log_dir = root if os.access(root, os.W_OK) else Path.cwd()
    log_path = log_dir / log_name

    try:
        log_path.write_text("\n".join(log_lines) + "\n", encoding="utf-8")
        print(f"\nLog written to: {log_path}")
    except OSError as exc:
        print(f"\nWARNING: could not write log file: {exc}", file=sys.stderr)

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
