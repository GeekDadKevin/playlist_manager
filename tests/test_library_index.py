from __future__ import annotations

import sqlite3

from app.services.library_index import (
    count_indexed_audio_files,
    get_library_report_counts,
    init_library_index,
    list_audio_health_candidates,
    list_incomplete_xml_pairs,
    list_library_report_items,
    list_missing_xml_audio_paths,
    list_musicbrainz_tag_candidates,
    list_orphaned_xml_paths,
    list_structure_tag_candidates,
    list_tag_fix_candidates,
    list_xml_id_repair_candidates,
    record_audio_health_result,
    record_musicbrainz_verification,
    refresh_library_index,
)
from app.services.song_metadata import write_song_metadata_xml


def test_refresh_library_index_tracks_xml_provenance_and_candidate_skips(
    tmp_path,
) -> None:
    db_path = tmp_path / "library_index.db"
    audio_path = (
        tmp_path
        / "Boards of Canada"
        / "Music Has the Right to Children"
        / "Roygbiv.flac"
    )
    audio_path.parent.mkdir(parents=True, exist_ok=True)
    audio_path.write_bytes(b"fake-flac")
    write_song_metadata_xml(
        audio_path,
        title="Roygbiv",
        artist="Boards of Canada",
        album="Music Has the Right to Children",
        provider="youtube",
        downloaded_from="youtube",
        source="https://www.youtube.com/watch?v=roygbiv",
    )

    init_library_index(db_path)
    summary = refresh_library_index(db_path, tmp_path)

    assert summary["scanned"] == 1
    assert count_indexed_audio_files(db_path, tmp_path) == 1

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT downloaded_from, xml_exists, xml_core_complete, xml_has_deezer_id
            FROM library_files
            WHERE relative_path = ?
            """,
            ("Boards of Canada/Music Has the Right to Children/Roygbiv.flac",),
        ).fetchone()

    assert row == ("youtube", 1, 1, 0)
    assert list_audio_health_candidates(db_path, tmp_path) == [audio_path]

    record_audio_health_result(db_path, audio_path, status="ok")
    assert list_audio_health_candidates(db_path, tmp_path) == []


def test_refresh_library_index_marks_changed_files_for_revalidation(tmp_path) -> None:
    db_path = tmp_path / "library_index.db"
    audio_path = tmp_path / "Massive Attack - Mezzanine - Teardrop.flac"
    audio_path.write_bytes(b"v1")

    refresh_library_index(db_path, tmp_path)
    record_audio_health_result(db_path, audio_path, status="ok")

    audio_path.write_bytes(b"v2-with-more-bytes")
    refresh_library_index(db_path, tmp_path)

    assert list_audio_health_candidates(db_path, tmp_path) == [audio_path]
    assert list_audio_health_candidates(db_path, tmp_path, force_full=True) == [
        audio_path
    ]


def test_refresh_library_index_prunes_deleted_audio_files(tmp_path) -> None:
    db_path = tmp_path / "library_index.db"
    audio_path = tmp_path / "Massive Attack" / "Mezzanine" / "Teardrop.flac"
    audio_path.parent.mkdir(parents=True, exist_ok=True)
    audio_path.write_bytes(b"v1")

    refresh_library_index(db_path, tmp_path)
    assert count_indexed_audio_files(db_path, tmp_path) == 1

    audio_path.unlink()
    refresh_library_index(db_path, tmp_path)

    assert count_indexed_audio_files(db_path, tmp_path) == 0

    with sqlite3.connect(db_path) as conn:
        row = conn.execute("SELECT COUNT(*) FROM library_files").fetchone()

    assert row == (0,)


def test_refresh_library_index_limit_runs_partial_refresh_without_marking_unseen_files_missing(
    tmp_path,
) -> None:
    db_path = tmp_path / "library_index.db"
    first_audio = tmp_path / "Autechre" / "Amber" / "Nine.flac"
    second_audio = tmp_path / "Boards of Canada" / "Geogaddi" / "1969.flac"
    first_audio.parent.mkdir(parents=True, exist_ok=True)
    second_audio.parent.mkdir(parents=True, exist_ok=True)
    first_audio.write_bytes(b"first")
    second_audio.write_bytes(b"second")

    refresh_library_index(db_path, tmp_path)

    summary = refresh_library_index(db_path, tmp_path, limit=1)

    assert summary["partial"] == 1
    assert summary["limit"] == 1
    assert summary["scanned"] == 1
    assert count_indexed_audio_files(db_path, tmp_path) == 2

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT relative_path, file_missing FROM library_files ORDER BY relative_path ASC"
        ).fetchall()

    assert rows == [
        ("Autechre/Amber/Nine.flac", 0),
        ("Boards of Canada/Geogaddi/1969.flac", 0),
    ]


def test_refresh_library_index_limit_upserts_existing_xml_sidecars(tmp_path) -> None:
    db_path = tmp_path / "library_index.db"
    audio_path = tmp_path / "Burial" / "Untrue" / "Archangel.flac"
    audio_path.parent.mkdir(parents=True, exist_ok=True)
    audio_path.write_bytes(b"audio")
    write_song_metadata_xml(
        audio_path,
        title="Archangel",
        artist="Burial",
        album="Untrue",
        provider="deezer",
        downloaded_from="deezer",
        source="https://www.deezer.com/track/1",
    )

    refresh_library_index(db_path, tmp_path)
    summary = refresh_library_index(db_path, tmp_path, limit=1)

    assert summary["partial"] == 1

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM library_xml_sidecars WHERE root_path = ?",
            (str(tmp_path.resolve()),),
        ).fetchone()

    assert row == (1,)


def test_refresh_library_index_can_skip_xml_sidecar_scan(tmp_path) -> None:
    db_path = tmp_path / "library_index.db"
    audio_path = tmp_path / "Burial" / "Untrue" / "Archangel.flac"
    audio_path.parent.mkdir(parents=True, exist_ok=True)
    audio_path.write_bytes(b"audio")
    audio_path.with_suffix(".xml").write_text(
        "<song><title>Archangel</title></song>",
        encoding="utf-8",
    )

    summary = refresh_library_index(db_path, tmp_path, scan_xml_sidecars=False)

    assert summary["xml_scanned"] == 0
    assert summary["xml_scan_skipped"] == 1

    with sqlite3.connect(db_path) as conn:
        row = conn.execute("SELECT COUNT(*) FROM library_xml_sidecars").fetchone()

    assert row == (0,)


def test_record_audio_health_result_upserts_missing_audio_rows(tmp_path) -> None:
    db_path = tmp_path / "library_index.db"
    audio_path = tmp_path / "Massive Attack" / "Mezzanine" / "Teardrop.flac"
    audio_path.parent.mkdir(parents=True, exist_ok=True)
    audio_path.write_bytes(b"audio")

    record_audio_health_result(db_path, audio_path, status="ok", root=tmp_path)

    assert count_indexed_audio_files(db_path, tmp_path) == 1
    assert list_audio_health_candidates(db_path, tmp_path) == []

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT audio_health_status, file_missing FROM library_files WHERE audio_path = ?",
            (str(audio_path.resolve()),),
        ).fetchone()

    assert row == ("ok", 0)


def test_library_index_reports_missing_incomplete_and_orphaned_xml(tmp_path) -> None:
    db_path = tmp_path / "library_index.db"

    missing_xml_audio = tmp_path / "Air" / "Moon Safari" / "La Femme d'Argent.flac"
    missing_xml_audio.parent.mkdir(parents=True, exist_ok=True)
    missing_xml_audio.write_bytes(b"audio-1")

    incomplete_audio = tmp_path / "Portishead" / "Dummy" / "Sour Times.flac"
    incomplete_audio.parent.mkdir(parents=True, exist_ok=True)
    incomplete_audio.write_bytes(b"audio-2")
    incomplete_audio.with_suffix(".xml").write_text(
        """<?xml version=\"1.0\" encoding=\"utf-8\"?>
<song>
  <title>Sour Times</title>
  <performingartist>Portishead</performingartist>
  <albumtitle>Dummy</albumtitle>
  <downloadedfrom>deezer</downloadedfrom>
</song>
""",
        encoding="utf-8",
    )

    orphan_xml = tmp_path / "Orphaned.xml"
    orphan_xml.write_text("<song><title>Missing Audio</title></song>", encoding="utf-8")

    refresh_library_index(db_path, tmp_path)

    record_musicbrainz_verification(db_path, incomplete_audio, root=tmp_path)

    assert list_missing_xml_audio_paths(db_path, tmp_path) == [missing_xml_audio]
    assert list_incomplete_xml_pairs(db_path, tmp_path) == [
        (incomplete_audio.with_suffix(".xml"), incomplete_audio)
    ]
    assert list_orphaned_xml_paths(db_path, tmp_path) == [orphan_xml]
    assert list_xml_id_repair_candidates(db_path, tmp_path) == []

    counts = get_library_report_counts(db_path, tmp_path)
    assert counts["musicbrainz_pending"] == 1
    assert counts["musicbrainz_stale"] == 0
    assert counts["accepted_as_is"] == 0
    assert counts["missing_xml"] == 1
    assert counts["incomplete_xml"] == 1
    assert counts["orphaned_xml"] == 1

    missing_items = list_library_report_items(
        db_path,
        tmp_path,
        report_filter="missing-xml",
    )
    assert missing_items[0]["path"] == "Air/Moon Safari/La Femme d'Argent.flac"

    pending_items = list_library_report_items(
        db_path,
        tmp_path,
        report_filter="musicbrainz-pending",
    )
    assert pending_items[0]["path"] == "Air/Moon Safari/La Femme d'Argent.flac"
    assert pending_items[0]["badge"] == "unverified"


def test_library_reports_musicbrainz_stale_and_accepted_as_is(tmp_path) -> None:
    db_path = tmp_path / "library_index.db"
    stale_audio = tmp_path / "Orbital" / "In Sides" / "Stale Track.flac"
    kept_audio = tmp_path / "Orbital" / "In Sides" / "Kept Track.flac"
    stale_audio.parent.mkdir(parents=True, exist_ok=True)
    stale_audio.write_bytes(b"stale-v1")
    kept_audio.write_bytes(b"kept-audio")

    refresh_library_index(db_path, tmp_path)
    record_musicbrainz_verification(db_path, stale_audio, root=tmp_path)

    stale_audio.write_bytes(b"stale-v2")
    refresh_library_index(db_path, tmp_path)

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE library_files SET identify_audio_review_status = ? WHERE audio_path = ?",
            ("accepted-as-is", str(kept_audio)),
        )
        conn.commit()

    counts = get_library_report_counts(db_path, tmp_path)

    assert counts["musicbrainz_pending"] == 1
    assert counts["musicbrainz_stale"] == 1
    assert counts["accepted_as_is"] == 1

    pending_items = list_library_report_items(
        db_path,
        tmp_path,
        report_filter="musicbrainz-pending",
    )
    assert pending_items == [
        {
            "path": "Orbital/In Sides/Stale Track.flac",
            "detail": "File changed after the last MusicBrainz verification.",
            "badge": "stale",
        }
    ]

    kept_items = list_library_report_items(
        db_path,
        tmp_path,
        report_filter="accepted-as-is",
    )
    assert kept_items == [
        {
            "path": "Orbital/In Sides/Kept Track.flac",
            "detail": "Accepted during fingerprint review without changing tags or XML.",
            "badge": "kept",
        }
    ]


def test_library_index_lists_tag_fix_candidates_from_embedded_tags(tmp_path) -> None:
    db_path = tmp_path / "library_index.db"
    normal_audio = (
        tmp_path / "Orbital" / "In Sides" / "Orbital - In Sides - 01 - The Moebius.flac"
    )
    normal_audio.parent.mkdir(parents=True, exist_ok=True)
    normal_audio.write_bytes(b"audio-normal")
    va_audio = (
        tmp_path
        / "Various Artists"
        / "Warp Sampler"
        / "Autechre - Warp Sampler - 02 - Cichli.flac"
    )
    va_audio.parent.mkdir(parents=True, exist_ok=True)
    va_audio.write_bytes(b"audio-va")

    refresh_library_index(db_path, tmp_path)

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE library_files SET embedded_artist = ?, embedded_albumartist = ? "
            "WHERE audio_path = ?",
            ("Wrong Artist", "Wrong Album Artist", str(normal_audio)),
        )
        conn.execute(
            "UPDATE library_files SET embedded_artist = ?, embedded_albumartist = ? "
            "WHERE audio_path = ?",
            ("Various Artists", "Wrong Album Artist", str(va_audio)),
        )
        conn.commit()

    assert list_tag_fix_candidates(db_path, tmp_path) == [normal_audio, va_audio]


def test_library_index_lists_musicbrainz_tag_candidates_for_missing_track_numbers(
    tmp_path,
) -> None:
    db_path = tmp_path / "library_index.db"
    audio_path = tmp_path / "Burial" / "Untrue" / "Archangel.flac"
    audio_path.parent.mkdir(parents=True, exist_ok=True)
    audio_path.write_bytes(b"audio")

    refresh_library_index(db_path, tmp_path)

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE library_files SET embedded_title = ?, embedded_artist = ?, "
            "embedded_album = ?, embedded_albumartist = ?, "
            "embedded_track_number = ?, embedded_musicbrainz_album_id = ?, "
            "embedded_musicbrainz_artist_id = ?, "
            "embedded_musicbrainz_albumartist_id = ?, "
            "embedded_musicbrainz_track_id = ? "
            "WHERE audio_path = ?",
            (
                "Archangel",
                "Burial",
                "Untrue",
                "Burial",
                "0",
                "release-456",
                "artist-789",
                "albumartist-789",
                "",
                str(audio_path),
            ),
        )
        conn.commit()

    assert list_musicbrainz_tag_candidates(db_path, tmp_path) == [audio_path]


def test_library_index_lists_musicbrainz_tag_candidates_for_missing_artist_ids(
    tmp_path,
) -> None:
    db_path = tmp_path / "library_index.db"
    audio_path = tmp_path / "Burial" / "Untrue" / "Archangel.flac"
    audio_path.parent.mkdir(parents=True, exist_ok=True)
    audio_path.write_bytes(b"audio")

    refresh_library_index(db_path, tmp_path)

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE library_files SET embedded_title = ?, embedded_artist = ?, "
            "embedded_album = ?, embedded_albumartist = ?, embedded_track_number = ?, "
            "embedded_musicbrainz_album_id = ?, embedded_musicbrainz_artist_id = ?, "
            "embedded_musicbrainz_albumartist_id = ?, embedded_musicbrainz_track_id = ? "
            "WHERE audio_path = ?",
            (
                "Archangel",
                "Burial",
                "Untrue",
                "Burial",
                "1",
                "release-456",
                "",
                "albumartist-789",
                "recording-123",
                str(audio_path),
            ),
        )
        conn.commit()

    assert list_musicbrainz_tag_candidates(db_path, tmp_path) == [audio_path]


def test_musicbrainz_candidates_require_verification_and_requeue_when_file_changes(
    tmp_path,
) -> None:
    db_path = tmp_path / "library_index.db"
    audio_path = tmp_path / "Burial" / "Untrue" / "Archangel.flac"
    audio_path.parent.mkdir(parents=True, exist_ok=True)
    audio_path.write_bytes(b"audio")

    refresh_library_index(db_path, tmp_path)

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE library_files SET embedded_title = ?, embedded_artist = ?, embedded_album = ?, "
            "embedded_albumartist = ?, embedded_track_number = ?, "
            "embedded_musicbrainz_album_id = ?, "
            "embedded_musicbrainz_artist_id = ?, embedded_musicbrainz_albumartist_id = ?, "
            "embedded_musicbrainz_track_id = ? WHERE audio_path = ?",
            (
                "Archangel",
                "Burial",
                "Untrue",
                "Burial",
                "1",
                "release-456",
                "artist-789",
                "albumartist-789",
                "recording-123",
                str(audio_path),
            ),
        )
        conn.commit()

    assert list_musicbrainz_tag_candidates(db_path, tmp_path) == [audio_path]

    record_musicbrainz_verification(db_path, audio_path, root=tmp_path)
    assert list_musicbrainz_tag_candidates(db_path, tmp_path) == []

    audio_path.write_bytes(b"audio-updated")
    refresh_library_index(db_path, tmp_path)

    assert list_musicbrainz_tag_candidates(db_path, tmp_path) == [audio_path]


def test_library_index_lists_structure_tag_candidates_for_missing_preliminary_fields(
    tmp_path,
) -> None:
    db_path = tmp_path / "library_index.db"
    audio_path = (
        tmp_path
        / "Various Artists"
        / "Warp Sampler"
        / "Autechre - Warp Sampler - 02 - Cichli.flac"
    )
    audio_path.parent.mkdir(parents=True, exist_ok=True)
    audio_path.write_bytes(b"audio")

    refresh_library_index(db_path, tmp_path)

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE library_files SET embedded_title = '', embedded_artist = '', "
            "embedded_album = '', "
            "embedded_albumartist = '', embedded_track_number = '' WHERE audio_path = ?",
            (str(audio_path),),
        )
        conn.commit()

    assert list_structure_tag_candidates(db_path, tmp_path) == [audio_path]
