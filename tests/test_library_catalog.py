from __future__ import annotations

import sqlite3

from app.services.library_catalog import (
    CATALOG_PAGE_SIZE,
    list_catalog_tracks,
    run_catalog_batch_action,
)
from app.services.library_index import refresh_library_index


def test_list_catalog_tracks_filters_missing_info(tmp_path) -> None:
    db_path = tmp_path / "library_index.db"
    audio_path = tmp_path / "Burial" / "Untrue" / "Archangel.flac"
    audio_path.parent.mkdir(parents=True, exist_ok=True)
    audio_path.write_bytes(b"audio")

    refresh_library_index(db_path, tmp_path)

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE library_files SET embedded_title = ?, embedded_artist = ?, embedded_album = ?, "
            "embedded_albumartist = ?, embedded_track_number = ? WHERE audio_path = ?",
            ("Archangel", "Burial", "Untrue", "Burial", "", str(audio_path)),
        )
        conn.commit()

    listing = list_catalog_tracks(
        db_path,
        tmp_path,
        issue_filter="missing-info",
        search="",
        sort_by="path",
        sort_dir="asc",
        page=1,
        per_page=50,
    )

    assert listing["total"] == 1
    assert listing["per_page"] == CATALOG_PAGE_SIZE
    assert listing["items"][0]["relative_path"] == "Burial/Untrue/Archangel.flac"
    assert any(
        badge["label"] == "Missing Info" for badge in listing["items"][0]["anomalies"]
    )


def test_list_catalog_tracks_filters_musicbrainz_pending(tmp_path) -> None:
    db_path = tmp_path / "library_index.db"
    audio_path = tmp_path / "Burial" / "Untrue" / "Archangel.flac"
    audio_path.parent.mkdir(parents=True, exist_ok=True)
    audio_path.write_bytes(b"audio")

    refresh_library_index(db_path, tmp_path)

    listing = list_catalog_tracks(
        db_path,
        tmp_path,
        issue_filter="musicbrainz-pending",
        search="",
        sort_by="path",
        sort_dir="asc",
        page=1,
        per_page=50,
    )

    assert listing["total"] == 1
    assert listing["items"][0]["musicbrainz_status"] == "unverified"
    assert listing["items"][0]["musicbrainz_label"] == "MB Unverified"


def test_run_catalog_batch_action_dispatches_selected_paths(
    tmp_path, monkeypatch
) -> None:
    data_dir = tmp_path / "data"
    audio_path = tmp_path / "Burial" / "Untrue" / "Archangel.flac"
    audio_path.parent.mkdir(parents=True, exist_ok=True)
    audio_path.write_bytes(b"audio")
    captured: dict[str, object] = {}

    class FakeModule:
        @staticmethod
        def check_library(root, *, dry_run=False, db_path=None, selected_paths=None):
            captured["root"] = root
            captured["dry_run"] = dry_run
            captured["db_path"] = db_path
            captured["selected_paths"] = selected_paths
            return (["SUMMARY  scanned=1  ok=1  warnings=0  errors=0"], 0)

    monkeypatch.setattr(
        "app.services.library_catalog._load_script_module",
        lambda name: FakeModule(),
    )
    monkeypatch.setattr(
        "app.services.library_catalog.refresh_library_index_for_paths",
        lambda db_path, root, audio_paths, scan_xml_sidecars=True: captured.setdefault(
            "refreshed_paths", list(audio_paths)
        ),
    )

    result = run_catalog_batch_action(
        {
            "NAVIDROME_MUSIC_ROOT": str(tmp_path),
            "LIBRARY_INDEX_DB_PATH": str(tmp_path / "library_index.db"),
            "DATA_DIR": str(data_dir),
        },
        action="check-audio",
        relative_paths=["Burial/Untrue/Archangel.flac"],
        dry_run=False,
    )

    assert result["exit_code"] == 0
    assert captured["dry_run"] is False
    assert captured["db_path"] == str(tmp_path / "library_index.db")
    assert captured["selected_paths"] == [audio_path]
    assert captured["refreshed_paths"] == [audio_path]


def test_run_catalog_batch_action_dispatches_audio_identification(
    tmp_path, monkeypatch
) -> None:
    data_dir = tmp_path / "data"
    audio_path = tmp_path / "Burial" / "Untrue" / "Archangel.flac"
    audio_path.parent.mkdir(parents=True, exist_ok=True)
    audio_path.write_bytes(b"audio")
    captured: dict[str, object] = {}

    class FakeModule:
        @staticmethod
        def identify_tracks_by_audio(
            root, *, dry_run=False, db_path=None, selected_paths=None
        ):
            captured["root"] = root
            captured["dry_run"] = dry_run
            captured["db_path"] = db_path
            captured["selected_paths"] = selected_paths
            return (["SUMMARY  scanned=1  updated=1  unresolved=0  failed=0"], 0)

    monkeypatch.setattr(
        "app.services.library_catalog._load_script_module",
        lambda name: FakeModule(),
    )
    monkeypatch.setattr(
        "app.services.library_catalog.refresh_library_index_for_paths",
        lambda db_path, root, audio_paths, scan_xml_sidecars=True: captured.setdefault(
            "refreshed_paths", list(audio_paths)
        ),
    )

    result = run_catalog_batch_action(
        {
            "NAVIDROME_MUSIC_ROOT": str(tmp_path),
            "LIBRARY_INDEX_DB_PATH": str(tmp_path / "library_index.db"),
            "DATA_DIR": str(data_dir),
        },
        action="identify-audio",
        relative_paths=["Burial/Untrue/Archangel.flac"],
        dry_run=False,
    )

    assert result["exit_code"] == 0
    assert captured["dry_run"] is False
    assert captured["db_path"] == str(tmp_path / "library_index.db")
    assert captured["selected_paths"] == [audio_path]
    assert captured["refreshed_paths"] == [audio_path]


def test_run_catalog_batch_action_dispatches_structure_identification(
    tmp_path,
    monkeypatch,
) -> None:
    data_dir = tmp_path / "data"
    audio_path = tmp_path / "Burial" / "Untrue" / "Archangel.flac"
    audio_path.parent.mkdir(parents=True, exist_ok=True)
    audio_path.write_bytes(b"audio")
    captured: dict[str, object] = {}

    class FakeModule:
        @staticmethod
        def identify_tracks_from_layout(
            root, *, dry_run=False, db_path=None, selected_paths=None
        ):
            captured["root"] = root
            captured["dry_run"] = dry_run
            captured["db_path"] = db_path
            captured["selected_paths"] = selected_paths
            return (["SUMMARY  scanned=1  updated=1  unchanged=0  failed=0"], 0)

    monkeypatch.setattr(
        "app.services.library_catalog._load_script_module",
        lambda name: FakeModule(),
    )
    monkeypatch.setattr(
        "app.services.library_catalog.refresh_library_index_for_paths",
        lambda db_path, root, audio_paths, scan_xml_sidecars=True: captured.setdefault(
            "refreshed_paths", list(audio_paths)
        ),
    )

    result = run_catalog_batch_action(
        {
            "NAVIDROME_MUSIC_ROOT": str(tmp_path),
            "LIBRARY_INDEX_DB_PATH": str(tmp_path / "library_index.db"),
            "DATA_DIR": str(data_dir),
        },
        action="identify-structure",
        relative_paths=["Burial/Untrue/Archangel.flac"],
        dry_run=False,
    )

    assert result["exit_code"] == 0
    assert captured["dry_run"] is False
    assert captured["db_path"] == str(tmp_path / "library_index.db")
    assert captured["selected_paths"] == [audio_path]
    assert captured["refreshed_paths"] == [audio_path]


def test_run_catalog_batch_action_rejects_more_than_page_size(tmp_path) -> None:
    relative_paths: list[str] = []
    for index in range(CATALOG_PAGE_SIZE + 1):
        audio_path = tmp_path / "Burial" / "Untrue" / f"Track {index:02}.flac"
        audio_path.parent.mkdir(parents=True, exist_ok=True)
        audio_path.write_bytes(b"audio")
        relative_paths.append(audio_path.relative_to(tmp_path).as_posix())

    try:
        run_catalog_batch_action(
            {
                "NAVIDROME_MUSIC_ROOT": str(tmp_path),
                "LIBRARY_INDEX_DB_PATH": str(tmp_path / "library_index.db"),
                "DATA_DIR": str(tmp_path / "data"),
            },
            action="check-audio",
            relative_paths=relative_paths,
            dry_run=True,
        )
    except ValueError as exc:
        assert str(CATALOG_PAGE_SIZE) in str(exc)
    else:
        raise AssertionError(
            "Expected oversized catalog batch selection to be rejected"
        )
