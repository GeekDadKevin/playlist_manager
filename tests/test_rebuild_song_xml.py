from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

from app.services.library_index import refresh_library_index


def _load_rebuild_module():
    repo_root = Path(__file__).resolve().parents[1]
    module_path = repo_root / "scripts" / "rebuild_song_xml.py"
    spec = importlib.util.spec_from_file_location("rebuild_song_xml_test_module", module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_rebuild_updates_existing_xml_with_recovered_ids(tmp_path, monkeypatch) -> None:
    rebuild_module = _load_rebuild_module()

    audio_path = tmp_path / "Massive Attack" / "Mezzanine" / "Teardrop.flac"
    audio_path.parent.mkdir(parents=True)
    audio_path.write_bytes(b"fake-flac")

    xml_path = audio_path.with_suffix(".xml")
    xml_path.write_text(
        """<?xml version=\"1.0\" encoding=\"utf-8\"?>
<song>
  <title>Teardrop</title>
  <performingartist>Massive Attack</performingartist>
  <albumtitle>Mezzanine</albumtitle>
</song>
""",
        encoding="utf-8",
    )

    monkeypatch.setattr(rebuild_module, "_HAS_MUTAGEN", False)
    monkeypatch.setattr(
        rebuild_module,
        "load_embedded_audio_metadata",
        lambda path: {
            "title": "Teardrop",
            "artist": "Massive Attack",
            "album": "Mezzanine",
            "albumartist": "Massive Attack",
            "musicbrainz_track_id": "abcd1234-0000-1111-2222-abcdefabcdef",
            "deezer_id": "12345",
            "deezer_artist_id": "99",
            "deezer_album_id": "77",
            "deezer_link": "https://www.deezer.com/track/12345",
        },
    )

    rebuild_module.rebuild(tmp_path, dry_run=False)

    updated_xml = xml_path.read_text(encoding="utf-8")
    assert (
        "<musicbrainztrackid>abcd1234-0000-1111-2222-abcdefabcdef</musicbrainztrackid>"
        in updated_xml
    )
    assert "<deezerid>12345</deezerid>" in updated_xml
    assert "<deezerartistid>99</deezerartistid>" in updated_xml
    assert "<deezeralbumid>77</deezeralbumid>" in updated_xml


def test_rebuild_uses_index_to_find_missing_and_orphaned_xml(tmp_path, monkeypatch) -> None:
    rebuild_module = _load_rebuild_module()

    missing_audio = tmp_path / "Orbital" / "In Sides" / "The Girl with the Sun in Her Head.flac"
    missing_audio.parent.mkdir(parents=True, exist_ok=True)
    missing_audio.write_bytes(b"fake-flac")
    orphan_xml = tmp_path / "orphan.xml"
    orphan_xml.write_text("<song><title>orphan</title></song>", encoding="utf-8")

    monkeypatch.setattr(rebuild_module, "_HAS_MUTAGEN", False)
    monkeypatch.setattr(
        rebuild_module,
        "load_embedded_audio_metadata",
        lambda path: {
            "title": "The Girl with the Sun in Her Head",
            "artist": "Orbital",
            "album": "In Sides",
            "albumartist": "Orbital",
            "musicbrainz_track_id": "",
            "deezer_id": "",
            "deezer_artist_id": "",
            "deezer_album_id": "",
            "deezer_link": "",
        },
    )

    db_path = tmp_path / "library_index.db"
    refresh_library_index(db_path, tmp_path)
    rebuild_module.rebuild(tmp_path, dry_run=False, db_path=db_path)

    assert not orphan_xml.exists()
    assert missing_audio.with_suffix(".xml").exists()


def test_rebuild_refresh_respects_limit(tmp_path, monkeypatch) -> None:
    rebuild_module = _load_rebuild_module()
    captured_limits: list[int | None] = []

    def fake_refresh_library_index(db_path_value, root, **kwargs):
        captured_limits.append(kwargs.get("limit"))
        return {
            "scanned": 0,
            "changed": 0,
            "unchanged": 0,
            "xml_scanned": 0,
            "xml_changed": 0,
            "embedded_changed": 0,
            "partial": int(kwargs.get("limit") is not None),
            "limit": kwargs.get("limit") or 0,
            "xml_scan_skipped": 0,
        }

    monkeypatch.setattr(rebuild_module, "refresh_library_index", fake_refresh_library_index)
    monkeypatch.setattr(rebuild_module, "list_orphaned_xml_paths", lambda *args, **kwargs: [])
    monkeypatch.setattr(rebuild_module, "list_missing_xml_audio_paths", lambda *args, **kwargs: [])
    monkeypatch.setattr(rebuild_module, "list_incomplete_xml_pairs", lambda *args, **kwargs: [])

    rebuild_module.rebuild(tmp_path, dry_run=False, limit=5, db_path=tmp_path / "library_index.db")

    assert captured_limits == [5, 5]

