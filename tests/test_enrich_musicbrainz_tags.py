from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def _load_enrich_module():
    repo_root = Path(__file__).resolve().parents[1]
    module_path = repo_root / "scripts" / "enrich_musicbrainz_tags.py"
    spec = importlib.util.spec_from_file_location(
        "enrich_musicbrainz_tags_test_module",
        module_path,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_enrich_musicbrainz_tags_uses_directory_artist_fallback(
    tmp_path, monkeypatch
) -> None:
    enrich_module = _load_enrich_module()
    audio_path = tmp_path / "Burial" / "Untrue" / "Archangel.flac"
    audio_path.parent.mkdir(parents=True, exist_ok=True)
    audio_path.write_bytes(b"audio")
    captured_details: list[dict[str, object]] = []
    captured_scan_xml_sidecars: list[bool | None] = []

    class FakeListenBrainzService:
        @staticmethod
        def from_config(config):
            return FakeListenBrainzService()

        def lookup_recording_metadata(
            self, *, artist_name, recording_name, release_name=""
        ):
            return {}

    class FakeMusicBrainzService:
        @staticmethod
        def from_config(config):
            return FakeMusicBrainzService()

        def lookup_recording_details(self, **kwargs):
            if kwargs.get("artist_name") != "Burial":
                return {}
            return {
                "recording_mbid": "recording-123",
                "release_mbid": "release-456",
                "artist_mbid": "artist-789",
                "albumartist_mbid": "albumartist-789",
                "title": "Archangel",
                "artist": "Burial",
                "album": "Untrue",
                "albumartist": "Burial",
                "track_number": 1,
            }

    monkeypatch.setattr(enrich_module, "_HAS_MUTAGEN", True)
    monkeypatch.setattr(enrich_module, "ListenBrainzService", FakeListenBrainzService)
    monkeypatch.setattr(enrich_module, "MusicBrainzService", FakeMusicBrainzService)
    monkeypatch.setattr(
        enrich_module,
        "refresh_library_index",
        lambda *args, **kwargs: captured_scan_xml_sidecars.append(
            kwargs.get("scan_xml_sidecars")
        )
        or {
            "scanned": 1,
            "changed": 1,
            "unchanged": 0,
            "xml_scanned": 0,
            "xml_changed": 0,
            "embedded_changed": 1,
            "partial": 0,
            "limit": 0,
            "xml_scan_skipped": int(kwargs.get("scan_xml_sidecars") is False),
        },
    )
    monkeypatch.setattr(
        enrich_module,
        "refresh_library_index_for_paths",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        enrich_module,
        "list_musicbrainz_tag_candidates",
        lambda *args, **kwargs: [audio_path],
    )
    monkeypatch.setattr(
        enrich_module,
        "load_embedded_audio_metadata",
        lambda path: {
            "title": "Archangel",
            "artist": "Wrong Artist",
            "album": "Untrue",
            "albumartist": "Wrong Artist",
            "track_number": "0",
            "musicbrainz_album_id": "",
            "musicbrainz_artist_id": "",
            "musicbrainz_albumartist_id": "",
            "musicbrainz_track_id": "",
        },
    )
    monkeypatch.setattr(enrich_module, "load_song_metadata_xml", lambda path: {})
    monkeypatch.setattr(
        enrich_module,
        "_write_tags",
        lambda path, details: captured_details.append(details),
    )

    lines, exit_code = enrich_module.enrich_musicbrainz_tags(tmp_path, dry_run=False)

    assert exit_code == 0
    assert captured_scan_xml_sidecars == [False, False]
    assert captured_details and captured_details[0]["artist"] == "Burial"
    assert captured_details[0]["artist_mbid"] == "artist-789"
    assert captured_details[0]["albumartist_mbid"] == "albumartist-789"
    assert any("UPDATED:" in line for line in lines)


def test_enrich_musicbrainz_tags_reports_unresolved_files(
    tmp_path, monkeypatch
) -> None:
    enrich_module = _load_enrich_module()
    audio_path = tmp_path / "Burial" / "Untrue" / "Archangel.flac"
    audio_path.parent.mkdir(parents=True, exist_ok=True)
    audio_path.write_bytes(b"audio")
    captured_scan_xml_sidecars: list[bool | None] = []

    class FakeListenBrainzService:
        @staticmethod
        def from_config(config):
            return FakeListenBrainzService()

        def lookup_recording_metadata(
            self, *, artist_name, recording_name, release_name=""
        ):
            return {}

    class FakeMusicBrainzService:
        @staticmethod
        def from_config(config):
            return FakeMusicBrainzService()

        def lookup_recording_details(self, **kwargs):
            return {}

    monkeypatch.setattr(enrich_module, "_HAS_MUTAGEN", True)
    monkeypatch.setattr(enrich_module, "ListenBrainzService", FakeListenBrainzService)
    monkeypatch.setattr(enrich_module, "MusicBrainzService", FakeMusicBrainzService)
    monkeypatch.setattr(
        enrich_module,
        "refresh_library_index",
        lambda *args, **kwargs: captured_scan_xml_sidecars.append(
            kwargs.get("scan_xml_sidecars")
        )
        or {
            "scanned": 1,
            "changed": 1,
            "unchanged": 0,
            "xml_scanned": 0,
            "xml_changed": 0,
            "embedded_changed": 1,
            "partial": 0,
            "limit": 0,
            "xml_scan_skipped": int(kwargs.get("scan_xml_sidecars") is False),
        },
    )
    monkeypatch.setattr(
        enrich_module,
        "refresh_library_index_for_paths",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        enrich_module,
        "list_musicbrainz_tag_candidates",
        lambda *args, **kwargs: [audio_path],
    )
    monkeypatch.setattr(
        enrich_module,
        "load_embedded_audio_metadata",
        lambda path: {
            "title": "Archangel",
            "artist": "Unknown",
            "album": "Untrue",
            "albumartist": "Unknown",
            "track_number": "",
            "musicbrainz_album_id": "",
            "musicbrainz_artist_id": "",
            "musicbrainz_albumartist_id": "",
            "musicbrainz_track_id": "",
        },
    )
    monkeypatch.setattr(enrich_module, "load_song_metadata_xml", lambda path: {})

    lines, exit_code = enrich_module.enrich_musicbrainz_tags(tmp_path, dry_run=False)

    assert exit_code == 0
    assert captured_scan_xml_sidecars == [False, False]
    assert any("Unresolved MusicBrainz matches:" in line for line in lines)
    assert any("MISSING: Burial" in line and "Archangel.flac" in line for line in lines)


def test_enrich_musicbrainz_tags_falls_back_to_direct_musicbrainz_metadata(
    tmp_path,
    monkeypatch,
) -> None:
    enrich_module = _load_enrich_module()
    audio_path = tmp_path / "Unknown" / "Album" / "missing.flac"
    audio_path.parent.mkdir(parents=True, exist_ok=True)
    audio_path.write_bytes(b"audio")
    captured_details: list[dict[str, object]] = []

    class FakeListenBrainzService:
        @staticmethod
        def from_config(config):
            return FakeListenBrainzService()

        def lookup_recording_metadata(
            self, *, artist_name, recording_name, release_name=""
        ):
            return {}

    class FakeMusicBrainzService:
        @staticmethod
        def from_config(config):
            return FakeMusicBrainzService()

        def lookup_recording_details(self, **kwargs):
            return {}

    monkeypatch.setattr(enrich_module, "_HAS_MUTAGEN", True)
    monkeypatch.setattr(enrich_module, "ListenBrainzService", FakeListenBrainzService)
    monkeypatch.setattr(enrich_module, "MusicBrainzService", FakeMusicBrainzService)
    monkeypatch.setattr(
        enrich_module,
        "refresh_library_index",
        lambda *args, **kwargs: {
            "scanned": 1,
            "changed": 1,
            "unchanged": 0,
            "xml_scanned": 0,
            "xml_changed": 0,
            "embedded_changed": 1,
            "partial": 0,
            "limit": 0,
            "xml_scan_skipped": int(kwargs.get("scan_xml_sidecars") is False),
        },
    )
    monkeypatch.setattr(
        enrich_module,
        "refresh_library_index_for_paths",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        enrich_module,
        "list_musicbrainz_tag_candidates",
        lambda *args, **kwargs: [audio_path],
    )
    monkeypatch.setattr(
        enrich_module,
        "load_embedded_audio_metadata",
        lambda path: {
            "title": "",
            "artist": "",
            "album": "",
            "albumartist": "",
            "track_number": "",
            "musicbrainz_album_id": "",
            "musicbrainz_artist_id": "",
            "musicbrainz_albumartist_id": "",
            "musicbrainz_track_id": "",
        },
    )
    monkeypatch.setattr(enrich_module, "load_song_metadata_xml", lambda path: {})
    monkeypatch.setattr(
        enrich_module,
        "lookup_musicbrainz_metadata_match",
        lambda *args, **kwargs: {
            "recording_mbid": "recording-123",
            "release_mbid": "release-456",
            "artist_mbid": "artist-789",
            "albumartist_mbid": "albumartist-789",
            "title": "Recovered Song",
            "artist": "Recovered Artist",
            "album": "Recovered Album",
            "albumartist": "Recovered Artist",
            "track_number": 3,
        },
    )
    monkeypatch.setattr(
        enrich_module,
        "_write_tags",
        lambda path, details: captured_details.append(details),
    )

    lines, exit_code = enrich_module.enrich_musicbrainz_tags(tmp_path, dry_run=False)

    assert exit_code == 0
    assert captured_details and captured_details[0]["recording_mbid"] == "recording-123"
    assert any("source=musicbrainz-metadata" in line for line in lines)
