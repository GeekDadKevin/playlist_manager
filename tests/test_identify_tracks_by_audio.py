from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def _load_identify_module():
    repo_root = Path(__file__).resolve().parents[1]
    module_path = repo_root / "scripts" / "identify_tracks_by_audio.py"
    spec = importlib.util.spec_from_file_location(
        "identify_tracks_by_audio_test_module",
        module_path,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_identify_tracks_by_audio_updates_xml_for_selected_paths(
    tmp_path, monkeypatch
) -> None:
    identify_module = _load_identify_module()
    audio_path = tmp_path / "Massive Attack" / "Mezzanine" / "Teardrop.flac"
    audio_path.parent.mkdir(parents=True, exist_ok=True)
    audio_path.write_bytes(b"audio")
    captured: dict[str, object] = {}

    class FakeService:
        api_key = "demo-key"
        fpcalc_path = "fpcalc"

        def identify_track(self, audio_path, *, musicbrainz_service=None):
            return {
                "accepted": True,
                "match": {
                    "recording_mbid": "recording-123",
                    "release_mbid": "release-456",
                    "release_group_mbid": "group-789",
                    "title": "Teardrop",
                    "artist": "Massive Attack",
                    "album": "Mezzanine",
                    "albumartist": "Massive Attack",
                    "artist_sort": "Massive Attack",
                    "albumartist_sort": "Massive Attack",
                    "artist_mbid": "artist-1",
                    "albumartist_mbid": "artist-1",
                    "track_number": 10,
                    "track_total": 11,
                    "disc_number": 1,
                    "disc_total": 1,
                    "date": "1998-04-20",
                    "original_date": "1998-04-20",
                    "genre": "Trip Hop",
                    "isrc": "GBBKS9801234",
                    "barcode": "724384559524",
                    "label": "Virgin",
                    "catalog_number": "WBRCD2",
                    "media_format": "CD",
                    "release_country": "GB",
                    "release_status": "Official",
                    "release_type": "Album",
                    "release_secondary_types": "Compilation",
                    "language": "eng",
                    "script": "Latn",
                    "recording_disambiguation": "album version",
                    "album_disambiguation": "2019 remaster",
                    "acoustid_score": 0.97,
                },
            }

    monkeypatch.setattr(
        identify_module.AcoustIdService,
        "from_config",
        lambda config: FakeService(),
    )
    monkeypatch.setattr(
        identify_module,
        "_details_need_update",
        lambda audio_path, details: True,
    )
    monkeypatch.setattr(
        identify_module,
        "_write_tags",
        lambda audio_path, details: captured.setdefault(
            "written_audio", str(audio_path)
        ),
    )
    monkeypatch.setattr(
        identify_module,
        "refresh_library_index_for_paths",
        lambda db_path, root, audio_paths, scan_xml_sidecars=True: captured.setdefault(
            "refreshed_paths", list(audio_paths)
        ),
    )
    monkeypatch.setattr(
        identify_module,
        "record_library_tool_run",
        lambda *args, **kwargs: captured.setdefault("recorded", True),
    )

    lines, exit_code = identify_module.identify_tracks_by_audio(
        tmp_path,
        dry_run=False,
        db_path=tmp_path / "library_index.db",
        selected_paths=[audio_path],
    )

    assert exit_code == 0
    assert any("UPDATED:" in line for line in lines)
    assert captured["written_audio"] == str(audio_path)
    assert captured["refreshed_paths"] == [audio_path]
    metadata_xml = audio_path.with_suffix(".xml").read_text(encoding="utf-8")
    assert "<musicbrainztrackid>recording-123</musicbrainztrackid>" in metadata_xml
    assert "<albumartist>Massive Attack</albumartist>" in metadata_xml
    assert (
        "<musicbrainzreleasegroupid>group-789</musicbrainzreleasegroupid>"
        in metadata_xml
    )
    assert "<genre>Trip Hop</genre>" in metadata_xml
    assert "<barcode>724384559524</barcode>" in metadata_xml


def test_identify_tracks_by_audio_reports_low_confidence_matches(
    tmp_path, monkeypatch
) -> None:
    identify_module = _load_identify_module()
    audio_path = tmp_path / "Unknown" / "Unknown Album" / "mystery.flac"
    audio_path.parent.mkdir(parents=True, exist_ok=True)
    audio_path.write_bytes(b"audio")
    captured: dict[str, object] = {}

    class FakeService:
        api_key = "demo-key"
        fpcalc_path = "fpcalc"

        def identify_track(self, audio_path, *, musicbrainz_service=None):
            return {
                "accepted": False,
                "match": {
                    "recording_mbid": "recording-low",
                    "title": "Possible Song",
                    "artist": "Possible Artist",
                    "acoustid_score": 0.41,
                },
            }

    monkeypatch.setattr(
        identify_module.AcoustIdService,
        "from_config",
        lambda config: FakeService(),
    )
    monkeypatch.setattr(
        identify_module,
        "lookup_musicbrainz_metadata_match",
        lambda *args, **kwargs: {},
    )
    monkeypatch.setattr(
        identify_module,
        "record_library_tool_run",
        lambda *args, **kwargs: captured.setdefault("run", kwargs),
    )

    lines, exit_code = identify_module.identify_tracks_by_audio(
        tmp_path,
        dry_run=True,
        db_path=tmp_path / "library_index.db",
        selected_paths=[audio_path],
    )

    assert exit_code == 0
    assert any("low-confidence fingerprint match" in line for line in lines)
    review = captured["run"]["result"]["review"]
    assert review["low_confidence_count"] == 1
    assert review["no_match_count"] == 0
    assert (
        review["low_confidence_items"][0]["relative_path"]
        == "Unknown/Unknown Album/mystery.flac"
    )
    assert review["low_confidence_items"][0]["match_title"] == "Possible Song"
    assert review["low_confidence_items"][0]["match_albumartist"] == "Possible Artist"


def test_identify_tracks_by_audio_records_no_match_items(tmp_path, monkeypatch) -> None:
    identify_module = _load_identify_module()
    audio_path = tmp_path / "Unknown" / "Missing Album" / "ghost.flac"
    audio_path.parent.mkdir(parents=True, exist_ok=True)
    audio_path.write_bytes(b"audio")
    captured: dict[str, object] = {}

    class FakeService:
        api_key = "demo-key"
        fpcalc_path = "fpcalc"

        def identify_track(self, audio_path, *, musicbrainz_service=None):
            return {"accepted": False, "match": {}}

    monkeypatch.setattr(
        identify_module.AcoustIdService,
        "from_config",
        lambda config: FakeService(),
    )
    monkeypatch.setattr(
        identify_module,
        "lookup_musicbrainz_metadata_match",
        lambda *args, **kwargs: {},
    )
    monkeypatch.setattr(
        identify_module,
        "record_library_tool_run",
        lambda *args, **kwargs: captured.setdefault("run", kwargs),
    )

    lines, exit_code = identify_module.identify_tracks_by_audio(
        tmp_path,
        dry_run=True,
        db_path=tmp_path / "library_index.db",
        selected_paths=[audio_path],
    )

    assert exit_code == 0
    assert any("no fingerprint or metadata match found" in line for line in lines)
    review = captured["run"]["result"]["review"]
    assert review["low_confidence_count"] == 0
    assert review["no_match_count"] == 1
    assert (
        review["no_match_items"][0]["relative_path"]
        == "Unknown/Missing Album/ghost.flac"
    )


def test_identify_tracks_by_audio_falls_back_to_musicbrainz_metadata(
    tmp_path, monkeypatch
) -> None:
    identify_module = _load_identify_module()
    audio_path = tmp_path / "Unknown" / "Album" / "missing.flac"
    audio_path.parent.mkdir(parents=True, exist_ok=True)
    audio_path.write_bytes(b"audio")
    captured: dict[str, object] = {}

    class FakeService:
        api_key = "demo-key"
        fpcalc_path = "fpcalc"

        def identify_track(self, audio_path, *, musicbrainz_service=None):
            return {"accepted": False, "match": {}}

    monkeypatch.setattr(
        identify_module.AcoustIdService,
        "from_config",
        lambda config: FakeService(),
    )
    monkeypatch.setattr(
        identify_module,
        "lookup_musicbrainz_metadata_match",
        lambda *args, **kwargs: {
            "recording_mbid": "recording-123",
            "release_mbid": "release-456",
            "title": "Recovered Song",
            "artist": "Recovered Artist",
            "album": "Recovered Album",
            "albumartist": "Recovered Artist",
            "artist_mbid": "artist-1",
            "albumartist_mbid": "artist-1",
            "track_number": 3,
        },
    )
    monkeypatch.setattr(
        identify_module,
        "_details_need_update",
        lambda _audio_path, _details: True,
    )
    monkeypatch.setattr(
        identify_module,
        "record_library_tool_run",
        lambda *args, **kwargs: captured.setdefault("run", kwargs),
    )

    lines, exit_code = identify_module.identify_tracks_by_audio(
        tmp_path,
        dry_run=True,
        db_path=tmp_path / "library_index.db",
        selected_paths=[audio_path],
    )

    assert exit_code == 0
    assert any("source=musicbrainz-metadata" in line for line in lines)
    review = captured["run"]["result"]["review"]
    assert review["low_confidence_count"] == 0
    assert review["no_match_count"] == 0


def test_identify_tracks_by_audio_downgrades_suspicious_match(
    tmp_path, monkeypatch
) -> None:
    identify_module = _load_identify_module()
    audio_path = tmp_path / "2 LIVE CREW" / "Banned In the USA" / "06 - Strip Club.flac"
    audio_path.parent.mkdir(parents=True, exist_ok=True)
    audio_path.write_bytes(b"audio")
    captured: dict[str, object] = {}

    class FakeService:
        api_key = "demo-key"
        fpcalc_path = "fpcalc"

        def identify_track(self, audio_path, *, musicbrainz_service=None):
            return {
                "accepted": True,
                "match": {
                    "recording_mbid": "recording-wrong",
                    "release_mbid": "release-wrong",
                    "title": "News Flash Nation by Storm",
                    "artist": "Luke The 2 Live Crew",
                    "album": "Banned in the U.S.A.",
                    "albumartist": "Luke The 2 Live Crew",
                    "artist_mbid": "artist-2live",
                    "albumartist_mbid": "artist-2live",
                    "track_number": 6,
                    "acoustid_score": 0.98,
                },
            }

    monkeypatch.setattr(
        identify_module.AcoustIdService,
        "from_config",
        lambda config: FakeService(),
    )
    monkeypatch.setattr(
        identify_module,
        "load_embedded_audio_metadata",
        lambda _audio_path: {
            "title": "Strip Club",
            "artist": "2 LIVE CREW",
            "album": "Banned In the USA",
            "albumartist": "2 LIVE CREW",
        },
    )
    monkeypatch.setattr(
        identify_module,
        "load_song_metadata_xml",
        lambda _xml_path: {},
    )
    monkeypatch.setattr(
        identify_module,
        "record_library_tool_run",
        lambda *args, **kwargs: captured.setdefault("run", kwargs),
    )

    lines, exit_code = identify_module.identify_tracks_by_audio(
        tmp_path,
        dry_run=True,
        db_path=tmp_path / "library_index.db",
        selected_paths=[audio_path],
    )

    assert exit_code == 0
    assert any("needs manual review" in line for line in lines)
    review = captured["run"]["result"]["review"]
    assert review["low_confidence_count"] == 1
    assert review["low_confidence_items"][0]["reason"] == "guardrail"
    assert review["low_confidence_items"][0]["reference_title"] == "Strip Club"


def test_identify_tracks_by_audio_accepts_match_when_only_filename_guess_disagrees(
    tmp_path,
    monkeypatch,
) -> None:
    identify_module = _load_identify_module()
    audio_path = tmp_path / "Wrong Artist" / "Wrong Album" / "Totally Wrong Title.flac"
    audio_path.parent.mkdir(parents=True, exist_ok=True)
    audio_path.write_bytes(b"audio")
    captured: dict[str, object] = {}

    class FakeService:
        api_key = "demo-key"
        fpcalc_path = "fpcalc"

        def identify_track(self, audio_path, *, musicbrainz_service=None):
            return {
                "accepted": True,
                "match": {
                    "recording_mbid": "recording-123",
                    "release_mbid": "release-456",
                    "title": "Teardrop",
                    "artist": "Massive Attack",
                    "album": "Mezzanine",
                    "albumartist": "Massive Attack",
                    "artist_mbid": "artist-1",
                    "albumartist_mbid": "artist-1",
                    "track_number": 10,
                    "acoustid_score": 0.98,
                },
            }

    monkeypatch.setattr(
        identify_module.AcoustIdService,
        "from_config",
        lambda config: FakeService(),
    )
    monkeypatch.setattr(
        identify_module,
        "load_embedded_audio_metadata",
        lambda _audio_path: {},
    )
    monkeypatch.setattr(
        identify_module,
        "load_song_metadata_xml",
        lambda _xml_path: {},
    )
    monkeypatch.setattr(
        identify_module,
        "_details_need_update",
        lambda _audio_path, _details: True,
    )
    monkeypatch.setattr(
        identify_module,
        "_write_tags",
        lambda _audio_path, _details: captured.setdefault("updated", True),
    )
    monkeypatch.setattr(
        identify_module,
        "record_library_tool_run",
        lambda *args, **kwargs: captured.setdefault("run", kwargs),
    )

    lines, exit_code = identify_module.identify_tracks_by_audio(
        tmp_path,
        dry_run=True,
        db_path=tmp_path / "library_index.db",
        selected_paths=[audio_path],
    )

    assert exit_code == 0
    assert any("would update" in line for line in lines)
    review = captured["run"]["result"]["review"]
    assert review["low_confidence_count"] == 0
    assert review["no_match_count"] == 0


def test_details_need_update_ignores_missing_optional_track_number(
    tmp_path, monkeypatch
) -> None:
    identify_module = _load_identify_module()
    audio_path = tmp_path / "10cc" / "The Original Soundtrack" / "track.mp3"
    audio_path.parent.mkdir(parents=True, exist_ok=True)
    audio_path.write_bytes(b"audio")

    monkeypatch.setattr(
        identify_module,
        "load_embedded_audio_metadata",
        lambda _audio_path: {
            "title": "I'm Not in Love",
            "artist": "10cc",
            "album": "The Original Soundtrack",
            "albumartist": "10cc",
            "track_number": "2/10",
            "musicbrainz_track_id": "track-1",
            "musicbrainz_album_id": "album-1",
            "musicbrainz_artist_id": "artist-1",
            "musicbrainz_albumartist_id": "artist-1",
        },
    )
    monkeypatch.setattr(
        identify_module,
        "load_song_metadata_xml",
        lambda _xml_path: {
            "title": "I'm Not in Love",
            "performingartist": "10cc",
            "albumtitle": "The Original Soundtrack",
            "albumartist": "10cc",
            "tracknumber": "",
            "musicbrainztrackid": "track-1",
            "musicbrainzalbumid": "album-1",
            "musicbrainzartistid": "artist-1",
            "musicbrainzalbumartistid": "artist-1",
        },
    )

    assert not identify_module._details_need_update(
        audio_path,
        {
            "title": "I'm Not in Love",
            "artist": "10cc",
            "album": "The Original Soundtrack",
            "albumartist": "10cc",
            "track_number": "",
            "recording_mbid": "track-1",
            "release_mbid": "album-1",
            "artist_mbid": "artist-1",
            "albumartist_mbid": "artist-1",
        },
    )
