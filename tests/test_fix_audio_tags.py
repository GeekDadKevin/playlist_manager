from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

from app.services.library_index import refresh_library_index


def _load_fix_tags_module():
    repo_root = Path(__file__).resolve().parents[1]
    module_path = repo_root / "scripts" / "fix_audio_tags.py"
    spec = importlib.util.spec_from_file_location(
        "fix_audio_tags_test_module", module_path
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_fix_tags_uses_catalog_candidates(tmp_path, monkeypatch) -> None:
    fix_module = _load_fix_tags_module()

    audio_path = (
        tmp_path / "Orbital" / "In Sides" / "Orbital - In Sides - 01 - The Moebius.flac"
    )
    audio_path.parent.mkdir(parents=True, exist_ok=True)
    audio_path.write_bytes(b"fake-audio")

    class FakeMutagenFile(dict):
        def __init__(self, filename, easy=True):
            self.filename = filename
            self.saved = False
            self.update(
                {
                    "artist": ["Wrong Artist"],
                    "albumartist": ["Wrong Album Artist"],
                }
            )

        def get(self, key, default=None):
            return dict.get(self, key, default)

        def save(self):
            self.saved = True

    monkeypatch.setattr(fix_module, "_HAS_MUTAGEN", True)
    monkeypatch.setattr(fix_module, "MutagenFile", FakeMutagenFile)
    monkeypatch.setattr(
        fix_module,
        "_run_musicbrainz_enrichment",
        lambda *args, **kwargs: (
            [
                "SUMMARY  scanned=1  updated=1  unresolved=0  failed=0  "
                "dry_run=False  full_scan=False"
            ],
            0,
        ),
    )

    db_path = tmp_path / "library_index.db"
    refresh_library_index(db_path, tmp_path)
    lines = fix_module.fix_tags(tmp_path, dry_run=False, db_path=db_path)

    assert any("selected 1 candidate file(s)" in line for line in lines)
    assert any("FIXED:" in line for line in lines)
    assert any("musicbrainz_updated=1" in line for line in lines)


def test_fix_tags_refresh_respects_limit(tmp_path, monkeypatch) -> None:
    fix_module = _load_fix_tags_module()
    audio_path = (
        tmp_path / "Orbital" / "In Sides" / "Orbital - In Sides - 01 - The Moebius.flac"
    )
    audio_path.parent.mkdir(parents=True, exist_ok=True)
    audio_path.write_bytes(b"fake-audio")
    captured_limits: list[int | None] = []
    captured_scan_xml_sidecars: list[bool | None] = []

    class FakeMutagenFile(dict):
        def __init__(self, filename, easy=True):
            self.filename = filename
            self.saved = False
            self.update(
                {"artist": ["Wrong Artist"], "albumartist": ["Wrong Album Artist"]}
            )

        def get(self, key, default=None):
            return dict.get(self, key, default)

        def save(self):
            self.saved = True

    def fake_refresh_library_index(db_path_value, root, **kwargs):
        captured_limits.append(kwargs.get("limit"))
        captured_scan_xml_sidecars.append(kwargs.get("scan_xml_sidecars"))
        return {
            "scanned": 1,
            "changed": 1,
            "unchanged": 0,
            "xml_scanned": 0,
            "xml_changed": 0,
            "embedded_changed": 1,
            "partial": int(kwargs.get("limit") is not None),
            "limit": kwargs.get("limit") or 0,
            "xml_scan_skipped": int(kwargs.get("scan_xml_sidecars") is False),
        }

    monkeypatch.setattr(fix_module, "_HAS_MUTAGEN", True)
    monkeypatch.setattr(fix_module, "MutagenFile", FakeMutagenFile)
    monkeypatch.setattr(fix_module, "refresh_library_index", fake_refresh_library_index)
    monkeypatch.setattr(
        fix_module, "list_tag_fix_candidates", lambda *args, **kwargs: [audio_path]
    )
    monkeypatch.setattr(
        fix_module,
        "_run_musicbrainz_enrichment",
        lambda *args, **kwargs: (
            [
                "SUMMARY  scanned=0  updated=0  unresolved=0  failed=0  "
                "dry_run=False  full_scan=False"
            ],
            0,
        ),
    )

    lines = fix_module.fix_tags(
        tmp_path,
        dry_run=False,
        limit=5,
        db_path=tmp_path / "library_index.db",
    )

    assert captured_limits == [5, 5]
    assert captured_scan_xml_sidecars == [False, False]
    assert any("selected 1 candidate file(s)" in line for line in lines)


def test_fix_tags_forwards_remaining_limit_to_musicbrainz_pass(
    tmp_path, monkeypatch
) -> None:
    fix_module = _load_fix_tags_module()
    audio_path = (
        tmp_path / "Orbital" / "In Sides" / "Orbital - In Sides - 01 - The Moebius.flac"
    )
    audio_path.parent.mkdir(parents=True, exist_ok=True)
    audio_path.write_bytes(b"fake-audio")
    captured_limits: list[int | None] = []
    captured_scan_xml_sidecars: list[bool | None] = []

    class FakeMutagenFile(dict):
        def __init__(self, filename, easy=True):
            self.filename = filename
            self.saved = False
            self.update(
                {"artist": ["Wrong Artist"], "albumartist": ["Wrong Album Artist"]}
            )

        def get(self, key, default=None):
            return dict.get(self, key, default)

        def save(self):
            self.saved = True

    monkeypatch.setattr(fix_module, "_HAS_MUTAGEN", True)
    monkeypatch.setattr(fix_module, "MutagenFile", FakeMutagenFile)
    monkeypatch.setattr(
        fix_module,
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
            "partial": 1,
            "limit": kwargs.get("limit") or 0,
            "xml_scan_skipped": int(kwargs.get("scan_xml_sidecars") is False),
        },
    )
    monkeypatch.setattr(
        fix_module, "list_tag_fix_candidates", lambda *args, **kwargs: [audio_path]
    )

    def fake_enrichment(*args, **kwargs):
        captured_limits.append(kwargs.get("limit"))
        return (
            [
                "SUMMARY  scanned=1  updated=0  unresolved=0  failed=0  "
                "dry_run=False  full_scan=False"
            ],
            0,
        )

    monkeypatch.setattr(fix_module, "_run_musicbrainz_enrichment", fake_enrichment)

    fix_module.fix_tags(
        tmp_path,
        dry_run=False,
        limit=2,
        db_path=tmp_path / "library_index.db",
    )

    assert captured_scan_xml_sidecars == [False, False]
    assert captured_limits == [1]
