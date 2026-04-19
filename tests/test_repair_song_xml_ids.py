from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

from app.services.library_index import refresh_library_index


def _load_repair_module():
    repo_root = Path(__file__).resolve().parents[1]
    module_path = repo_root / "scripts" / "repair_song_xml_ids.py"
    spec = importlib.util.spec_from_file_location(
        "repair_song_xml_ids_test_module", module_path
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_repair_xml_ids_uses_catalog_candidates(tmp_path, monkeypatch) -> None:
    repair_module = _load_repair_module()

    audio_path = tmp_path / "Massive Attack - Mezzanine - Teardrop.flac"
    audio_path.write_bytes(b"fake-flac")
    xml_path = audio_path.with_suffix(".xml")
    xml_path.write_text(
        """<?xml version=\"1.0\" encoding=\"utf-8\"?>
<song>
  <provider>deezer</provider>
  <downloadedfrom>deezer</downloadedfrom>
  <title>Teardrop</title>
</song>
""",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        repair_module,
        "repair_song_metadata_xml_ids",
        lambda root, dry_run, limit, xml_paths=None, progress_callback=None: {
            "root": str(root),
            "scanned": len(xml_paths or []),
            "updated": 1,
            "unchanged": 0,
            "unresolved": 0,
            "failed": 0,
            "written": [str(xml_path)],
            "unresolved_items": [],
        },
    )
    monkeypatch.setattr(
        repair_module,
        "list_xml_id_repair_candidates",
        lambda db_path, root, force_full=False, limit=None: [(xml_path, audio_path)],
    )

    db_path = tmp_path / "library_index.db"
    refresh_library_index(db_path, tmp_path)
    with_path = repair_module.repair_xml_ids(tmp_path, db_path=db_path)

    assert with_path[1] == 0
    assert any("Found 1 XML sidecar(s) to inspect" in line for line in with_path[0])


def test_repair_xml_ids_refresh_respects_limit(tmp_path, monkeypatch) -> None:
    repair_module = _load_repair_module()
    audio_path = tmp_path / "Massive Attack - Mezzanine - Teardrop.flac"
    audio_path.write_bytes(b"fake-flac")
    xml_path = audio_path.with_suffix(".xml")
    xml_path.write_text("<song><title>Teardrop</title></song>", encoding="utf-8")
    captured_limits: list[int | None] = []

    def fake_refresh_library_index(db_path_value, root, **kwargs):
        captured_limits.append(kwargs.get("limit"))
        return {
            "scanned": 1,
            "changed": 1,
            "unchanged": 0,
            "xml_scanned": 1,
            "xml_changed": 1,
            "embedded_changed": 1,
            "partial": int(kwargs.get("limit") is not None),
            "limit": kwargs.get("limit") or 0,
            "xml_scan_skipped": 0,
        }

    monkeypatch.setattr(
        repair_module, "refresh_library_index", fake_refresh_library_index
    )
    monkeypatch.setattr(
        repair_module,
        "repair_song_metadata_xml_ids",
        lambda root, dry_run, limit, xml_paths=None, progress_callback=None: {
            "root": str(root),
            "scanned": len(xml_paths or []),
            "updated": 1,
            "unchanged": 0,
            "unresolved": 0,
            "failed": 0,
            "written": [str(xml_path)],
            "unresolved_items": [],
        },
    )
    monkeypatch.setattr(
        repair_module,
        "list_xml_id_repair_candidates",
        lambda db_path, root, force_full=False, limit=None: [(xml_path, audio_path)],
    )

    result = repair_module.repair_xml_ids(
        tmp_path, limit=5, db_path=tmp_path / "library_index.db"
    )

    assert result[1] == 0
    assert captured_limits == [5, 5]
