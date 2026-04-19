from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

from app.services.library_index import refresh_library_index


def _load_structure_module():
    repo_root = Path(__file__).resolve().parents[1]
    module_path = repo_root / "scripts" / "identify_tracks_from_layout.py"
    spec = importlib.util.spec_from_file_location(
        "identify_tracks_from_layout_test_module",
        module_path,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_identify_tracks_from_layout_uses_catalog_candidates(
    tmp_path, monkeypatch
) -> None:
    layout_module = _load_structure_module()
    audio_path = (
        tmp_path
        / "Various Artists"
        / "Warp Sampler"
        / "Autechre - Warp Sampler - 02 - Cichli.flac"
    )
    audio_path.parent.mkdir(parents=True, exist_ok=True)
    audio_path.write_bytes(b"fake-audio")

    captured: dict[str, object] = {}

    monkeypatch.setattr(
        layout_module,
        "write_musicbrainz_tags",
        lambda path, details: captured.setdefault("written", []).append(
            (path, details)
        ),
    )

    db_path = tmp_path / "library_index.db"
    refresh_library_index(db_path, tmp_path)

    lines, exit_code = layout_module.identify_tracks_from_layout(
        tmp_path,
        dry_run=False,
        db_path=db_path,
    )

    assert exit_code == 0
    assert any("selected 1 candidate file(s)" in line for line in lines)
    assert any("TAGGED:" in line for line in lines)
    written = captured["written"]
    assert len(written) == 1
    assert written[0][0] == audio_path
    assert written[0][1]["artist"] == "Autechre"
    assert written[0][1]["albumartist"] == "Various Artists"
    assert written[0][1]["track_number"] == "2"


def test_identify_tracks_from_layout_refresh_respects_limit(
    tmp_path, monkeypatch
) -> None:
    layout_module = _load_structure_module()
    audio_path = (
        tmp_path / "Orbital" / "In Sides" / "Orbital - In Sides - 01 - The Moebius.flac"
    )
    audio_path.parent.mkdir(parents=True, exist_ok=True)
    audio_path.write_bytes(b"fake-audio")
    captured_limits: list[int | None] = []
    captured_scan_xml_sidecars: list[bool | None] = []

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

    monkeypatch.setattr(
        layout_module, "refresh_library_index", fake_refresh_library_index
    )
    monkeypatch.setattr(
        layout_module,
        "list_structure_tag_candidates",
        lambda *args, **kwargs: [audio_path],
    )
    monkeypatch.setattr(
        layout_module, "write_musicbrainz_tags", lambda *args, **kwargs: None
    )

    lines, exit_code = layout_module.identify_tracks_from_layout(
        tmp_path,
        dry_run=False,
        limit=5,
        db_path=tmp_path / "library_index.db",
    )

    assert exit_code == 0
    assert captured_limits == [5]
    assert captured_scan_xml_sidecars == [False]
    assert any("selected 1 candidate file(s)" in line for line in lines)
