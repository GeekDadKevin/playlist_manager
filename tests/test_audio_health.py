from __future__ import annotations

import importlib.util
import sqlite3
import sys

from app.services.audio_health import check_audio_file, iter_audio_files


def _load_check_audio_module():
    repo_root = __import__("pathlib").Path(__file__).resolve().parents[1]
    module_path = repo_root / "scripts" / "check_audio_health.py"
    spec = importlib.util.spec_from_file_location(
        "check_audio_health_test_module", module_path
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_iter_audio_files_filters_and_sorts_supported_extensions(tmp_path) -> None:
    (tmp_path / "b-track.mp3").write_bytes(b"b")
    (tmp_path / "a-track.flac").write_bytes(b"a")
    (tmp_path / "notes.txt").write_text("ignore", encoding="utf-8")

    files = iter_audio_files(tmp_path)

    assert [path.name for path in files] == ["a-track.flac", "b-track.mp3"]


def test_check_audio_file_marks_zero_byte_files_as_errors(tmp_path) -> None:
    audio_path = tmp_path / "broken.flac"
    audio_path.write_bytes(b"")

    result = check_audio_file(audio_path)

    assert result.status == "error"
    assert result.message == "Zero-byte file."


def test_check_audio_file_warns_when_ffmpeg_passes_but_mutagen_fails(
    tmp_path, monkeypatch
) -> None:
    audio_path = tmp_path / "suspicious.mp3"
    audio_path.write_bytes(b"not-really-an-mp3")

    monkeypatch.setattr(
        "app.services.audio_health._run_mutagen_parse_check",
        lambda path: "bad frame header",
    )
    monkeypatch.setattr(
        "app.services.audio_health._run_ffmpeg_decode_check",
        lambda path, ffmpeg_path: "",
    )

    result = check_audio_file(audio_path, ffmpeg_path="ffmpeg")

    assert result.status == "warning"
    assert "FFmpeg decode passed" in result.message


def test_check_audio_file_uses_ffmpeg_failure_as_corruption_signal(
    tmp_path, monkeypatch
) -> None:
    audio_path = tmp_path / "corrupt.m4a"
    audio_path.write_bytes(b"not-really-an-m4a")

    monkeypatch.setattr(
        "app.services.audio_health._run_mutagen_parse_check", lambda path: ""
    )
    monkeypatch.setattr(
        "app.services.audio_health._run_ffmpeg_decode_check",
        lambda path, ffmpeg_path: "Invalid data found when processing input",
    )

    result = check_audio_file(audio_path, ffmpeg_path="ffmpeg")

    assert result.status == "error"
    assert result.message == "Invalid data found when processing input"


def test_check_library_falls_back_when_catalog_refresh_is_locked(
    tmp_path, monkeypatch
) -> None:
    check_module = _load_check_audio_module()
    audio_path = tmp_path / "clean.flac"
    audio_path.write_bytes(b"audio")
    db_path = tmp_path / "library_index.db"

    monkeypatch.setattr(check_module, "find_ffmpeg_executable", lambda: None)
    monkeypatch.setattr(
        check_module,
        "refresh_library_index",
        lambda db_path_value, root: (_ for _ in ()).throw(
            sqlite3.OperationalError("database is locked")
        ),
    )
    monkeypatch.setattr(
        check_module,
        "check_audio_file",
        lambda path, ffmpeg_path=None: __import__(
            "app.services.audio_health", fromlist=["AudioCheckResult"]
        ).AudioCheckResult(path=path, status="ok", message=""),
    )

    lines, exit_code = check_module.check_library(
        tmp_path, full_scan=True, db_path=db_path
    )

    assert exit_code == 0
    assert any(
        "falling back to direct filesystem scan" in line.lower() for line in lines
    )
    assert any("CHECK: 1/1" in line for line in lines)
    assert any("OK: clean.flac" in line for line in lines)


def test_check_library_emits_refresh_discovery_progress(tmp_path, monkeypatch) -> None:
    check_module = _load_check_audio_module()
    first_audio = tmp_path / "alpha.flac"
    second_audio = tmp_path / "beta.mp3"
    first_audio.write_bytes(b"audio-a")
    second_audio.write_bytes(b"audio-b")
    db_path = tmp_path / "library_index.db"

    monkeypatch.setattr(check_module, "find_ffmpeg_executable", lambda: None)
    monkeypatch.setattr(
        check_module,
        "check_audio_file",
        lambda path, ffmpeg_path=None: __import__(
            "app.services.audio_health", fromlist=["AudioCheckResult"]
        ).AudioCheckResult(path=path, status="ok", message=""),
    )

    lines, exit_code = check_module.check_library(
        tmp_path, full_scan=True, db_path=db_path
    )

    assert exit_code == 0
    assert any("PROGRESS: discovered 1 audio file(s) so far" in line for line in lines)
    assert any("PROGRESS: refresh list build complete" in line for line in lines)
    assert any("CHECK: 1/2" in line for line in lines)
    assert any("CHECK: 2/2" in line for line in lines)


def test_check_library_refresh_skips_xml_sidecars(tmp_path, monkeypatch) -> None:
    check_module = _load_check_audio_module()
    audio_path = tmp_path / "alpha.flac"
    audio_path.write_bytes(b"audio-a")
    db_path = tmp_path / "library_index.db"
    captured: dict[str, object] = {}

    monkeypatch.setattr(check_module, "find_ffmpeg_executable", lambda: None)

    def fake_refresh_library_index(db_path_value, root, **kwargs):
        captured.update(kwargs)
        return {
            "scanned": 1,
            "changed": 1,
            "unchanged": 0,
            "xml_scanned": 0,
            "xml_changed": 0,
            "embedded_changed": 1,
            "partial": 0,
            "limit": 0,
            "xml_scan_skipped": 1,
        }

    monkeypatch.setattr(
        check_module, "refresh_library_index", fake_refresh_library_index
    )
    monkeypatch.setattr(
        check_module, "count_indexed_audio_files", lambda db_path_value, root: 1
    )
    monkeypatch.setattr(
        check_module,
        "list_audio_health_candidates",
        lambda *args, **kwargs: [audio_path],
    )
    monkeypatch.setattr(
        check_module,
        "check_audio_file",
        lambda path, ffmpeg_path=None: __import__(
            "app.services.audio_health", fromlist=["AudioCheckResult"]
        ).AudioCheckResult(path=path, status="ok", message=""),
    )

    lines, exit_code = check_module.check_library(
        tmp_path, full_scan=True, db_path=db_path
    )

    assert exit_code == 0
    assert captured["scan_xml_sidecars"] is False
    assert any("CHECK: 1/1" in line for line in lines)


def test_check_library_refresh_respects_limit(tmp_path, monkeypatch) -> None:
    check_module = _load_check_audio_module()
    audio_path = tmp_path / "alpha.flac"
    audio_path.write_bytes(b"audio-a")
    db_path = tmp_path / "library_index.db"
    captured: dict[str, object] = {}

    monkeypatch.setattr(check_module, "find_ffmpeg_executable", lambda: None)

    def fake_refresh_library_index(db_path_value, root, **kwargs):
        captured.update(kwargs)
        return {
            "scanned": 1,
            "changed": 1,
            "unchanged": 0,
            "xml_scanned": 0,
            "xml_changed": 0,
            "embedded_changed": 1,
            "partial": 1,
            "limit": 5,
            "xml_scan_skipped": 1,
        }

    monkeypatch.setattr(
        check_module, "refresh_library_index", fake_refresh_library_index
    )
    monkeypatch.setattr(
        check_module, "count_indexed_audio_files", lambda db_path_value, root: 1
    )
    monkeypatch.setattr(
        check_module,
        "list_audio_health_candidates",
        lambda *args, **kwargs: [audio_path],
    )
    monkeypatch.setattr(
        check_module,
        "check_audio_file",
        lambda path, ffmpeg_path=None: __import__(
            "app.services.audio_health", fromlist=["AudioCheckResult"]
        ).AudioCheckResult(path=path, status="ok", message=""),
    )

    lines, exit_code = check_module.check_library(
        tmp_path,
        limit=5,
        full_scan=True,
        db_path=db_path,
    )

    assert exit_code == 0
    assert captured["limit"] == 5
    assert any("CHECK: 1/1" in line for line in lines)
