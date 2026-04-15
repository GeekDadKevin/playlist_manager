from __future__ import annotations

from app.services.audio_health import check_audio_file, iter_audio_files


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


def test_check_audio_file_warns_when_ffmpeg_passes_but_mutagen_fails(tmp_path, monkeypatch) -> None:
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


def test_check_audio_file_uses_ffmpeg_failure_as_corruption_signal(tmp_path, monkeypatch) -> None:
    audio_path = tmp_path / "corrupt.m4a"
    audio_path.write_bytes(b"not-really-an-m4a")

    monkeypatch.setattr("app.services.audio_health._run_mutagen_parse_check", lambda path: "")
    monkeypatch.setattr(
        "app.services.audio_health._run_ffmpeg_decode_check",
        lambda path, ffmpeg_path: "Invalid data found when processing input",
    )

    result = check_audio_file(audio_path, ffmpeg_path="ffmpeg")

    assert result.status == "error"
    assert result.message == "Invalid data found when processing input"
