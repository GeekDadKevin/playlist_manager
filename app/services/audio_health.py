from __future__ import annotations

import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from mutagen import File as MutagenFile

from app.services.song_metadata import AUDIO_EXTENSIONS


@dataclass(slots=True)
class AudioCheckResult:
    path: Path
    status: Literal["ok", "warning", "error"]
    message: str = ""


def find_ffmpeg_executable() -> str | None:
    return shutil.which("ffmpeg")


def iter_audio_files(root: str | Path) -> list[Path]:
    root_path = Path(root).expanduser()
    if not root_path.exists() or not root_path.is_dir():
        raise ValueError(f"Music library root does not exist or is not a directory: {root_path}")

    return sorted(
        path
        for path in root_path.rglob("*")
        if path.is_file() and path.suffix.lower() in AUDIO_EXTENSIONS
    )


def check_audio_file(
    audio_path: str | Path,
    *,
    ffmpeg_path: str | None = None,
) -> AudioCheckResult:
    path = Path(audio_path)
    if not path.exists() or not path.is_file():
        return AudioCheckResult(path=path, status="error", message="File does not exist.")

    if path.stat().st_size <= 0:
        return AudioCheckResult(path=path, status="error", message="Zero-byte file.")

    parse_error = _run_mutagen_parse_check(path)
    decode_error = _run_ffmpeg_decode_check(path, ffmpeg_path) if ffmpeg_path else ""

    if decode_error:
        return AudioCheckResult(path=path, status="error", message=decode_error)

    if parse_error and ffmpeg_path:
        return AudioCheckResult(
            path=path,
            status="warning",
            message=f"FFmpeg decode passed, but metadata parsing failed: {parse_error}",
        )

    if parse_error:
        return AudioCheckResult(path=path, status="error", message=parse_error)

    return AudioCheckResult(path=path, status="ok")


def _run_mutagen_parse_check(audio_path: Path) -> str:
    try:
        parsed = MutagenFile(audio_path)
    except Exception as exc:
        return _normalize_error(str(exc) or exc.__class__.__name__)

    if parsed is None:
        return "Mutagen could not identify the audio container or metadata."

    return ""


def _run_ffmpeg_decode_check(audio_path: Path, ffmpeg_path: str) -> str:
    try:
        proc = subprocess.run(
            [
                ffmpeg_path,
                "-v",
                "error",
                "-nostdin",
                "-xerror",
                "-i",
                str(audio_path),
                "-f",
                "null",
                "-",
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
    except OSError as exc:
        return _normalize_error(f"Could not launch ffmpeg: {exc}")

    if proc.returncode == 0:
        return ""

    message = (proc.stderr or proc.stdout or "").strip()
    if not message:
        message = f"ffmpeg exited with code {proc.returncode}."
    return _normalize_error(message)


def _normalize_error(message: str, *, limit: int = 240) -> str:
    compact = " ".join(str(message).split())
    if len(compact) <= limit:
        return compact
    return compact[: limit - 3].rstrip() + "..."
