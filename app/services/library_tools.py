"""Library maintenance tools for the web UI.

Streams subprocess output line-by-line so the browser can consume it via
Server-Sent Events.  Each tool is an ordinary CLI script in ``scripts/``; we
run it with the same interpreter that is running Flask so all installed
packages are available without any extra PATH gymnastics.
"""
from __future__ import annotations

import subprocess
import sys
import threading
from collections.abc import Generator
from pathlib import Path

_SCRIPTS_DIR = Path(__file__).resolve().parent.parent.parent / "scripts"

# One lock per tool name so concurrent identical runs are rejected cleanly.
_LOCKS: dict[str, threading.Lock] = {}


def _lock_for(tool: str) -> threading.Lock:
    if tool not in _LOCKS:
        _LOCKS[tool] = threading.Lock()
    return _LOCKS[tool]


TOOLS: dict[str, dict] = {
    "check-audio": {
        "script": "check_audio_health.py",
        "label": "Check Audio Integrity",
        "description": (
            "Scans every audio file in the music root and flags likely corruption. "
            "Uses ffmpeg decode validation when available, with a mutagen parser "
            "fallback when ffmpeg is not installed."
        ),
    },
    "rebuild-xml": {
        "script": "rebuild_song_xml.py",
        "label": "Rebuild XML Sidecars",
        "description": (
            "Deletes orphaned XML sidecars whose audio files no longer exist, "
            "creates new XML sidecars for audio files that are missing one, "
            "and patches every existing sidecar so that "
            "<performingartist> and <albumartist> reflect the embedded tags "
            "(falling back to the artist directory name)."
        ),
    },
    "fix-tags": {
        "script": "fix_audio_tags.py",
        "label": "Fix Audio Tags",
        "description": (
            "Walks every audio file in the music root and ensures the embedded "
            "artist and albumartist tags match the artist directory name "
            "(the grandparent folder in the {artist}/{album}/{file} layout). "
            "Fixes mismatches and missing values in-place."
        ),
    },
}


def is_running(tool: str) -> bool:
    """Return True if a tool job is currently active."""
    lock = _lock_for(tool)
    if lock.acquire(blocking=False):
        lock.release()
        return False
    return True


def stream_tool(
    tool: str,
    root: Path,
    *,
    dry_run: bool = False,
    limit: int | None = None,
) -> Generator[str, None, None]:
    """Yield log lines from running *tool* against *root*.

    Yields ``__BUSY__`` immediately if the tool is already running.
    Yields ``__EXIT__<code>`` as the final line with the process exit code.
    """
    lock = _lock_for(tool)
    if not lock.acquire(blocking=False):
        yield "__BUSY__"
        return

    script_name = TOOLS[tool]["script"]
    script_path = _SCRIPTS_DIR / script_name
    if not script_path.exists():
        lock.release()
        yield f"ERROR: script not found: {script_path}"
        yield "__EXIT__1"
        return

    cmd = [sys.executable, str(script_path), str(root)]
    if dry_run:
        cmd.append("--dry-run")
    if limit is not None:
        cmd.extend(["--limit", str(limit)])

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )
        assert proc.stdout is not None
        for raw_line in proc.stdout:
            yield raw_line.rstrip("\n")
        proc.wait()
        yield f"__EXIT__{proc.returncode}"
    except Exception as exc:
        yield f"ERROR launching tool: {exc}"
        yield "__EXIT__1"
    finally:
        lock.release()
