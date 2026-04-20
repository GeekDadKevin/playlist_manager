"""Library maintenance tools for the web UI.

Streams subprocess output line-by-line so the browser can consume it via
Server-Sent Events.  Each tool is an ordinary CLI script in ``scripts/``; we
run it with the same interpreter that is running Flask so all installed
packages are available without any extra PATH gymnastics.
"""

from __future__ import annotations

import os
import subprocess
import sys
import threading
from collections.abc import Generator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

_SCRIPTS_DIR = Path(__file__).resolve().parent.parent.parent / "scripts"
_STATE_LINE_LIMIT = 120

# One lock per tool name so concurrent identical runs are rejected cleanly.
_LOCKS: dict[str, threading.Lock] = {}
_STATE_LOCK = threading.Lock()
_RUN_STATES: dict[str, dict[str, Any]] = {}


def _lock_for(tool: str) -> threading.Lock:
    if tool not in _LOCKS:
        _LOCKS[tool] = threading.Lock()
    return _LOCKS[tool]


TOOLS: dict[str, dict] = {
    "create-db": {
        "script": "create_library_db.py",
        "label": "Create Database",
        "description": "Step 1. Creates the library index database if it does not exist.",
    },
    "refresh-catalog": {
        "script": "refresh_library_catalog.py",
        "label": "Refresh Library Catalog",
        "description": (
            "Step 2. Rebuilds the library index database from the current music root so "
            "the later maintenance tools and browser reports start from fresh catalog state."
        ),
    },
    # The rest of the tools are unordered
    "check-audio": {
        "script": "check_audio_health.py",
        "label": "Check Audio Integrity",
        "description": (
            "Scans every audio file in the music root and flags likely corruption. "
            "Uses ffmpeg decode validation when available, with a mutagen parser "
            "fallback when ffmpeg is not installed."
        ),
    },
    "fix-tags": {
        "script": "fix_audio_tags.py",
        "label": "Fix Audio Tags",
        "description": (
            "Uses the catalog to fix folder-based artist and albumartist mismatches, "
            "then enriches missing track numbers and MusicBrainz IDs from ListenBrainz "
            "and MusicBrainz in the same run."
        ),
    },
    "identify-audio": {
        "script": "identify_tracks_by_audio.py",
        "label": "Identify Tracks By Audio",
        "description": (
            "Uses fpcalc plus AcoustID fingerprint lookup to identify badly tagged "
            "audio, then writes back MusicBrainz-based title, artist, album, and track "
            "metadata."
        ),
    },
    "sync-xml": {
        "script": "rebuild_song_xml.py",
        "label": "Sync XML Sidecars",
        "description": (
            "Keeps XML sidecars aligned with embedded tags by deleting orphaned XML "
            "files, creating missing sidecars, and patching existing sidecars with recovered "
            "artist, album artist, MusicBrainz, and Deezer fields."
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


def get_tool_status_snapshot(*, line_limit: int = 200) -> dict[str, Any]:
    with _STATE_LOCK:
        tools = [
            _trimmed_state(state, line_limit=line_limit)
            for state in _RUN_STATES.values()
        ]

    running = [state for state in tools if state["status"] in {"running", "stopping"}]
    running.sort(key=lambda state: state.get("started_at", ""), reverse=True)
    recent = sorted(
        tools,
        key=lambda state: state.get("completed_at") or state.get("started_at") or "",
        reverse=True,
    )
    primary = running[0] if running else (recent[0] if recent else None)
    return {
        "active": bool(running),
        "primary": primary,
        "tools": tools,
    }


def get_tool_status(tool: str, *, line_limit: int = 200) -> dict[str, Any] | None:
    with _STATE_LOCK:
        state = _RUN_STATES.get(tool)
        if state is None:
            return None
        return _trimmed_state(state, line_limit=line_limit)


def stop_tool(tool: str) -> dict[str, Any] | None:
    with _STATE_LOCK:
        state = _RUN_STATES.get(tool)
        if state is None:
            return None
        proc = state.get("process")
        if proc is None or proc.poll() is not None:
            return _trimmed_state(state, line_limit=_STATE_LINE_LIMIT)

        state["stop_requested"] = True
        state["status"] = "stopping"

    _terminate_process(proc)
    return get_tool_status(tool, line_limit=_STATE_LINE_LIMIT)


def start_process_job(
    tool: str,
    *,
    label: str,
    description: str,
    cmd: list[str],
    dry_run: bool = False,
    full_scan: bool = False,
    limit: int | None = None,
    metadata: dict[str, Any] | None = None,
    env_overrides: dict[str, str] | None = None,
) -> dict[str, Any] | None:
    lock = _lock_for(tool)
    if not lock.acquire(blocking=False):
        return None

    state = {
        "tool": tool,
        "label": label,
        "description": description,
        "status": "running",
        "started_at": _utc_now(),
        "completed_at": "",
        "exit_code": None,
        "dry_run": dry_run,
        "full_scan": full_scan,
        "limit": limit,
        "summary_line": "",
        "dropped_line_count": 0,
        "stop_requested": False,
        "lines": [],
        "process": None,
    }
    if metadata:
        state.update(metadata)
    _set_tool_state(tool, state)

    thread = threading.Thread(
        target=_run_process_job,
        args=(tool, cmd, lock, env_overrides or {}),
        name=f"tool-job-{tool}",
        daemon=True,
    )
    thread.start()
    return get_tool_status(tool, line_limit=_STATE_LINE_LIMIT)


def stream_tool(
    tool: str,
    root: Path,
    *,
    dry_run: bool = False,
    limit: int | None = None,
    full_scan: bool = False,
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
    if full_scan:
        cmd.append("--full-scan")
    if limit is not None:
        cmd.extend(["--limit", str(limit)])

    _set_tool_state(
        tool,
        {
            "tool": tool,
            "label": TOOLS[tool]["label"],
            "description": TOOLS[tool]["description"],
            "status": "running",
            "started_at": _utc_now(),
            "completed_at": "",
            "exit_code": None,
            "dry_run": dry_run,
            "full_scan": full_scan,
            "limit": limit,
            "summary_line": "",
            "dropped_line_count": 0,
            "stop_requested": False,
            "lines": [],
            "process": None,
        },
    )

    try:
        env = os.environ.copy()
        env.setdefault("PYTHONIOENCODING", "utf-8")
        env.setdefault("PYTHONUTF8", "1")
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
            env=env,
        )
        _set_tool_process(tool, proc)
        assert proc.stdout is not None
        for raw_line in proc.stdout:
            line = raw_line.rstrip("\n")
            _append_tool_line(tool, line)
            yield line
        proc.wait()
        _complete_tool_state(tool, proc.returncode)
        yield f"__EXIT__{proc.returncode}"
    except Exception as exc:
        line = f"ERROR launching tool: {exc}"
        _append_tool_line(tool, line)
        _complete_tool_state(tool, 1)
        yield line
        yield "__EXIT__1"
    finally:
        lock.release()


def _set_tool_state(tool: str, state: dict[str, Any]) -> None:
    with _STATE_LOCK:
        _RUN_STATES[tool] = state


def _set_tool_process(tool: str, proc: subprocess.Popen[str]) -> None:
    with _STATE_LOCK:
        state = _RUN_STATES.get(tool)
        if state is None:
            return
        state["process"] = proc


def _run_process_job(
    tool: str,
    cmd: list[str],
    lock: threading.Lock,
    env_overrides: dict[str, str],
) -> None:
    try:
        env = os.environ.copy()
        env.setdefault("PYTHONIOENCODING", "utf-8")
        env.setdefault("PYTHONUTF8", "1")
        env.update(env_overrides)
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
            env=env,
        )
        _set_tool_process(tool, proc)
        assert proc.stdout is not None
        for raw_line in proc.stdout:
            _append_tool_line(tool, raw_line.rstrip("\n"))
        proc.wait()
        _complete_tool_state(tool, proc.returncode)
    except Exception as exc:
        _append_tool_line(tool, f"ERROR launching tool: {exc}")
        _complete_tool_state(tool, 1)
    finally:
        lock.release()


def _append_tool_line(tool: str, line: str) -> None:
    with _STATE_LOCK:
        state = _RUN_STATES.get(tool)
        if state is None:
            return
        lines = list(state.get("lines", []))
        if line.lower().startswith("summary"):
            state["summary_line"] = line
        if _is_progress_line(line) and lines and _is_progress_line(lines[-1]):
            lines[-1] = line
        else:
            lines.append(line)
        if len(lines) > _STATE_LINE_LIMIT:
            state["dropped_line_count"] = (
                int(state.get("dropped_line_count", 0)) + len(lines) - _STATE_LINE_LIMIT
            )
            lines = lines[-_STATE_LINE_LIMIT:]
        state["lines"] = lines


def _complete_tool_state(tool: str, exit_code: int) -> None:
    with _STATE_LOCK:
        state = _RUN_STATES.get(tool)
        if state is None:
            return
        stop_requested = bool(state.get("stop_requested"))
        state["status"] = (
            "stopped" if stop_requested else ("done" if exit_code == 0 else "error")
        )
        state["exit_code"] = exit_code
        state["completed_at"] = _utc_now()
        state["process"] = None


def _trimmed_state(state: dict[str, Any], *, line_limit: int) -> dict[str, Any]:
    trimmed = dict(state)
    trimmed.pop("process", None)
    lines = list(trimmed.get("lines", []))
    trimmed["line_count"] = len(lines)
    trimmed["lines"] = lines[-line_limit:]
    return trimmed


def _terminate_process(proc: subprocess.Popen[str]) -> None:
    if proc.poll() is not None:
        return
    if os.name == "nt":
        subprocess.run(
            ["taskkill", "/PID", str(proc.pid), "/T", "/F"],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return
    proc.terminate()


def _is_progress_line(line: str) -> bool:
    return str(line or "").startswith("PROGRESS:")


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()
