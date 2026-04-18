from __future__ import annotations

import sys
from typing import TextIO


def emit_console_line(line: str, *, stream: TextIO | None = None) -> None:
    target = stream or sys.stdout
    text = f"{line}\n"
    try:
        target.write(text)
        target.flush()
        return
    except UnicodeEncodeError:
        pass

    encoding = getattr(target, "encoding", None) or "utf-8"
    buffer = getattr(target, "buffer", None)
    encoded = text.encode(encoding, errors="backslashreplace")
    if buffer is not None:
        buffer.write(encoded)
        buffer.flush()
        return

    target.write(encoded.decode(encoding, errors="ignore"))
    target.flush()
