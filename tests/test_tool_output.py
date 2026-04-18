from __future__ import annotations

import io

from app.services.tool_output import emit_console_line


def test_emit_console_line_handles_cp1252_unencodable_characters() -> None:
    buffer = io.BytesIO()
    stream = io.TextIOWrapper(buffer, encoding="cp1252", errors="strict")

    emit_console_line("INDEX XML: bad-\uf022-name.xml", stream=stream)

    output = buffer.getvalue().decode("cp1252")
    assert "INDEX XML: bad-" in output
    assert "\\uf022" in output
    assert output.endswith("name.xml\n")
