window.PlaylistSyncLiveRunViewer = (() => {
  function classify(line) {
    const lower = String(line || "").toLowerCase();
    if (lower.startsWith("summary")) return "log-line-summary";
    if (lower.startsWith("warn") || lower.includes("warn:")) return "log-line-warning";
    if (lower.includes("fixed:") || lower.startsWith("  fixed:")) return "log-line-fixed";
    if (lower.includes("created:") || lower.startsWith("  created:")) return "log-line-created";
    if (lower.includes("deleted:") || lower.startsWith("  deleted:")) return "log-line-deleted";
    if (lower.startsWith("  skip") || lower.includes("already correct")) return "log-line-skip";
    if (lower.includes("error") || lower.startsWith("error")) return "log-line-error";
    if (lower.includes("[dry-run]")) return "log-line-dry";
    return "";
  }

  function isProgressLine(text) {
    return typeof text === "string" && text.startsWith("PROGRESS:");
  }

  function trimLog(logEl, maxLines) {
    while (logEl.childElementCount > maxLines + 1) {
      logEl.removeChild(logEl.firstElementChild);
    }
  }

  function appendLine(logEl, shell, text, maxLines = 120) {
    const lineClass = classify(text);
    const atBottom = shell.scrollHeight - shell.scrollTop - shell.clientHeight < 50;
    const lastLine = logEl.lastElementChild;
    if (isProgressLine(text) && lastLine?.classList.contains("tool-log-progress")) {
      lastLine.textContent = text;
      if (atBottom) shell.scrollTop = shell.scrollHeight;
      return;
    }

    const lineEl = document.createElement("div");
    lineEl.classList.add("tool-log-line");
    if (isProgressLine(text)) {
      lineEl.classList.add("tool-log-progress");
    }
    if (lineClass) {
      lineEl.classList.add(lineClass);
    }
    lineEl.textContent = text;
    logEl.appendChild(lineEl);
    trimLog(logEl, maxLines);
    if (atBottom) shell.scrollTop = shell.scrollHeight;
  }

  function setStatusChip(element, status, baseClass = "tool-status") {
    if (!element) return;
    element.textContent = status;
    element.className = `${baseClass} ${status}`.trim();
  }

  function renderSummary(gridEl, boxEl, line) {
    if (!gridEl || !boxEl) return;
    const pairs = String(line || "")
      .replace(/^summary\s*/i, "")
      .trim()
      .split(/\s+/)
      .map((token) => token.split("="))
      .filter((parts) => parts.length === 2 && parts[0] && parts[1]);

    gridEl.innerHTML = "";
    pairs.forEach(([key, value]) => {
      const item = document.createElement("div");
      item.className = "tool-summary-item";

      const keyEl = document.createElement("span");
      keyEl.className = "tool-summary-key";
      keyEl.textContent = key.replaceAll("_", " ");

      const valueEl = document.createElement("span");
      valueEl.className = "tool-summary-value";
      valueEl.textContent = value;

      item.appendChild(keyEl);
      item.appendChild(valueEl);
      gridEl.appendChild(item);
    });

    boxEl.hidden = gridEl.childElementCount === 0;
  }

  function resetLog(logEl, emptyText = "Waiting for tool output.") {
    if (!logEl) return;
    logEl.replaceChildren();
    logEl.textContent = emptyText;
  }

    // copyAllLog removed; log is now always selectable for manual copy.
    return {
      appendLine,
      classify,
      renderSummary,
      resetLog,
      setStatusChip,
    };
})();
