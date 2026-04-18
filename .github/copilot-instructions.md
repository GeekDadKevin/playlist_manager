# Copilot Instructions for `playlist_manager`

- For repo-specific work in this project, prefer the `Playlist Sync Builder` custom agent.
- For runtime debugging, `/tools` or `/settings` or `/sync` issues, SQLite lock failures, startup problems, or CRUD changes across routes/services/templates, prefer the `Playlist Runtime Triage` custom agent.
- Treat `NAVIDROME_MUSIC_ROOT` as the single shared music/download root; do **not** reintroduce `DEEZER_DOWNLOAD_DIR`.
- In Docker/Compose, the user supplies host paths via `NAVIDROME_MUSIC_ROOT` and `NAVIDROME_PLAYLIST_DIR`, but the app should use the stable in-container paths `/navidrome/root` and `/navidrome/playlist`.
- Live sync is sequential by default and review-first: if any Deezer match is low-confidence, pause Navidrome playlist export until the user resolves it in the web UI.
- The low-confidence review flow should pre-run SoundCloud and YouTube searches for each low-confidence track before showing the final choice list, with a simple loading dialog/status message so the user knows searches are running.
- The low-confidence review screen should offer bulk actions to accept all remaining tracks as missing or try SoundCloud/YouTube across all remaining low-confidence tracks; high-confidence fallback matches should auto-resolve, while the remaining tracks should show inline Deezer plus SoundCloud/YouTube choices for the user to pick from.
- For `identify-audio` fingerprint work, do not treat a high AcoustID score as sufficient on its own; preserve the title/artist/album similarity guardrail before writing tags or XML.
- For `identify-audio` retry or review flows, do not bypass the saved review snapshot with ad hoc DB edits or one-off acceptance logic; use the same guardrail-aware review path the Tools page uses, and keep guardrail failures in manual review.
- If a fingerprint review item was accepted incorrectly, repair the file back to reference/path-derived metadata, clear MusicBrainz IDs that came from the bad match, refresh the library index for those paths, and reseed the review snapshot before continuing with more writes.
- When running larger fingerprint write batches, prefer a dry-run first, then apply only trusted candidates in small explicit selections rather than broad auto-accept passes.
- Deezer remains the primary authenticated provider. SoundCloud/YouTube are manual-review-only options, handled through `yt-dlp` with `SOUNDCLOUD_FALLBACK`, `YOUTUBE_FALLBACK`, and their match thresholds; do not auto-download them during the normal Deezer sync pass or invent provider APIs/endpoints/flags.
- On Windows, prefer `uv run python -m flask` or the repo `start.ps1` script; the app is expected to bind to port `3000`.
- You may start the app or related local services for testing, but **do not leave them running after testing is complete**.
- Before finishing a task, stop any server process you launched, clean up any stale `.app.lock` file if needed, and avoid leaving port `3000` occupied by the repo app.
- Use fresh verification evidence from commands like `uv run python -m pytest`, `uv run python -m ruff check .`, or a live HTTP check before claiming completion.

# Maintenance Tool DB-Only Policy & Risks

- All maintenance tools (catalog, tag, XML, health, repair, etc.) **must** use the library database for all file and sidecar lists. No direct filesystem scans are allowed.
- The user is responsible for running the catalog refresh tool before any maintenance tool; tools operate only on the DB state.
- When adding or updating any maintenance tool, the agent must verify that it uses only the DB for file/sidecar discovery and does not scan the filesystem directly.
- If a new tool is added, or an existing tool is modified, always check for DB-only compliance and update this policy if needed.
