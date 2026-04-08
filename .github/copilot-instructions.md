# Copilot Instructions for `playlist_manager`

- For repo-specific work in this project, prefer the `Octo Playlist Sync Builder` custom agent.
- You may start the app or related local services for testing, but **do not leave them running after testing is complete**.
- Before finishing a task, stop any server process you launched, clean up any stale `.app.lock` file if needed, and avoid leaving port `3000` occupied by the repo app.
- Use fresh verification evidence from commands like `uv run python -m pytest`, `uv run python -m ruff check .`, or a live HTTP check before claiming completion.
