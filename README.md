# JSPF Converter / Octo Playlist Sync

A Docker-friendly Flask service for importing playlists from `m3u`, `jspf`, Navidrome missing-files `csv`, or ListenBrainz playlist sources, reviewing track metadata, and preparing Octo-Fiesta download handoff payloads for Navidrome workflows.

## Stack

- `Flask` for the API and lightweight web UI
- `uv` for environment, dependency, and command management
- `ruff` for linting and formatting
- `pytest` for parser and matching tests
- `docker compose` for local orchestration

## Quick start

### Local development

```powershell
uv sync --dev
uv run ruff format .
uv run ruff check .
uv run pytest
uv run python -m flask --app app run --debug
```

Open <http://127.0.0.1:5000>.

### Start scripts

Use the included one-command launchers:

```powershell
./start.ps1
```

```bash
./start.sh
```

Both scripts run `uv sync --dev`, validate the current `.env`, and then start the single Flask app that serves both the web UI and the API on **`http://127.0.0.1:3000` only**. If a variable is invalid, startup stops immediately and prints the exact variable name(s) that need fixing. Re-running either script reads `.app.lock`, stops the previously started app process, clears any stale listener on port `3000`, and writes the new PID back to that lock file.

### ListenBrainz playlist configuration

To import ListenBrainz playlists directly or auto-pull the latest "Created For You" playlist, set these optional values in `.env`:

```env
LISTENBRAINZ_API_BASE_URL=https://api.listenbrainz.org
LISTENBRAINZ_USERNAME=your_listenbrainz_username
LISTENBRAINZ_AUTH_TOKEN=your_token_if_needed
LISTENBRAINZ_PLAYLIST_TYPE=createdfor
LISTENBRAINZ_PLAYLIST_ID=
LISTENBRAINZ_JSPF_URL=
```

- Set `LISTENBRAINZ_USERNAME` to let the UI load both `createdfor` and your own ListenBrainz playlists into a chooser.
- Use `LISTENBRAINZ_PLAYLIST_ID` to pin a specific playlist UUID by default.
- Use `LISTENBRAINZ_JSPF_URL` only if you want to override the API lookup with a direct export URL.
- In the browser UI, you can now pick which ListenBrainz playlist to import before clicking **Upload and review playlist**.
- On the review screen, you can either **Create/update Navidrome playlist now** or trigger the Octo-Fiesta sync job.

### Octo-Fiesta sync configuration

To enable real match-and-download sync, set these values in `.env`:

```env
OCTO_FIESTA_BASE_URL=http://octo-fiesta:5274
OCTO_FIESTA_USERNAME=your_navidrome_username
OCTO_FIESTA_PASSWORD=your_navidrome_password_or_enc_value
OCTO_FIESTA_PROVIDER=deezer
OCTO_FIESTA_MATCH_THRESHOLD=72
```

The app uses the documented Octo-Fiesta/Subsonic endpoints:
- `GET /rest/search3` to search local + external matches
- `GET /rest/stream?id=ext-deezer-song-...` to trigger downloads for missing tracks

Sync is intentionally **sequential**: the app waits for each Octo-Fiesta stream/download to finish before moving to the next track, then records per-track completion feedback.

### Docker Compose

```powershell
Copy-Item .env.example .env
docker compose up --build
```

Open <http://127.0.0.1:3000>.

If you want the app to write `.m3u` playlists directly into the folder Navidrome watches, set these values in `.env` before starting Compose:

```env
NAVIDROME_PLAYLISTS_DIR=/app/data/navidrome_playlists
NAVIDROME_PLAYLISTS_DIR_HOST=/absolute/path/on/your/docker-host/navidrome/playlists
```

The Compose file now mounts that host directory into the container. When a sync completes, the app writes a Navidrome-compatible playlist there, and recurring daily/weekly playlist names are normalized to stable filenames so new runs overwrite the previous update instead of piling up dated duplicates. Missing tracks are kept listed in the exported playlist while Octo-Fiesta works on filling them in.

## Project layout

```text
app/
  matching/      Deezer-oriented normalization and ranking helpers
  parsers/       M3U and JSPF ingestion
  routes/        Web and API endpoints
  services/      ListenBrainz and Octo-Fiesta workflow helpers
  templates/     Minimal review UI
tests/           Parser, matching, and app smoke tests
```

## Notes

- The real sync path is now wired to Octo-Fiesta's documented `search3` and `stream` endpoints.
- Deezer search support is structured for balanced fuzzy matching, with low-confidence matches skipped instead of forced.
