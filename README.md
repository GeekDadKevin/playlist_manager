# JSPF Converter / Octo Playlist Sync

A Docker-friendly Flask service for importing playlists from `m3u`, `jspf`, or ListenBrainz-compatible sources, reviewing track metadata, and preparing Octo-Fiesta download handoff payloads for Navidrome workflows.

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
uv run flask --app app run --debug
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

Both scripts run `uv sync --dev` and then start the app on `http://127.0.0.1:8000` by default.

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

Open <http://127.0.0.1:8000>.

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
