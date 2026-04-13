# JSPF Converter / Playlist Sync

A Docker-friendly Flask service for importing playlists from `m3u`, `jspf`, Navidrome missing-files `csv`, or ListenBrainz playlist sources, reviewing track metadata, downloading missing tracks from Deezer and SoundCloud, and writing Navidrome-friendly playlist files.

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

To import ListenBrainz playlists through the browser chooser, set these optional values in `.env`:

```env
LISTENBRAINZ_API_BASE_URL=https://api.listenbrainz.org
LISTENBRAINZ_USERNAME=your_listenbrainz_username
LISTENBRAINZ_AUTH_TOKEN=your_token_if_needed
```

- Set `LISTENBRAINZ_USERNAME` to let the UI load both `Created For You` and your own ListenBrainz playlists into the chooser.
- You normally **do not need** playlist-type, playlist-ID, JSPF-URL, or upload-folder env overrides anymore; the UI picker and pasted URL field handle that workflow directly.
- On the review screen, you can either **Create/update Navidrome playlist now** or start the live download sync job.

### Live sync provider configuration

To enable real match-and-download sync, set these values in `.env`:

```env
DEEZER_ARL=your_deezer_session_cookie
NAVIDROME_MUSIC_ROOT=/absolute/path/on/your/docker-host/music/root
DEEZER_QUALITY=FLAC
DEEZER_MATCH_THRESHOLD=72
SOUNDCLOUD_FALLBACK=1
YOUTUBE_FALLBACK=0
SOUNDCLOUD_MATCH_THRESHOLD=72
YOUTUBE_MATCH_THRESHOLD=72
DOWNLOAD_THREADS=1
SOUNDCLOUD_REQUEST_TIMEOUT=25
SOUNDCLOUD_REQUEST_RETRIES=3
SOUNDCLOUD_FORCE_IPV4=1
```

Sync is **sequential by default** (set `DOWNLOAD_THREADS=1`), but you can raise `DOWNLOAD_THREADS` to allow parallel downloads. If a Deezer match is low-confidence, the app pauses playlist export so you can optionally choose a SoundCloud or YouTube result through `yt-dlp` during manual review, or accept the remaining low-confidence items as missing in one step.

If `config.json` exists at the repo root (mounted to `/app/config.json` in Docker), its values override any matching keys in `.env`.
Use [config.json.example](config.json.example) as a shareable template without secrets.

If Docker logs show a SoundCloud message like `_ssl.c:993: The handshake operation timed out`, the app now retries those lookups and forces IPv4 by default. You can further raise `SOUNDCLOUD_REQUEST_TIMEOUT` in `.env` if your network is slow.

### Docker Compose

```powershell
Copy-Item .env.example .env
docker compose up --build
```

Open <http://127.0.0.1:3000>.

If you want the app to write `.m3u` playlists directly into the folder Navidrome watches, set these values in `.env` before starting Compose:

```env
NAVIDROME_PLAYLIST_DIR=/absolute/path/on/your/docker-host/navidrome/playlists
NAVIDROME_MUSIC_ROOT=/absolute/path/on/your/docker-host/music/root
NAVIDROME_M3U_PATH_PREFIX=..
```

Docker Compose mounts those host folders into `/navidrome/playlist` and `/navidrome/root` inside the container, so the app always works with stable in-container paths while you only configure real host locations in `.env`.

The playlist exporter rewrites absolute paths rooted under `NAVIDROME_MUSIC_ROOT` into relative `.m3u` entries like `../Artist/Album/track.flac`, which is the format Navidrome expects when the playlist file lives inside a `playlists/` subfolder.

When a sync completes, the app writes a Navidrome-compatible playlist there, and recurring daily/weekly playlist names are normalized to stable filenames so new runs overwrite the previous update instead of piling up dated duplicates. Missing tracks are kept listed in the exported playlist while downloads are still pending.

## Project layout

```text
app/
  matching/      Multi-provider normalization and ranking helpers
  parsers/       M3U and JSPF ingestion
  routes/        Web and API endpoints
  services/      ListenBrainz, download, and playlist workflow helpers
  templates/     Minimal review UI
tests/           Parser, matching, and app smoke tests
```

## Notes

- The real sync path now uses the built-in Deezer workflow, with optional SoundCloud/YouTube choices only during low-confidence manual review.
- Matching is structured for balanced fuzzy search, with low-confidence matches skipped instead of forced.
