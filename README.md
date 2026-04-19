# JSPF Converter / Playlist Sync

A Docker-friendly Flask service for importing playlists from `m3u`, `jspf`, Navidrome missing-files `csv`, or ListenBrainz playlist sources, reviewing track metadata, downloading missing tracks from Deezer and SoundCloud, and writing Navidrome-friendly playlist files.

## Stack

- `Flask` for the API and lightweight web UI
- `uv` for environment, dependency, and command management
- `ruff` for linting and formatting
- `pytest` for parser and matching tests
- `docker compose` for local orchestration
- `ffmpeg` inside the Docker image for decode-based audio validation

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
#Kevinwashere
Both scripts run `uv sync --dev`, validate the current `.env`, and then start the single Flask app that serves both the web UI and the API on **`http://127.0.0.1:3000` only**. If a variable is invalid, startup stops immediately and prints the exact variable name(s) that need fixing. Re-running either script reads `.app.lock`, stops the previously started app process, clears any stale listener on port `3000`, and writes the new PID back to that lock file.

### ListenBrainz playlist configuration

To import ListenBrainz playlists through the browser chooser, set these optional values in `.env`:

```env
LISTENBRAINZ_API_BASE_URL=https://api.listenbrainz.org
LISTENBRAINZ_USERNAME=your_listenbrainz_username
LISTENBRAINZ_AUTH_TOKEN=your_token_if_needed
ACOUSTID_API_KEY=your_acoustid_api_key
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

For local Windows or shell testing, `.env` is the authoritative config source. `config.json` is intended for container deployment and is auto-applied only when the app is running inside Docker. If you want a local run to inherit `config.json`, set `PLAYLIST_MANAGER_USE_CONFIG_JSON=1` before startup.
Use [config.json.example](config.json.example) as a shareable Docker-oriented template without secrets.

If Docker logs show a SoundCloud message like `_ssl.c:993: The handshake operation timed out`, the app now retries those lookups and forces IPv4 by default. You can further raise `SOUNDCLOUD_REQUEST_TIMEOUT` in `.env` if your network is slow.

### Audio fingerprint fallback

For badly tagged files where filename, embedded tags, and XML are all unreliable, the Library Tools page includes **Identify Tracks By Audio**. It uses `fpcalc` plus the AcoustID API to fingerprint the audio itself, then resolves the best MusicBrainz recording metadata before writing tags and XML.

Optional `.env` or Docker config values:

```env
ACOUSTID_API_KEY=your_acoustid_api_key
ACOUSTID_LOOKUP_TIMEOUT=20
ACOUSTID_SCORE_THRESHOLD=0.9
ACOUSTID_FINGERPRINT_LENGTH=120
FPCALC_BIN=
```

- Only high-confidence AcoustID matches are applied automatically; low-confidence matches are left unresolved and reported in the tool log.
- Docker installs `fpcalc` through `chromaprint-tools`; for local Windows runs, put `fpcalc` on `PATH` or set `FPCALC_BIN` explicitly.
- This fallback is intentionally separate from normal tag repair because it is slower and depends on an external lookup service.

### Docker Compose

```powershell
Copy-Item .env.example .env
docker compose up -d --build
```

Open <http://127.0.0.1:3000>.

The Compose setup is intended to run from the built image directly. It no longer depends on a repo bind mount or a checked-in `config.json`, so a fresh machine only needs this repo, Docker, and a valid `.env` file.

#### Healthcheck and smoke test

The container now includes a healthcheck that waits for the app to respond at `/api/health`. You can also run a manual smoke test:

```sh
docker compose up -d --build
docker compose exec web python scripts/smoke_check.py
```

If the healthcheck fails, check your `.env` for valid `NAVIDROME_MUSIC_ROOT` and `NAVIDROME_PLAYLIST_DIR` host paths, and make sure those folders exist and are accessible from the Docker host.

For local Windows testing with [start.ps1](start.ps1), keep `DATA_DIR=./data` in `.env` so the SQLite databases and logs stay under the repo instead of landing in a container-style `/app/data` path.

If you want the app to write `.m3u` playlists directly into the folder Navidrome watches, set these values in `.env` before starting Compose:

```env
NAVIDROME_PLAYLIST_DIR=/absolute/path/on/your/docker-host/navidrome/playlists
NAVIDROME_MUSIC_ROOT=/absolute/path/on/your/docker-host/music/root
NAVIDROME_M3U_PATH_PREFIX=..
```

Docker Compose mounts those host folders into `/navidrome/playlist` and `/navidrome/root` inside the container, so the app always works with stable in-container paths while you only configure real host locations in `.env`.

Inside Docker, `DATA_DIR` is forced to `/app/data`, `fpcalc` is provided by the image, and the app ignores `config.json` unless you explicitly re-enable it. That keeps cross-machine startup predictable and avoids host-specific local config leaking into the container.

The Docker image also installs `ffmpeg` and `fpcalc`, so the Library Tools page can run both decode-based audio integrity scans and AcoustID fingerprint lookup inside the container.

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
