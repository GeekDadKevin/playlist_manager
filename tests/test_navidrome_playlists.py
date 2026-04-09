from __future__ import annotations

from app.services.navidrome_playlists import export_navidrome_playlist


def test_export_navidrome_playlist_writes_stable_weekly_file_and_removes_old_variant(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.delenv("NAVIDROME_MUSIC_ROOT", raising=False)
    monkeypatch.delenv("NAVIDROME_M3U_PATH_PREFIX", raising=False)
    monkeypatch.delenv("DEEZER_DOWNLOAD_DIR", raising=False)
    legacy_file = tmp_path / "weekly-exploration-for-geekdadkevin-week-of-2026-03-30-mon.m3u"
    legacy_file.write_text("#EXTM3U\nlegacy.mp3\n", encoding="utf-8")

    result = export_navidrome_playlist(
        playlist_dir=tmp_path,
        playlist_name="Weekly Exploration for geekdadkevin, week of 2026-04-06 Mon",
        sync_results=[
            {
                "track": {
                    "title": "Shake It Off",
                    "artist": "Taylor Swift",
                    "duration_seconds": 242,
                    "source": "https://musicbrainz.org/recording/123",
                },
                "match": {
                    "path": "Taylor Swift/1989/01 - Shake It Off.flac",
                },
            },
            {
                "track": {
                    "title": "Crutch",
                    "artist": "Theory of a Deadman",
                    "duration_seconds": 210,
                    "source": (
                        "Theory of a Deadman/Scars & Souvenirs (Special Edition)/"
                        "05 - Crutch (1).mp3"
                    ),
                },
                "match": {},
            },
        ],
    )

    assert result["written"] is True
    assert result["is_recurring"] is True
    assert result["target_path"].endswith("Weekly Exploration.m3u")
    assert legacy_file.exists() is False

    written = (tmp_path / "Weekly Exploration.m3u").read_text(encoding="utf-8")
    assert "#EXTM3U" in written
    assert "Taylor Swift/1989/01 - Shake It Off.flac" in written
    assert "Theory of a Deadman/Scars & Souvenirs (Special Edition)/05 - Crutch (1).mp3" in written


def test_export_navidrome_playlist_keeps_missing_tracks_as_comments(tmp_path) -> None:
    result = export_navidrome_playlist(
        playlist_dir=tmp_path,
        playlist_name="Top Discoveries of 2025",
        sync_results=[
            {
                "track": {
                    "title": "Unknown Song",
                    "artist": "Unknown Artist",
                    "source": "https://musicbrainz.org/recording/456",
                },
                "match": {},
            }
        ],
    )

    assert result["written"] is True
    assert result["entry_count"] == 1
    assert result["missing_count"] == 1

    written = (tmp_path / "top-discoveries-of-2025.m3u").read_text(encoding="utf-8")
    assert "#EXTM3U" in written
    assert "# MISSING: Unknown Artist - Unknown Song" in written
    assert "https://musicbrainz.org/recording/456" in written


def test_export_navidrome_playlist_rewrites_deezer_download_path_to_relative_entry(
    tmp_path,
) -> None:
    playlist_dir = tmp_path / "playlists"
    result = export_navidrome_playlist(
        playlist_dir=playlist_dir,
        playlist_name="Punk meets Pop",
        sync_results=[
            {
                "track": {
                    "title": "Hail to the King",
                    "artist": "Avenged Sevenfold",
                    "duration_seconds": 305,
                    "source": "https://musicbrainz.org/recording/hail-to-the-king",
                },
                "resolved_match": {
                    "path": (
                        "/app/downloads/Avenged Sevenfold/"
                        "Hail to the King/02 - Hail to the King.flac"
                    )
                },
            }
        ],
    )

    assert result["written"] is True
    written = (playlist_dir / "punk-meets-pop.m3u").read_text(encoding="utf-8")
    assert "#EXTINF:305,Avenged Sevenfold - Hail to the King" in written
    assert "../Avenged Sevenfold/Hail to the King/02 - Hail to the King.flac" in written


def test_export_navidrome_playlist_marks_unverified_source_path_as_missing(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.delenv("NAVIDROME_MUSIC_ROOT", raising=False)
    monkeypatch.delenv("NAVIDROME_M3U_PATH_PREFIX", raising=False)
    monkeypatch.delenv("DEEZER_DOWNLOAD_DIR", raising=False)
    playlist_dir = tmp_path / "playlists"

    result = export_navidrome_playlist(
        playlist_dir=playlist_dir,
        playlist_name="Broken source fallback",
        sync_results=[
            {
                "track": {
                    "title": "Linoleum",
                    "artist": "NOFX",
                    "duration_seconds": 130,
                    "source": "../NOFX/Punk In Drublic/01-01 - Linoleum.ogg",
                },
                "match": {},
            }
        ],
    )

    assert result["written"] is True
    assert result["playable_count"] == 0
    assert result["missing_count"] == 1

    written = (playlist_dir / "broken-source-fallback.m3u").read_text(encoding="utf-8")
    assert "# MISSING: NOFX - Linoleum" in written
    assert "\n../NOFX/Punk In Drublic/01-01 - Linoleum.ogg\n" not in written


def test_export_navidrome_playlist_resolves_real_file_from_shared_music_root(
    tmp_path,
    monkeypatch,
) -> None:
    music_root = tmp_path / "music"
    playlist_dir = music_root / "playlists"
    actual_dir = music_root / "Better Than Ezra" / "Deluxe"
    actual_dir.mkdir(parents=True, exist_ok=True)
    actual_file = actual_dir / "Better Than Ezra - Deluxe - Good.ogg"
    actual_file.write_bytes(b"ogg")

    monkeypatch.setenv("NAVIDROME_MUSIC_ROOT", str(music_root))
    monkeypatch.setenv("NAVIDROME_M3U_PATH_PREFIX", "..")
    monkeypatch.setenv("DEEZER_DOWNLOAD_DIR", str(music_root))

    result = export_navidrome_playlist(
        playlist_dir=playlist_dir,
        playlist_name="Weekly Jams",
        sync_results=[
            {
                "track": {
                    "title": "Good",
                    "artist": "Better Than Ezra",
                    "album": "Deluxe",
                    "duration_seconds": 185,
                    "source": "../Better Than Ezra/Deluxe/01-02 - Good.ogg",
                },
                "match": {
                    "path": "Better Than Ezra/Deluxe/01-02 - Good.ogg",
                    "source_kind": "local",
                },
                "resolved_match": {
                    "path": "Better Than Ezra/Deluxe/01-02 - Good.ogg",
                },
            }
        ],
    )

    assert result["written"] is True
    assert result["playable_count"] == 1
    assert result["missing_count"] == 0

    written = (playlist_dir / "Weekly Jams.m3u").read_text(encoding="utf-8")
    assert "../Better Than Ezra/Deluxe/Better Than Ezra - Deluxe - Good.ogg" in written
    assert "../Better Than Ezra/Deluxe/01-02 - Good.ogg" not in written
