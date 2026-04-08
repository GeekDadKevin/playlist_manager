from __future__ import annotations

from app.services.navidrome_playlists import export_navidrome_playlist


def test_export_navidrome_playlist_writes_stable_weekly_file_and_removes_old_variant(
    tmp_path,
) -> None:
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
    assert result["target_path"].endswith("weekly-exploration.m3u")
    assert legacy_file.exists() is False

    written = (tmp_path / "weekly-exploration.m3u").read_text(encoding="utf-8")
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
