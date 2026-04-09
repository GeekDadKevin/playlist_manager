from __future__ import annotations

from app.services.song_metadata import (
    backfill_missing_song_xml,
    guess_track_metadata,
    write_flac_tags,
)


def test_guess_track_metadata_from_artist_album_track_filename() -> None:
    result = guess_track_metadata("Type O Negative - October Rust - Be My Druidess.flac")

    assert result == {
        "title": "Be My Druidess",
        "artist": "Type O Negative",
        "album": "October Rust",
    }


def test_backfill_missing_song_xml_creates_sidecar_from_filename(tmp_path, monkeypatch) -> None:
    audio_path = tmp_path / "Type O Negative - October Rust - Be My Druidess.flac"
    audio_path.write_bytes(b"flac")
    calls: list[str] = []

    def fake_write_flac_tags(*args, **kwargs):
        calls.append(str(args[0]))

    monkeypatch.setattr("app.services.song_metadata.write_flac_tags", fake_write_flac_tags)

    summary = backfill_missing_song_xml(tmp_path)

    metadata_path = audio_path.with_suffix(".xml")
    assert summary["scanned"] == 1
    assert summary["created"] == 1
    assert metadata_path.exists()
    assert str(audio_path) in calls

    metadata_xml = metadata_path.read_text(encoding="utf-8")
    assert "<song>" in metadata_xml
    assert "<title>Be My Druidess</title>" in metadata_xml
    assert "<performingartist>Type O Negative</performingartist>" in metadata_xml
    assert "<albumtitle>October Rust</albumtitle>" in metadata_xml


def test_write_flac_tags_uses_listenbrainz_and_deezer_metadata(tmp_path, monkeypatch) -> None:
    audio_path = tmp_path / "Type O Negative - October Rust - Be My Druidess.flac"
    audio_path.write_bytes(b"not-a-real-flac")
    captured: dict[str, object] = {}

    class FakeFLAC(dict):
        def __init__(self, filename):
            captured["filename"] = filename

        def __setitem__(self, key, value):
            captured.setdefault("tags", {})[key] = value

        def save(self):
            captured["saved"] = True

    monkeypatch.setattr("app.services.song_metadata.FLAC", FakeFLAC)

    write_flac_tags(
        audio_path,
        title="Be My Druidess",
        artist="Type O Negative",
        album="October Rust",
        provider="deezer",
        deezer_id=12345,
        deezer_artist_id=99,
        deezer_album_id=77,
        deezer_link="https://www.deezer.com/track/12345",
        source="https://musicbrainz.org/recording/abcd1234-0000-1111-2222-abcdefabcdef",
        annotation="ListenBrainz weekly pick",
    )

    tags = captured["tags"]
    assert captured["saved"] is True
    assert tags["TITLE"] == ["Be My Druidess"]
    assert tags["ARTIST"] == ["Type O Negative"]
    assert tags["ALBUM"] == ["October Rust"]
    assert tags["DEEZER_TRACK_ID"] == ["12345"]
    assert tags["MUSICBRAINZ_TRACKID"] == ["abcd1234-0000-1111-2222-abcdefabcdef"]
    assert tags["COMMENT"] == ["ListenBrainz weekly pick"]
