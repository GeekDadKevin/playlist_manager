from __future__ import annotations

from app.services.song_metadata import (
    backfill_missing_song_xml,
    guess_preliminary_metadata,
    guess_track_metadata,
    load_embedded_audio_metadata,
    repair_song_metadata_xml_ids,
    write_flac_tags,
    write_song_metadata_xml,
)


def test_guess_track_metadata_from_artist_album_track_filename() -> None:
    result = guess_track_metadata("Type O Negative - October Rust - Be My Druidess.flac")

    assert result == {
        "title": "Be My Druidess",
        "artist": "Type O Negative",
        "album": "October Rust",
    }


def test_guess_preliminary_metadata_handles_va_folder_layout() -> None:
    result = guess_preliminary_metadata(
        "Various Artists/Warp Sampler/Autechre - Warp Sampler - 02 - Cichli.flac",
    )

    assert result == {
        "title": "Cichli",
        "artist": "Autechre",
        "album": "Warp Sampler",
        "albumartist": "Various Artists",
        "track_number": "2",
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
    assert tags["AUDIO_EXTENSION"] == [".flac"]
    assert tags["DEEZER_TRACK_ID"] == ["12345"]
    assert tags["MUSICBRAINZ_TRACKID"] == ["abcd1234-0000-1111-2222-abcdefabcdef"]
    assert tags["COMMENT"] == ["ListenBrainz weekly pick"]


def test_write_song_metadata_xml_records_downloaded_from(tmp_path) -> None:
    audio_path = tmp_path / "Autechre - Tri Repetae - Dael.flac"
    audio_path.write_bytes(b"fake")

    metadata_path = write_song_metadata_xml(
        audio_path,
        title="Dael",
        artist="Autechre",
        album="Tri Repetae",
        provider="youtube",
        downloaded_from="youtube",
        source="https://www.youtube.com/watch?v=abc123",
    )

    metadata_xml = metadata_path.read_text(encoding="utf-8")
    assert "<downloadedfrom>youtube</downloadedfrom>" in metadata_xml
    assert "<audioextension>.flac</audioextension>" in metadata_xml


def test_write_song_metadata_xml_records_musicbrainz_extra_fields(tmp_path) -> None:
    audio_path = tmp_path / "Autechre - Tri Repetae - Dael.flac"
    audio_path.write_bytes(b"fake")

    metadata_path = write_song_metadata_xml(
        audio_path,
        title="Dael",
        artist="Autechre",
        album="Tri Repetae",
        musicbrainz_track_id="track-123",
        extra_fields={
            "musicbrainzalbumid": "release-456",
            "musicbrainzreleasegroupid": "group-789",
            "genre": "IDM",
            "barcode": "724384559524",
        },
    )

    metadata_xml = metadata_path.read_text(encoding="utf-8")
    assert "<musicbrainzalbumid>release-456</musicbrainzalbumid>" in metadata_xml
    assert "<musicbrainzreleasegroupid>group-789</musicbrainzreleasegroupid>" in metadata_xml
    assert "<genre>IDM</genre>" in metadata_xml
    assert "<barcode>724384559524</barcode>" in metadata_xml


def test_load_embedded_audio_metadata_reads_custom_ids(tmp_path, monkeypatch) -> None:
    audio_path = tmp_path / "Massive Attack - Mezzanine - Teardrop.flac"
    audio_path.write_bytes(b"fake-flac")

    class FakeEasyAudio(dict):
        def get(self, key, default=None):
            data = {
                "title": ["Teardrop"],
                "artist": ["Massive Attack"],
                "album": ["Mezzanine"],
                "albumartist": ["Massive Attack"],
            }
            return data.get(key, default)

    class FakeRawAudio(dict):
        def items(self):
            return iter(
                [
                    ("MUSICBRAINZ_ALBUMID", ["release-456"]),
                    ("MUSICBRAINZ_TRACKID", ["abcd1234-0000-1111-2222-abcdefabcdef"]),
                    ("MUSICBRAINZ_ARTISTID", ["artist-789"]),
                    ("MUSICBRAINZ_ALBUMARTISTID", ["albumartist-789"]),
                    ("MUSICBRAINZ_RELEASEGROUPID", ["group-123"]),
                    ("ARTISTSORT", ["Attack, Massive"]),
                    ("ALBUMARTISTSORT", ["Attack, Massive"]),
                    ("TRACKTOTAL", ["11"]),
                    ("DISCNUMBER", ["1"]),
                    ("DISCTOTAL", ["1"]),
                    ("DATE", ["1998-04-20"]),
                    ("ORIGINALDATE", ["1998-04-20"]),
                    ("GENRE", ["Trip Hop"]),
                    ("ISRC", ["GBBKS9801234"]),
                    ("BARCODE", ["724384559524"]),
                    ("LABEL", ["Virgin"]),
                    ("CATALOGNUMBER", ["WBRCD2"]),
                    ("MEDIA", ["CD"]),
                    ("RELEASECOUNTRY", ["GB"]),
                    ("RELEASESTATUS", ["Official"]),
                    ("RELEASETYPE", ["Album"]),
                    ("RELEASESECONDARYTYPES", ["Compilation"]),
                    ("LANGUAGE", ["eng"]),
                    ("SCRIPT", ["Latn"]),
                    ("RECORDINGDISAMBIGUATION", ["album version"]),
                    ("ALBUMDISAMBIGUATION", ["2019 remaster"]),
                    ("DEEZER_TRACK_ID", ["12345"]),
                    ("DEEZER_ARTIST_ID", ["99"]),
                    ("DEEZER_ALBUM_ID", ["77"]),
                    ("DEEZER_LINK", ["https://www.deezer.com/track/12345"]),
                ]
            )

    def fake_mutagen_file(path, easy=False):
        return FakeEasyAudio() if easy else FakeRawAudio()

    monkeypatch.setattr("app.services.song_metadata._mutagen_file", fake_mutagen_file)

    metadata = load_embedded_audio_metadata(audio_path)

    assert metadata["title"] == "Teardrop"
    assert metadata["artist"] == "Massive Attack"
    assert metadata["musicbrainz_album_id"] == "release-456"
    assert metadata["musicbrainz_track_id"] == "abcd1234-0000-1111-2222-abcdefabcdef"
    assert metadata["musicbrainz_artist_id"] == "artist-789"
    assert metadata["musicbrainz_albumartist_id"] == "albumartist-789"
    assert metadata["musicbrainz_release_group_id"] == "group-123"
    assert metadata["artist_sort"] == "Attack, Massive"
    assert metadata["albumartist_sort"] == "Attack, Massive"
    assert metadata["track_total"] == "11"
    assert metadata["disc_number"] == "1"
    assert metadata["disc_total"] == "1"
    assert metadata["date"] == "1998-04-20"
    assert metadata["original_date"] == "1998-04-20"
    assert metadata["genre"] == "Trip Hop"
    assert metadata["isrc"] == "GBBKS9801234"
    assert metadata["barcode"] == "724384559524"
    assert metadata["label"] == "Virgin"
    assert metadata["catalog_number"] == "WBRCD2"
    assert metadata["media_format"] == "CD"
    assert metadata["release_country"] == "GB"
    assert metadata["release_status"] == "Official"
    assert metadata["release_type"] == "Album"
    assert metadata["release_secondary_types"] == "Compilation"
    assert metadata["language"] == "eng"
    assert metadata["script"] == "Latn"
    assert metadata["recording_disambiguation"] == "album version"
    assert metadata["album_disambiguation"] == "2019 remaster"
    assert metadata["deezer_id"] == "12345"
    assert metadata["deezer_artist_id"] == "99"
    assert metadata["deezer_album_id"] == "77"


def test_load_embedded_audio_metadata_reads_mp4_and_id3_style_custom_ids(
    tmp_path,
    monkeypatch,
) -> None:
    audio_path = tmp_path / "Massive Attack - Mezzanine - Teardrop.m4a"
    audio_path.write_bytes(b"fake-m4a")

    class FakeEasyAudio(dict):
        def get(self, key, default=None):
            return default

    class FakeRawAudio(dict):
        def items(self):
            return iter(
                [
                    ("----:com.apple.iTunes:MusicBrainz Album Id", [b"release-456"]),
                    (
                        "----:com.apple.iTunes:MusicBrainz Track Id",
                        [b"abcd1234-0000-1111-2222-abcdefabcdef"],
                    ),
                    ("TXXX:MusicBrainz Artist Id", [b"artist-789"]),
                    ("TXXX:MusicBrainz Album Artist Id", [b"albumartist-789"]),
                    ("TXXX:DEEZER_TRACK_ID", [b"12345"]),
                    ("TXXX:DEEZER_ARTIST_ID", [b"99"]),
                    ("TXXX:DEEZER_ALBUM_ID", [b"77"]),
                    ("TXXX:DEEZER_LINK", [b"https://www.deezer.com/track/12345"]),
                ]
            )

    def fake_mutagen_file(path, easy=False):
        return FakeEasyAudio() if easy else FakeRawAudio()

    monkeypatch.setattr("app.services.song_metadata._mutagen_file", fake_mutagen_file)

    metadata = load_embedded_audio_metadata(audio_path)

    assert metadata["musicbrainz_album_id"] == "release-456"
    assert metadata["musicbrainz_track_id"] == "abcd1234-0000-1111-2222-abcdefabcdef"
    assert metadata["musicbrainz_artist_id"] == "artist-789"
    assert metadata["musicbrainz_albumartist_id"] == "albumartist-789"
    assert metadata["deezer_id"] == "12345"
    assert metadata["deezer_artist_id"] == "99"
    assert metadata["deezer_album_id"] == "77"



def test_backfill_missing_song_xml_keeps_embedded_ids(tmp_path, monkeypatch) -> None:
    audio_path = tmp_path / "Massive Attack - Mezzanine - Teardrop.flac"
    audio_path.write_bytes(b"flac")

    monkeypatch.setattr(
        "app.services.song_metadata.load_embedded_audio_metadata",
        lambda path: {
            "title": "Teardrop",
            "artist": "Massive Attack",
            "album": "Mezzanine",
            "albumartist": "Massive Attack",
            "musicbrainz_track_id": "abcd1234-0000-1111-2222-abcdefabcdef",
            "deezer_id": "12345",
            "deezer_artist_id": "99",
            "deezer_album_id": "77",
            "deezer_link": "https://www.deezer.com/track/12345",
        },
    )
    monkeypatch.setattr("app.services.song_metadata.write_flac_tags", lambda *args, **kwargs: None)

    summary = backfill_missing_song_xml(tmp_path)

    metadata_path = audio_path.with_suffix(".xml")
    assert summary["created"] == 1
    metadata_xml = metadata_path.read_text(encoding="utf-8")
    assert (
        "<musicbrainztrackid>abcd1234-0000-1111-2222-abcdefabcdef</musicbrainztrackid>"
        in metadata_xml
    )
    assert "<deezerid>12345</deezerid>" in metadata_xml
    assert "<deezerartistid>99</deezerartistid>" in metadata_xml
    assert "<deezeralbumid>77</deezeralbumid>" in metadata_xml


def test_repair_song_metadata_xml_ids_updates_existing_xml_and_reports_unresolved(
    tmp_path,
    monkeypatch,
) -> None:
    audio_path = tmp_path / "Massive Attack - Mezzanine - Teardrop.flac"
    audio_path.write_bytes(b"flac")
    metadata_path = audio_path.with_suffix(".xml")
    metadata_path.write_text(
        """<?xml version=\"1.0\" encoding=\"utf-8\"?>
<song>
  <provider>deezer</provider>
  <title>Teardrop</title>
</song>
""",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "app.services.song_metadata.load_embedded_audio_metadata",
        lambda path: {
            "title": "Teardrop",
            "artist": "Massive Attack",
            "album": "Mezzanine",
            "albumartist": "Massive Attack",
            "musicbrainz_track_id": "abcd1234-0000-1111-2222-abcdefabcdef",
            "deezer_id": "12345",
            "deezer_artist_id": "99",
            "deezer_album_id": "77",
            "deezer_link": "https://www.deezer.com/track/12345",
        },
    )

    summary = repair_song_metadata_xml_ids(tmp_path)

    assert summary["scanned"] == 1
    assert summary["updated"] == 1
    assert summary["unresolved"] == 0

    metadata_xml = metadata_path.read_text(encoding="utf-8")
    assert (
        "<musicbrainztrackid>abcd1234-0000-1111-2222-abcdefabcdef</musicbrainztrackid>"
        in metadata_xml
    )
    assert "<deezerid>12345</deezerid>" in metadata_xml


def test_repair_song_metadata_xml_ids_reports_unresolved_deezer_xml(tmp_path, monkeypatch) -> None:
    audio_path = tmp_path / "Massive Attack - Mezzanine - Teardrop.flac"
    audio_path.write_bytes(b"flac")
    metadata_path = audio_path.with_suffix(".xml")
    metadata_path.write_text(
        """<?xml version=\"1.0\" encoding=\"utf-8\"?>
<song>
  <provider>deezer</provider>
  <title>Teardrop</title>
</song>
""",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "app.services.song_metadata.load_embedded_audio_metadata",
        lambda path: {
            "title": "Teardrop",
            "artist": "Massive Attack",
            "album": "Mezzanine",
            "albumartist": "Massive Attack",
            "musicbrainz_track_id": "",
            "deezer_id": "",
            "deezer_artist_id": "",
            "deezer_album_id": "",
            "deezer_link": "",
        },
    )

    summary = repair_song_metadata_xml_ids(tmp_path)

    assert summary["scanned"] == 1
    assert summary["updated"] == 0
    assert summary["unresolved"] == 1
    assert summary["unresolved_items"][0]["missing_fields"] == [
        "musicbrainztrackid",
        "deezerid",
    ]
