from __future__ import annotations

from app.services.path_template import build_download_path


def test_build_download_path_appends_audio_extension_for_dotted_titles(
    tmp_path,
) -> None:
    output_path = build_download_path(
        tmp_path,
        "{artist}/{album}/{artist} - {track} - {title}",
        artist="Massive Attack",
        album="Mezzanine",
        title="Track.Name",
        track_number=0,
        ext=".flac",
    )

    assert (
        output_path
        == tmp_path / "Massive Attack/Mezzanine/Massive Attack - 0 - Track.Name.flac"
    )


def test_build_download_path_replaces_existing_audio_extension_in_template(
    tmp_path,
) -> None:
    output_path = build_download_path(
        tmp_path,
        "{artist}/{album}/{title}.mp3",
        artist="Massive Attack",
        album="Mezzanine",
        title="Teardrop",
        track_number=1,
        ext=".flac",
    )

    assert output_path == tmp_path / "Massive Attack/Mezzanine/Teardrop.flac"
