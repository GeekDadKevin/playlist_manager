from __future__ import annotations

import base64
import logging
import os
from pathlib import Path
from typing import Any

import httpx

log = logging.getLogger(__name__)

_PLACEHOLDER_JPEG_BASE64 = (
    "/9j/4AAQSkZJRgABAQAAAQABAAD/2wCEAAkGBxISEhUSEhIVEhUVFRUVFRUV"
    "FRUVFRUWFhUVFRUYHSggGBolHRUVITEhJSkrLi4uFx8zODMtNygtLisBCgoKDg0OFxAQ"
    "Fy0dHR0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0t"
    "Lf/AABEIAAEAAQMBIgACEQEDEQH/xAAXAAEBAQEAAAAAAAAAAAAAAAAAAQID/8QAFhAB"
    "AQEAAAAAAAAAAAAAAAAAAQAC/9oADAMBAAIQAxAAAAGfAP/EABYQAQEBAAAAAAAAAAAAA"
    "AAAAAABEf/aAAgBAQABBQJb/8QAFBEBAAAAAAAAAAAAAAAAAAAAAP/aAAgBAwEBPwF//8Q"
    "AFBEBAAAAAAAAAAAAAAAAAAAAAP/aAAgBAgEBPwF//8QAFhABAQEAAAAAAAAAAAAAAAAA"
    "ABEh/9oACAEBAAY/Ai2f/8QAFhABAQEAAAAAAAAAAAAAAAAAABEh/9oACAEBAAE/IVZ//9k="
)


def ensure_cover_art(
    album_dir: Path,
    *,
    cover_url: str | None = None,
    fallback_title: str = "",
    fallback_artist: str = "",
    fallback_album: str = "",
    listenbrainz_base_url: str = "",
    listenbrainz_token: str = "",
    client: httpx.Client | None = None,
) -> Path:
    """Ensure album_dir/cover.jpg exists, downloading or generating a fallback."""
    album_dir.mkdir(parents=True, exist_ok=True)
    cover_path = album_dir / "cover.jpg"
    if cover_path.exists():
        return cover_path

    if cover_url:
        url = str(cover_url).strip()
        if url:
            try:
                downloaded = _download_image(url, client=client)
                if downloaded:
                    cover_path.write_bytes(downloaded)
                    return cover_path
            except Exception as exc:
                log.warning("Cover art download failed for %s: %s", url, exc)

    lookup_artist = fallback_artist.strip()
    lookup_title = fallback_title.strip()
    if lookup_artist and lookup_title:
        base_url = listenbrainz_base_url.strip() or os.getenv(
            "LISTENBRAINZ_API_BASE_URL", "https://api.listenbrainz.org"
        )
        token = listenbrainz_token.strip() or os.getenv("LISTENBRAINZ_AUTH_TOKEN", "")
        if token:
            try:
                lookup = _listenbrainz_lookup(
                    lookup_artist,
                    lookup_title,
                    fallback_album.strip(),
                    base_url,
                    token,
                    client=client,
                )
            except Exception as exc:
                log.warning(
                    "ListenBrainz lookup failed for %s - %s: %s",
                    lookup_artist,
                    lookup_title,
                    exc,
                )
                lookup = {}

            release_mbid = str(lookup.get("release_mbid") or "").strip()
            if release_mbid:
                downloaded = _download_cover_art_archive(release_mbid, client=client)
                if downloaded:
                    cover_path.write_bytes(downloaded)
                    return cover_path

            artist_mbids = _ensure_list(lookup.get("artist_mbids"))
            downloaded = _download_artist_image(artist_mbids, client=client)
            if downloaded:
                cover_path.write_bytes(downloaded)
                return cover_path

    cover_path.write_bytes(base64.b64decode(_PLACEHOLDER_JPEG_BASE64))
    return cover_path


def pick_thumbnail_url(info: dict[str, Any]) -> str:
    """Pick the best thumbnail URL available in a yt-dlp info dict."""
    direct = str(info.get("thumbnail") or "").strip()
    if direct:
        return direct

    thumbs = info.get("thumbnails")
    if isinstance(thumbs, list):
        for item in reversed(thumbs):
            if not isinstance(item, dict):
                continue
            url = str(item.get("url") or "").strip()
            if url:
                return url
    return ""


def _listenbrainz_lookup(
    artist_name: str,
    recording_name: str,
    release_name: str,
    base_url: str,
    token: str,
    *,
    client: httpx.Client | None = None,
) -> dict[str, Any]:
    params = {
        "artist_name": artist_name,
        "recording_name": recording_name,
    }
    if release_name:
        params["release_name"] = release_name

    headers = {"Authorization": f"Token {token}"}
    url = f"{base_url.rstrip('/')}/1/metadata/lookup/"
    response = _request_json(url, params=params, headers=headers, client=client)
    return response if isinstance(response, dict) else {}


def _download_cover_art_archive(
    release_mbid: str,
    *,
    client: httpx.Client | None = None,
) -> bytes | None:
    if not release_mbid:
        return None
    url = f"https://coverartarchive.org/release/{release_mbid}/front-500"
    return _download_image(url, client=client)


def _download_artist_image(
    artist_mbids: list[str],
    *,
    client: httpx.Client | None = None,
) -> bytes | None:
    for artist_mbid in artist_mbids:
        wikidata_id = _lookup_wikidata_id(artist_mbid, client=client)
        if not wikidata_id:
            continue
        filename = _lookup_wikidata_image(wikidata_id, client=client)
        if not filename:
            continue
        url = _lookup_commons_image_url(filename, client=client)
        if not url:
            continue
        downloaded = _download_image(url, client=client)
        if downloaded:
            return downloaded
    return None


def _lookup_wikidata_id(artist_mbid: str, *, client: httpx.Client | None = None) -> str:
    url = f"https://musicbrainz.org/ws/2/artist/{artist_mbid}"
    params = {"fmt": "json", "inc": "url-rels"}
    response = _request_json(url, params=params, client=client)
    relations = response.get("relations") if isinstance(response, dict) else []
    for relation in relations or []:
        if not isinstance(relation, dict):
            continue
        if relation.get("type") != "wikidata":
            continue
        resource = str(relation.get("url", {}).get("resource") or "").strip()
        if "/wiki/" in resource:
            return resource.rsplit("/", 1)[-1]
    return ""


def _lookup_wikidata_image(wikidata_id: str, *, client: httpx.Client | None = None) -> str:
    url = f"https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json"
    response = _request_json(url, client=client)
    entities = response.get("entities") if isinstance(response, dict) else {}
    entity = entities.get(wikidata_id) if isinstance(entities, dict) else None
    claims = entity.get("claims") if isinstance(entity, dict) else {}
    p18 = claims.get("P18") if isinstance(claims, dict) else None
    if not isinstance(p18, list) or not p18:
        return ""
    mainsnak = p18[0].get("mainsnak") if isinstance(p18[0], dict) else None
    datavalue = mainsnak.get("datavalue") if isinstance(mainsnak, dict) else None
    return str(datavalue.get("value") or "").strip() if isinstance(datavalue, dict) else ""


def _lookup_commons_image_url(filename: str, *, client: httpx.Client | None = None) -> str:
    if not filename:
        return ""
    params = {
        "action": "query",
        "titles": f"File:{filename}",
        "prop": "imageinfo",
        "iiprop": "url",
        "format": "json",
    }
    response = _request_json(
        "https://commons.wikimedia.org/w/api.php",
        params=params,
        client=client,
    )
    pages = response.get("query", {}).get("pages") if isinstance(response, dict) else {}
    if not isinstance(pages, dict):
        return ""
    for page in pages.values():
        if not isinstance(page, dict):
            continue
        imageinfo = page.get("imageinfo")
        if isinstance(imageinfo, list) and imageinfo:
            url = str(imageinfo[0].get("url") or "").strip()
            if url:
                return url
    return ""


def _download_image(url: str, *, client: httpx.Client | None = None) -> bytes | None:
    response = _request_binary(url, client=client)
    if response is None:
        return None
    content_type = str(response.headers.get("Content-Type", "")).lower()
    content = response.content
    if "svg" in content_type:
        return None
    if "image" in content_type:
        return content
    if content.startswith(b"\xff\xd8") or content.startswith(b"\x89PNG"):
        return content
    return None


def _request_json(
    url: str,
    *,
    params: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    client: httpx.Client | None = None,
) -> dict[str, Any]:
    if client is None:
        with httpx.Client(timeout=10.0, follow_redirects=True) as session:
            response = session.get(url, params=params, headers=headers)
    else:
        response = client.get(url, params=params, headers=headers, timeout=10.0)
    response.raise_for_status()
    payload = response.json()
    return payload if isinstance(payload, dict) else {}


def _request_binary(
    url: str,
    *,
    client: httpx.Client | None = None,
) -> httpx.Response | None:
    if client is None:
        with httpx.Client(timeout=10.0, follow_redirects=True) as session:
            response = session.get(url)
    else:
        response = client.get(url, timeout=10.0)
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response


def _ensure_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    return []
