from __future__ import annotations

import httpx


class ListenBrainzService:
    """Lightweight helper for fetching remote JSPF-style playlist JSON."""

    def fetch_jspf_document(self, url: str) -> dict:
        with httpx.Client(follow_redirects=True, timeout=15.0) as client:
            response = client.get(url)
            response.raise_for_status()
        return response.json()
