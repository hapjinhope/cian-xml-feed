"""Lightweight client for Supabase REST API."""

from __future__ import annotations

from typing import Any

import httpx


class SupabaseClient:
    """Minimal Supabase REST client that only fetches data we need."""

    def __init__(self, base_url: str, service_key: str) -> None:
        if not base_url or not service_key:
            raise ValueError("Supabase URL and service key must be provided")

        self._base_url = base_url.rstrip("/")
        self._headers = {
            "apikey": service_key,
            "Authorization": f"Bearer {service_key}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def fetch_objects(self) -> list[dict[str, Any]]:
        """Return all objects rows."""
        with httpx.Client(timeout=30) as client:
            response = client.get(
                f"{self._base_url}/rest/v1/objects",
                params={"select": "*"},
                headers=self._headers,
            )
            response.raise_for_status()
            data = response.json()
            return data if isinstance(data, list) else []

    def count_objects(self) -> int:
        """Return objects count using Supabase count preference."""
        with httpx.Client(timeout=30) as client:
            response = client.get(
                f"{self._base_url}/rest/v1/objects",
                params={"select": "id"},
                headers={**self._headers, "Prefer": "count=exact"},
            )
            response.raise_for_status()
            count_header = response.headers.get("content-range", "0-0/0")
            try:
                total = count_header.split("/")[-1]
                return int(total)
            except (ValueError, AttributeError):
                return 0
