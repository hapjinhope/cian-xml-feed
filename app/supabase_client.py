"""Lightweight client for Supabase REST API."""

from __future__ import annotations

from typing import Any, Iterable

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

    @property
    def _rest_base(self) -> str:
        return f"{self._base_url}/rest/v1"

    def _get(
        self, resource: str, params: dict[str, Any] | None = None, prefer: str | None = None
    ) -> httpx.Response:
        headers = dict(self._headers)
        if prefer:
            headers["Prefer"] = prefer

        with httpx.Client(timeout=30) as client:
            response = client.get(
                f"{self._rest_base}/{resource}",
                params=params,
                headers=headers,
            )
        response.raise_for_status()
        return response

    def fetch_objects(self) -> list[dict[str, Any]]:
        """Return only published objects with attached agent info when available."""
        params = {
            "select": "*",
            "status": "eq.published",
        }
        response = self._get("objects", params=params)
        data = response.json()
        objects: list[dict[str, Any]] = data if isinstance(data, list) else []

        agent_map = self._load_agents({row.get("agent_id") for row in objects})
        if agent_map:
            for row in objects:
                agent_id = row.get("agent_id")
                lookup_id = str(agent_id) if agent_id not in (None, "") else None
                if lookup_id and lookup_id in agent_map:
                    agent = agent_map[lookup_id]
                    row["agent"] = agent
                    if agent.get("email") and not row.get("subagent_email"):
                        # Прокидываем почту агента, чтобы она попала в SubAgent в XML
                        row["subagent_email"] = agent["email"]
        return objects

    def count_objects(self) -> int:
        """Return published objects count using Supabase count preference."""
        params = {
            "select": "id",
            "status": "eq.published",
        }
        response = self._get("objects", params=params, prefer="count=exact")
        count_header = response.headers.get("content-range", "0-0/0")
        try:
            total = count_header.split("/")[-1]
            return int(total)
        except (ValueError, AttributeError):
            return 0

    def _load_agents(self, agent_ids: Iterable[Any]) -> dict[Any, dict[str, Any]]:
        # Нормализуем id в строку, чтобы избежать расхождений типов (int/str)
        ids = [str(agent_id) for agent_id in agent_ids if agent_id not in (None, "")]
        if not ids:
            return {}

        id_list = ",".join(str(agent_id) for agent_id in ids)
        params = {
            "id": f"in.({id_list})",
            "select": "id,name,first_name,last_name,surname,phone,email",
        }
        try:
            response = self._get("agents", params=params)
        except httpx.HTTPStatusError as exc:  # pragma: no cover - depend on schema
            if exc.response.status_code == 400:
                fallback = {
                    "id": f"in.({id_list})",
                    "select": "id,name,surname,phone,email",
                }
                response = self._get("agents", params=fallback)
            else:
                raise
        data = response.json()
        if not isinstance(data, list):
            return {}

        return {str(row.get("id")): row for row in data}
