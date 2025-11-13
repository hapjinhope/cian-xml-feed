"""FastAPI application entrypoint."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import Response

from .config import get_settings
from .feed_builder import build_feed
from .supabase_client import SupabaseClient

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

settings = get_settings()
app = FastAPI(title=settings.app_name)

supabase_client: SupabaseClient | None = None
if settings.supabase_url and settings.supabase_key:
    try:
        supabase_client = SupabaseClient(settings.supabase_url, settings.supabase_key)
        logger.info("Supabase client initialized")
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to initialize Supabase client: %s", exc)
        supabase_client = None
else:
    logger.warning("Supabase credentials are not configured")


def _ensure_supabase() -> SupabaseClient:
    if supabase_client is None:
        raise HTTPException(
            status_code=500,
            detail="Supabase не сконфигурирован. Проверьте переменные окружения.",
        )
    return supabase_client


def _is_published(row: dict[str, Any]) -> bool:
    status = row.get("status")
    if isinstance(status, str):
        return status.strip().lower() == "published"
    if isinstance(status, bool):
        return bool(status)
    return False


@app.get("/feed.xml")
def get_feed() -> Response:
    """Return the XML feed for CIAN."""
    client = _ensure_supabase()
    try:
        apartments_raw = client.fetch_objects()
        apartments = [apt for apt in apartments_raw if _is_published(apt)]
        if not apartments:
            logger.info("No apartments found, returning empty feed")
            empty = (
                '<?xml version="1.0" encoding="UTF-8"?>'
                "<Feed><Feed_Version>2</Feed_Version></Feed>"
            )
            return Response(content=empty, media_type="application/xml")

        xml_feed = build_feed(apartments)
        logger.info("Generated feed with %s apartments", len(apartments))
        return Response(content=xml_feed, media_type="application/xml")
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        logger.exception("Unexpected error while building feed: %s", exc)
        raise HTTPException(status_code=500, detail="Internal server error") from exc


@app.get("/health")
def health() -> dict[str, Any]:
    """Simple health check endpoint."""
    status = "ok" if supabase_client else "supabase_not_configured"
    return {"status": status}


@app.get("/api/count")
def count_apartments() -> dict[str, Any]:
    """Return the number of apartments."""
    client = _ensure_supabase()
    try:
        return {"count": client.count_objects()}
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to count apartments: %s", exc)
        raise HTTPException(
            status_code=500, detail="Ошибка при обращении к Supabase"
        ) from exc
