"""Application configuration helpers."""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    supabase_url: str | None
    supabase_key: str | None
    app_name: str = "CIAN XML Feed Generator"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings(
        supabase_url=os.getenv("SUPABASE_URL"),
        supabase_key=os.getenv("SUPABASE_KEY"),
    )
