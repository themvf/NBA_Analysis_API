from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    source_api_base_url: str
    source_api_timeout_seconds: int
    source_api_page_size: int
    database_url: str
    ingestion_default_lookback_days: int
    ingestion_max_page_size: int

    @staticmethod
    def from_env() -> "Settings":
        load_dotenv()
        database_url = os.getenv("DATABASE_URL", "").strip()
        if not database_url:
            raise ValueError("DATABASE_URL must be set.")

        return Settings(
            source_api_base_url=os.getenv("SOURCE_API_BASE_URL", "http://127.0.0.1:8000").rstrip("/"),
            source_api_timeout_seconds=int(os.getenv("SOURCE_API_TIMEOUT_SECONDS", "20")),
            source_api_page_size=int(os.getenv("SOURCE_API_PAGE_SIZE", "500")),
            database_url=database_url,
            ingestion_default_lookback_days=int(os.getenv("INGESTION_DEFAULT_LOOKBACK_DAYS", "45")),
            ingestion_max_page_size=int(os.getenv("INGESTION_MAX_PAGE_SIZE", "1000")),
        )

