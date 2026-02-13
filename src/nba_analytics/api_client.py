from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Iterable

import requests


class ApiClientError(RuntimeError):
    """Raised for API request failures."""


@dataclass
class NbaDailyApiClient:
    base_url: str
    timeout_seconds: int = 20
    max_retries: int = 3
    retry_backoff_seconds: float = 0.75

    def __post_init__(self) -> None:
        self.session = requests.Session()

    def close(self) -> None:
        self.session.close()

    def _url(self, path: str) -> str:
        return f"{self.base_url}/{path.lstrip('/')}"

    def get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        last_error: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.get(self._url(path), params=params, timeout=self.timeout_seconds)
                if response.status_code >= 400:
                    detail = response.text[:500]
                    raise ApiClientError(f"GET {path} failed ({response.status_code}): {detail}")
                return response.json()
            except (requests.RequestException, ValueError, ApiClientError) as exc:
                last_error = exc
                if attempt == self.max_retries:
                    break
                time.sleep(self.retry_backoff_seconds * attempt)

        raise ApiClientError(f"GET {path} failed after retries: {last_error}")

    @staticmethod
    def records(payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, dict) and isinstance(payload.get("data"), list):
            return [x for x in payload["data"] if isinstance(x, dict)]
        if isinstance(payload, list):
            return [x for x in payload if isinstance(x, dict)]
        if isinstance(payload, dict):
            return [payload]
        return []

    @staticmethod
    def date_values(payload: Any) -> list[str]:
        if isinstance(payload, dict) and isinstance(payload.get("data"), list):
            return [str(x) for x in payload["data"]]
        if isinstance(payload, list):
            return [str(x) for x in payload]
        return []

    def paginated_records(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        page_size: int = 500,
        max_pages: int = 500,
    ) -> list[dict[str, Any]]:
        all_rows: list[dict[str, Any]] = []
        params = dict(params or {})
        offset = int(params.pop("offset", 0))
        limit = int(params.pop("limit", page_size))

        for _ in range(max_pages):
            page_params = {**params, "offset": offset, "limit": limit}
            payload = self.get(path, page_params)
            rows = self.records(payload)
            if not rows:
                break
            all_rows.extend(rows)

            if isinstance(payload, dict) and isinstance(payload.get("count"), int):
                if offset + len(rows) >= payload["count"]:
                    break
            if len(rows) < limit:
                break
            offset += len(rows)

        return all_rows

    def fetch_prediction_dates(self) -> list[str]:
        return self.date_values(self.get("/dates/predictions"))

    def fetch_dfs_dates(self) -> list[str]:
        return self.date_values(self.get("/dates/dfs-slates"))

    def fetch_backtest_dates(self) -> list[str]:
        return self.date_values(self.get("/dates/backtests"))

    @staticmethod
    def sorted_dates(values: Iterable[str]) -> list[str]:
        return sorted(set(v for v in values if v))

