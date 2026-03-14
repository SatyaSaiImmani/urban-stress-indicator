from __future__ import annotations

import time
from typing import Any, Optional

import httpx


class Open311Client:
    def __init__(
        self,
        base_url: str,
        api_key: Optional[str] = None,
        timeout: float = 30.0,
        max_retries: int = 3,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout
        self.max_retries = max_retries

    def _headers(self) -> dict[str, str]:
        headers = {"Accept": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    def get(self, path: str, params: dict[str, Any]) -> tuple[int, Any, dict[str, str]]:
        url = f"{self.base_url}{path}"

        for attempt in range(1, self.max_retries + 1):
            try:
                with httpx.Client(timeout=self.timeout) as client:
                    response = client.get(url, params=params, headers=self._headers())

                if response.status_code == 429:
                    retry_after = response.headers.get("Retry-After", "30")
                    sleep_seconds = int(retry_after) if retry_after.isdigit() else 30
                    if attempt == self.max_retries:
                        response.raise_for_status()
                    time.sleep(sleep_seconds)
                    continue

                response.raise_for_status()
                return response.status_code, response.json(), dict(response.headers)

            except httpx.HTTPError:
                if attempt == self.max_retries:
                    raise
                time.sleep(2 ** attempt)

        raise RuntimeError("Open311 request failed after retries")
