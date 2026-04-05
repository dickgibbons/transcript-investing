"""Base scraper class with shared utilities."""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


@dataclass
class TranscriptResult:
    entity_name: str
    source: str          # seeking_alpha | youtube | news | podcast
    url: str
    title: str
    published_at: str    # ISO-8601 string
    raw_text: str
    metadata: dict[str, Any] = field(default_factory=dict)


class BaseScraper(ABC):
    source_name: str = "unknown"

    def __init__(self, lookback_days: int = 30, max_results: int = 10):
        self.lookback_days = lookback_days
        self.max_results = max_results
        self.since: datetime = datetime.now(timezone.utc) - timedelta(days=lookback_days)

    def is_within_window(self, dt: datetime | None) -> bool:
        if dt is None:
            return True  # If we can't parse the date, include it
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt >= self.since

    @abstractmethod
    def fetch(self, entity: dict[str, Any]) -> list[TranscriptResult]:
        """Fetch transcripts for the given watchlist entity."""

    @staticmethod
    def _clean_text(text: str) -> str:
        """Remove excessive whitespace from scraped text."""
        import re
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r" {2,}", " ", text)
        return text.strip()

    @staticmethod
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def _get(url: str, **kwargs) -> "requests.Response":  # noqa: F821
        import requests
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            )
        }
        kwargs.setdefault("timeout", 30)
        kwargs.setdefault("headers", headers)
        resp = requests.get(url, **kwargs)
        resp.raise_for_status()
        return resp
