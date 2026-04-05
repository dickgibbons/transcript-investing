"""News & conference scraper using Tavily search + BeautifulSoup."""

import logging
import os
import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from .base import BaseScraper, TranscriptResult

logger = logging.getLogger(__name__)

# High-quality sources for interviews, speeches, and conference content
NEWS_SOURCES = [
    "cnbc.com",
    "bloomberg.com",
    "wsj.com",
    "ft.com",
    "reuters.com",
    "fortune.com",
    "techcrunch.com",
    "wired.com",
    "theverge.com",
    "davos.com",
    "weforum.org",
    "ted.com",
    "axios.com",
    "businessinsider.com",
]

TRANSCRIPT_KEYWORDS = [
    "transcript",
    "interview",
    "speech",
    "keynote",
    "remarks",
    "conversation",
    "talk",
    "fireside chat",
]


class NewsScraper(BaseScraper):
    source_name = "news"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.tavily_api_key = os.environ.get("TAVILY_API_KEY", "")

    def fetch(self, entity: dict[str, Any]) -> list[TranscriptResult]:
        if not self.tavily_api_key:
            logger.warning("TAVILY_API_KEY not set — skipping news scraper")
            return []

        results: list[TranscriptResult] = []
        seen_urls: set[str] = set()

        for query in self._build_queries(entity):
            try:
                search_results = self._tavily_search(query)
                for item in search_results:
                    url = item.get("url", "")
                    if url in seen_urls:
                        continue
                    seen_urls.add(url)

                    published_at = self._parse_date(item.get("published_date", ""))
                    if not self.is_within_window(published_at):
                        continue

                    text = self._extract_article_text(url, item.get("content", ""))
                    if len(text) < 400:
                        continue

                    results.append(
                        TranscriptResult(
                            entity_name=entity["name"],
                            source=self.source_name,
                            url=url,
                            title=item.get("title", url),
                            published_at=published_at.isoformat() if published_at else "",
                            raw_text=self._clean_text(text),
                            metadata={"domain": urlparse(url).netloc},
                        )
                    )
                    if len(results) >= self.max_results:
                        return results
            except Exception as exc:
                logger.warning("News search error for %s: %s", entity["name"], exc)

        return results

    def _build_queries(self, entity: dict[str, Any]) -> list[str]:
        name = entity["name"]
        sources_str = " OR ".join(f"site:{s}" for s in NEWS_SOURCES[:6])
        queries = [
            f'"{name}" transcript interview speech ({sources_str})',
            f'"{name}" keynote conference remarks 2026',
        ]
        for alias in entity.get("aliases", [])[:2]:
            queries.append(f'"{alias}" interview OR transcript 2026')
        return queries

    def _tavily_search(self, query: str) -> list[dict]:
        from tavily import TavilyClient

        client = TavilyClient(api_key=self.tavily_api_key)
        resp = client.search(
            query=query,
            search_depth="advanced",
            max_results=self.max_results,
            include_raw_content=False,
        )
        return resp.get("results", [])

    def _extract_article_text(self, url: str, fallback_content: str = "") -> str:
        """Fetch full article text with BeautifulSoup, fall back to Tavily snippet."""
        try:
            resp = self._get(url, timeout=15)
            soup = BeautifulSoup(resp.text, "lxml")

            # Remove boilerplate elements
            for tag in soup(["script", "style", "nav", "header", "footer", "aside", "form"]):
                tag.decompose()

            # Try common article content selectors
            for sel in [
                "article",
                '[role="main"]',
                ".article-body",
                ".post-content",
                ".entry-content",
                "#content",
                "main",
            ]:
                el = soup.select_one(sel)
                if el:
                    return el.get_text(separator=" ", strip=True)

            return soup.get_text(separator=" ", strip=True)
        except Exception:
            return fallback_content

    @staticmethod
    def _parse_date(date_str: str) -> datetime | None:
        if not date_str:
            return None
        for fmt in [
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%d",
            "%B %d, %Y",
            "%b %d, %Y",
        ]:
            try:
                dt = datetime.strptime(date_str[:25], fmt)
                if not dt.tzinfo:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except ValueError:
                continue
        return None
