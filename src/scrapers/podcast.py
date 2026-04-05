"""Podcast scraper: Tavily search for transcript pages + OpenAI Whisper for audio."""

import logging
import os
import re
import tempfile
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

import requests

from .base import BaseScraper, TranscriptResult

logger = logging.getLogger(__name__)

# Podcast hosts/sites that commonly publish transcript pages
PODCAST_DOMAINS = [
    "lexfridman.com",
    "tim.blog",
    "andreessen.com",
    "a16z.com",
    "acquired.fm",
    "coatue.com",
    "founderspodcast.com",
    "joincolossus.com",
    "invest.saastr.com",
    "nytimes.com/column/hard-fork",
    "hbr.org",
]

AUDIO_EXTENSIONS = (".mp3", ".m4a", ".wav", ".ogg", ".opus")
MAX_AUDIO_BYTES = 25 * 1024 * 1024  # 25 MB (OpenAI Whisper API limit)


class PodcastScraper(BaseScraper):
    source_name = "podcast"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.tavily_api_key = os.environ.get("TAVILY_API_KEY", "")
        self.openai_api_key = os.environ.get("OPENAI_API_KEY", "")

    def fetch(self, entity: dict[str, Any]) -> list[TranscriptResult]:
        if not self.tavily_api_key:
            logger.warning("TAVILY_API_KEY not set — skipping podcast scraper")
            return []

        results: list[TranscriptResult] = []
        seen_urls: set[str] = set()

        for query in self._build_queries(entity):
            try:
                from tavily import TavilyClient

                client = TavilyClient(api_key=self.tavily_api_key)
                resp = client.search(
                    query=query,
                    search_depth="advanced",
                    max_results=self.max_results,
                    include_raw_content=False,
                )
                for item in resp.get("results", []):
                    url = item.get("url", "")
                    if url in seen_urls:
                        continue
                    seen_urls.add(url)

                    if not self._looks_like_podcast(url, item.get("title", "")):
                        continue

                    published_at = self._parse_date(item.get("published_date", ""))
                    if not self.is_within_window(published_at):
                        continue

                    text = self._extract_podcast_content(url, item.get("content", ""))
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
                logger.warning("Podcast search error for %s: %s", entity["name"], exc)

        return results

    def _build_queries(self, entity: dict[str, Any]) -> list[str]:
        name = entity["name"]
        domains_str = " OR ".join(f"site:{d}" for d in PODCAST_DOMAINS[:5])
        queries = [
            f'"{name}" podcast transcript 2026',
            f'"{name}" podcast episode ({domains_str})',
        ]
        for alias in entity.get("aliases", [])[:2]:
            queries.append(f'"{alias}" podcast interview transcript 2026')
        return queries

    def _looks_like_podcast(self, url: str, title: str) -> bool:
        text = (url + " " + title).lower()
        podcast_hints = ["podcast", "episode", "ep.", "ep ", "transcript", "#", "interview"]
        return any(h in text for h in podcast_hints)

    def _extract_podcast_content(self, url: str, fallback: str) -> str:
        """Try to get transcript text from a podcast page, or transcribe audio."""
        try:
            resp = self._get(url, timeout=15)
            from bs4 import BeautifulSoup

            soup = BeautifulSoup(resp.text, "lxml")

            # Look for audio links on the page and transcribe if found
            audio_url = self._find_audio_link(soup, url)
            if audio_url and self.openai_api_key:
                transcript = self._transcribe_audio(audio_url)
                if transcript:
                    return transcript

            # Otherwise extract transcript text from the page
            for tag in soup(["script", "style", "nav", "header", "footer"]):
                tag.decompose()

            # Podcast sites often put transcript in a specific section
            for sel in [
                ".transcript",
                "#transcript",
                '[class*="transcript"]',
                '[id*="transcript"]',
                "article",
                ".post-content",
                ".entry-content",
                "main",
            ]:
                el = soup.select_one(sel)
                if el:
                    text = el.get_text(separator=" ", strip=True)
                    if len(text) > 400:
                        return text

            return soup.get_text(separator=" ", strip=True)
        except Exception:
            return fallback

    def _find_audio_link(self, soup, base_url: str) -> str | None:
        for tag in soup.find_all(["audio", "source", "a"]):
            src = tag.get("src") or tag.get("href") or ""
            if any(src.lower().endswith(ext) for ext in AUDIO_EXTENSIONS):
                if src.startswith("http"):
                    return src
                return requests.compat.urljoin(base_url, src)
        return None

    def _transcribe_audio(self, audio_url: str) -> str | None:
        """Download audio (up to 25MB) and transcribe via OpenAI Whisper API."""
        if not self.openai_api_key:
            return None
        try:
            resp = requests.get(audio_url, stream=True, timeout=60)
            resp.raise_for_status()

            ext = os.path.splitext(urlparse(audio_url).path)[1] or ".mp3"
            with tempfile.NamedTemporaryFile(suffix=ext, delete=False) as tmp:
                size = 0
                for chunk in resp.iter_content(chunk_size=8192):
                    tmp.write(chunk)
                    size += len(chunk)
                    if size >= MAX_AUDIO_BYTES:
                        logger.info("Audio file too large, truncating at 25MB")
                        break
                tmp_path = tmp.name

            from openai import OpenAI

            client = OpenAI(api_key=self.openai_api_key)
            with open(tmp_path, "rb") as f:
                result = client.audio.transcriptions.create(
                    model="whisper-1", file=f, response_format="text"
                )
            os.unlink(tmp_path)
            return result if isinstance(result, str) else result.text
        except Exception as exc:
            logger.warning("Whisper transcription failed for %s: %s", audio_url, exc)
            return None

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
