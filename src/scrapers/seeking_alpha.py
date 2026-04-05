"""Seeking Alpha scraper using Playwright for earnings call transcripts."""

import logging
import os
import re
from datetime import datetime, timezone
from typing import Any

from .base import BaseScraper, TranscriptResult

logger = logging.getLogger(__name__)

SEEKING_ALPHA_BASE = "https://seekingalpha.com"


class SeekingAlphaScraper(BaseScraper):
    source_name = "seeking_alpha"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.email = os.environ.get("SEEKING_ALPHA_EMAIL", "")
        self.password = os.environ.get("SEEKING_ALPHA_PASSWORD", "")

    def fetch(self, entity: dict[str, Any]) -> list[TranscriptResult]:
        slug = entity.get("seeking_alpha_slug") or entity.get("ticker")
        if not slug:
            logger.info(
                "Skipping Seeking Alpha for %s — no ticker/slug configured",
                entity["name"],
            )
            return []

        from playwright.sync_api import sync_playwright

        results: list[TranscriptResult] = []
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                )
            )
            page = context.new_page()
            try:
                self._login(page)
                transcript_links = self._list_transcripts(page, slug)
                for link in transcript_links[: self.max_results]:
                    result = self._fetch_transcript(page, entity["name"], link)
                    if result:
                        results.append(result)
            except Exception as exc:
                logger.error("Seeking Alpha error for %s: %s", entity["name"], exc)
            finally:
                browser.close()

        return results

    def _login(self, page) -> None:
        if not self.email or not self.password:
            logger.warning("Seeking Alpha credentials not set — skipping login")
            return
        page.goto(f"{SEEKING_ALPHA_BASE}/login", timeout=30000)
        page.wait_for_load_state("networkidle")
        # Fill sign-in form
        email_input = page.locator('input[name="email"], input[type="email"]').first
        email_input.fill(self.email)
        pw_input = page.locator('input[name="password"], input[type="password"]').first
        pw_input.fill(self.password)
        page.locator('button[type="submit"], button:has-text("Sign In")').first.click()
        page.wait_for_load_state("networkidle")
        logger.info("Logged in to Seeking Alpha")

    def _list_transcripts(self, page, slug: str) -> list[str]:
        """Return a list of transcript page URLs for the given ticker."""
        url = f"{SEEKING_ALPHA_BASE}/symbol/{slug}/earnings/transcripts"
        page.goto(url, timeout=30000)
        page.wait_for_load_state("networkidle")

        links: list[str] = []
        anchors = page.locator("a[href*='/article/']").all()
        for a in anchors:
            href = a.get_attribute("href") or ""
            if "transcript" in href.lower() or "earnings" in href.lower():
                full_url = href if href.startswith("http") else SEEKING_ALPHA_BASE + href
                if full_url not in links:
                    links.append(full_url)
            if len(links) >= self.max_results:
                break

        # Also check the broader transcripts page
        if not links:
            anchors = page.locator("a[href*='transcript']").all()
            for a in anchors:
                href = a.get_attribute("href") or ""
                full_url = href if href.startswith("http") else SEEKING_ALPHA_BASE + href
                if full_url not in links:
                    links.append(full_url)
                if len(links) >= self.max_results:
                    break

        return links

    def _fetch_transcript(
        self, page, entity_name: str, url: str
    ) -> TranscriptResult | None:
        try:
            page.goto(url, timeout=30000)
            page.wait_for_load_state("networkidle")

            # Extract published date from meta or page content
            published_at = self._extract_date(page)
            if not self.is_within_window(published_at):
                return None

            title = page.title() or url
            # Main article body
            body_sel = page.locator("article, [data-test-id='article-content'], .sa-art-article-body")
            text = body_sel.first.inner_text() if body_sel.count() > 0 else page.inner_text("body")
            text = self._clean_text(text)

            if len(text) < 500:
                return None

            return TranscriptResult(
                entity_name=entity_name,
                source=self.source_name,
                url=url,
                title=title,
                published_at=published_at.isoformat() if published_at else "",
                raw_text=text,
            )
        except Exception as exc:
            logger.warning("Failed to fetch SA transcript %s: %s", url, exc)
            return None

    @staticmethod
    def _extract_date(page) -> datetime | None:
        # Try meta tags first
        for sel in [
            'meta[property="article:published_time"]',
            'meta[name="publishdate"]',
            'time[datetime]',
        ]:
            el = page.locator(sel).first
            if el.count() > 0:
                val = el.get_attribute("content") or el.get_attribute("datetime") or ""
                try:
                    dt = datetime.fromisoformat(val.replace("Z", "+00:00"))
                    return dt.replace(tzinfo=timezone.utc) if not dt.tzinfo else dt
                except ValueError:
                    continue

        # Fall back to text parsing in the page
        text = page.inner_text("body")
        match = re.search(
            r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\.?\s+\d{1,2},?\s+20\d{2}",
            text,
        )
        if match:
            try:
                return datetime.strptime(match.group(), "%b %d, %Y").replace(tzinfo=timezone.utc)
            except ValueError:
                pass
        return None
