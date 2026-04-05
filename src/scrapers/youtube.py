"""YouTube scraper using Data API v3 + youtube-transcript-api."""

import logging
import os
from datetime import datetime, timezone
from typing import Any

from .base import BaseScraper, TranscriptResult

logger = logging.getLogger(__name__)

# Set when API returns 403 quotaExceeded — skip all further YouTube calls this process
_youtube_quota_exhausted = False


class YouTubeScraper(BaseScraper):
    source_name = "youtube"

    def __init__(
        self,
        *,
        enabled: bool = True,
        max_search_queries: int = 8,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.api_key = os.environ.get("YOUTUBE_API_KEY", "")
        self.enabled = enabled
        self.max_search_queries = max_search_queries

    def fetch(self, entity: dict[str, Any]) -> list[TranscriptResult]:
        global _youtube_quota_exhausted
        if not self.enabled:
            return []
        if _youtube_quota_exhausted:
            return []
        if not self.api_key:
            logger.warning("YOUTUBE_API_KEY not set — skipping YouTube scraper")
            return []

        from googleapiclient.discovery import build

        youtube = build("youtube", "v3", developerKey=self.api_key)
        results: list[TranscriptResult] = []

        queries = self._build_queries(entity)[: self.max_search_queries]
        seen_video_ids: set[str] = set()

        from googleapiclient.errors import HttpError

        for query in queries:
            try:
                videos = self._search_videos(youtube, query)
                for video in videos:
                    vid_id = video["id"]["videoId"]
                    if vid_id in seen_video_ids:
                        continue
                    seen_video_ids.add(vid_id)

                    published_at = self._parse_date(video["snippet"]["publishedAt"])
                    if not self.is_within_window(published_at):
                        continue

                    transcript = self._get_transcript(vid_id)
                    if not transcript:
                        continue

                    title = video["snippet"]["title"]
                    url = f"https://www.youtube.com/watch?v={vid_id}"
                    results.append(
                        TranscriptResult(
                            entity_name=entity["name"],
                            source=self.source_name,
                            url=url,
                            title=title,
                            published_at=published_at.isoformat() if published_at else "",
                            raw_text=transcript,
                            metadata={
                                "channel": video["snippet"].get("channelTitle", ""),
                                "description": video["snippet"].get("description", "")[:500],
                            },
                        )
                    )
                    if len(results) >= self.max_results:
                        return results
            except HttpError as exc:
                body = (exc.content or b"").decode("utf-8", errors="replace")
                if exc.resp.status == 403 and (
                    "quotaExceeded" in body or "dailyLimitExceeded" in body
                ):
                    _youtube_quota_exhausted = True
                    logger.error(
                        "YouTube API daily quota exceeded — skipping further YouTube searches "
                        "(reset tomorrow, or request a higher quota in Google Cloud Console)"
                    )
                    return results
                logger.warning("YouTube search error for '%s': %s", query, exc)
            except Exception as exc:
                logger.warning("YouTube search error for '%s': %s", query, exc)

        return results

    def _build_queries(self, entity: dict[str, Any]) -> list[str]:
        name = entity["name"]
        base_terms = ["interview", "keynote", "talk", "speech", "conference", "earnings"]
        queries = [f'"{name}" {term}' for term in base_terms]
        for alias in entity.get("aliases", []):
            queries.append(f'"{alias}" interview OR talk')
        return queries

    def _search_videos(self, youtube, query: str) -> list[dict]:
        published_after = self.since.strftime("%Y-%m-%dT%H:%M:%SZ")
        resp = (
            youtube.search()
            .list(
                q=query,
                type="video",
                part="id,snippet",
                maxResults=min(self.max_results, 10),
                publishedAfter=published_after,
                relevanceLanguage="en",
                videoCaption="closedCaption",  # only videos with captions
            )
            .execute()
        )
        return resp.get("items", [])

    @staticmethod
    def _get_transcript(video_id: str) -> str | None:
        from youtube_transcript_api import (
            NoTranscriptFound,
            TranscriptsDisabled,
            YouTubeTranscriptApi,
        )

        try:
            transcript_list = YouTubeTranscriptApi.list_transcripts(video_id)
            # Prefer manually created English transcript, fall back to auto
            try:
                transcript = transcript_list.find_manually_created_transcript(["en"])
            except NoTranscriptFound:
                try:
                    transcript = transcript_list.find_generated_transcript(["en"])
                except NoTranscriptFound:
                    return None

            entries = transcript.fetch()
            text = " ".join(e["text"] for e in entries)
            return BaseScraper._clean_text(text)
        except (TranscriptsDisabled, Exception) as exc:
            logger.debug("No transcript for video %s: %s", video_id, exc)
            return None

    @staticmethod
    def _parse_date(date_str: str) -> datetime | None:
        try:
            return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            return None
