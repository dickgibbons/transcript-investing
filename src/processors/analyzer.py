"""
Transcript analyzer using Claude Opus 4.6 with adaptive thinking.

For each transcript, Claude extracts:
- Forward-looking signals (with conviction 1-5 and time horizon)
- Macro themes
- Key quotes
"""

import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import anthropic

from .cleaner import chunk_transcript, normalize

logger = logging.getLogger(__name__)

SIGNAL_SCHEMA = {
    "type": "object",
    "properties": {
        "signals": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["topic", "statement", "conviction", "time_horizon", "quote"],
                "properties": {
                    "topic": {
                        "type": "string",
                        "description": "The specific technology, sector, or trend being signalled",
                    },
                    "statement": {
                        "type": "string",
                        "description": "One concise sentence summarising the forward-looking view",
                    },
                    "conviction": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 5,
                        "description": "How strongly the speaker expressed this view (1=vague mention, 5=explicit bet)",
                    },
                    "time_horizon": {
                        "type": "string",
                        "enum": ["near-term (0-12 months)", "mid-term (1-3 years)", "long-term (3+ years)"],
                    },
                    "quote": {
                        "type": "string",
                        "description": "Verbatim or near-verbatim quote from the transcript supporting this signal",
                    },
                    "companies_mentioned": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Specific companies or tickers mentioned in relation to this signal",
                    },
                    "sentiment": {
                        "type": "string",
                        "enum": ["bullish", "bearish", "neutral", "cautious"],
                    },
                },
            },
        },
        "themes": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Top-level macro themes emerging from this transcript (e.g. 'AI infrastructure buildout', 'energy demand surge')",
        },
        "summary": {
            "type": "string",
            "description": "2-3 sentence summary of the speaker's overall outlook on AI, technology, and the economy",
        },
    },
    "required": ["signals", "themes", "summary"],
}

SYSTEM_PROMPT = """You are an expert investment analyst specialising in technology and AI.
Your task is to extract investment-relevant intelligence from public speech transcripts.

Focus exclusively on:
1. Forward-looking statements about AI, technology, the economy, and capital allocation
2. Explicit or implied bets the speaker is making (investments, hiring, partnerships, products)
3. Sectors, companies, or technologies the speaker is bullish or bearish on
4. Macro trends the speaker thinks will define the next 1-5 years

Do NOT include:
- Historical facts or backward-looking statements
- Marketing language without substance
- Personal anecdotes unrelated to business/markets

Be precise with quotes — use the speaker's exact words when possible.
Rate conviction honestly: 5 = "we are betting the company on this", 1 = "mentioned once in passing".

IMPORTANT: Respond ONLY with a valid JSON object. No markdown, no code fences, no explanation.
Use this exact structure:
{
  "signals": [
    {
      "topic": "string",
      "statement": "string",
      "conviction": 1-5,
      "time_horizon": "near-term (0-12 months)" | "mid-term (1-3 years)" | "long-term (3+ years)",
      "quote": "string",
      "companies_mentioned": ["string"],
      "sentiment": "bullish" | "bearish" | "neutral" | "cautious"
    }
  ],
  "themes": ["string"],
  "summary": "string"
}"""


class TranscriptAnalyzer:
    def __init__(self, model: str = "claude-opus-4-6", max_parallel: int = 5):
        self.model = model
        self.max_parallel = max_parallel
        self.client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))

    def analyze(
        self, entity_name: str, title: str, raw_text: str, source: str
    ) -> dict[str, Any]:
        """
        Analyze a single transcript. Returns merged signals/themes/summary dict.
        Long transcripts are chunked and processed in parallel, then merged.
        """
        clean = normalize(raw_text)
        chunks = chunk_transcript(clean)
        logger.info(
            "Analyzing '%s' (%s) — %d chunk(s), %d words",
            title,
            entity_name,
            len(chunks),
            len(clean.split()),
        )

        if len(chunks) == 1:
            return self._analyze_chunk(entity_name, title, chunks[0], source)

        # Parallel chunk analysis
        chunk_results: list[dict] = []
        with ThreadPoolExecutor(max_workers=min(self.max_parallel, len(chunks))) as pool:
            futures = {
                pool.submit(
                    self._analyze_chunk,
                    entity_name,
                    f"{title} (part {i + 1}/{len(chunks)})",
                    chunk,
                    source,
                ): i
                for i, chunk in enumerate(chunks)
            }
            for future in as_completed(futures):
                try:
                    chunk_results.append(future.result())
                except Exception as exc:
                    logger.warning("Chunk analysis failed: %s", exc)

        return self._merge_chunk_results(entity_name, chunk_results)

    def _analyze_chunk(
        self, entity_name: str, title: str, text: str, source: str
    ) -> dict[str, Any]:
        user_prompt = f"""Speaker/Entity: {entity_name}
Source: {source}
Title: {title}

TRANSCRIPT:
{text}

Extract all investment-relevant signals, themes, and a summary following the schema exactly."""

        with self.client.messages.stream(
            model=self.model,
            max_tokens=8192,
            thinking={"type": "adaptive"},
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_prompt}],
        ) as stream:
            message = stream.get_final_message()
        return _parse_json_response(message, {"signals": [], "themes": [], "summary": ""})

    def _merge_chunk_results(
        self, entity_name: str, results: list[dict]
    ) -> dict[str, Any]:
        """Merge chunk-level results and run a consolidation pass with Claude."""
        if not results:
            return {"signals": [], "themes": [], "summary": ""}

        all_signals = []
        all_themes: set[str] = set()
        summaries: list[str] = []

        for r in results:
            all_signals.extend(r.get("signals", []))
            all_themes.update(r.get("themes", []))
            if r.get("summary"):
                summaries.append(r["summary"])

        # Deduplicate signals by topic similarity (keep highest conviction)
        deduplicated = _deduplicate_signals(all_signals)

        # Final consolidation pass
        consolidation_input = json.dumps(
            {"signals": deduplicated, "themes": list(all_themes), "summaries": summaries},
            indent=2,
        )
        user_prompt = f"""Below are investment signals extracted from multiple chunks of a transcript by {entity_name}.
Consolidate them: merge duplicates, keep the best version of each signal, and write one unified summary.

{consolidation_input}

Return the consolidated result in the exact same JSON schema."""

        with self.client.messages.stream(
            model=self.model,
            max_tokens=8192,
            thinking={"type": "adaptive"},
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_prompt}],
        ) as stream:
            message = stream.get_final_message()
        return _parse_json_response(
            message, {"signals": deduplicated, "themes": list(all_themes), "summary": " ".join(summaries)}
        )


def _parse_json_response(message, fallback: dict) -> dict:
    """Extract and parse the JSON text block from a Claude message."""
    import re
    for block in message.content:
        if hasattr(block, "text") and block.text:
            text = block.text.strip()
            # Strip markdown code fences if present
            text = re.sub(r"^```(?:json)?\s*", "", text)
            text = re.sub(r"\s*```$", "", text)
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                # Try to extract JSON object from within the text
                match = re.search(r"\{[\s\S]*\}", text)
                if match:
                    try:
                        return json.loads(match.group())
                    except json.JSONDecodeError:
                        pass
    return fallback


def _deduplicate_signals(signals: list[dict]) -> list[dict]:
    """Keep the highest-conviction signal per unique topic."""
    by_topic: dict[str, dict] = {}
    for sig in signals:
        topic = sig.get("topic", "").lower().strip()
        existing = by_topic.get(topic)
        if not existing or sig.get("conviction", 0) > existing.get("conviction", 0):
            by_topic[topic] = sig
    return sorted(by_topic.values(), key=lambda s: s.get("conviction", 0), reverse=True)
