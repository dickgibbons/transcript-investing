"""
Investment opportunity mapper using Claude Opus 4.6 with structured output.

Takes aggregated signals from all analyzed transcripts and produces a ranked
list of investment opportunities spanning:
  - Macro themes
  - Sectors
  - ETFs (sector plays)
  - Individual stocks
  - Private/venture themes
  - Crypto plays
"""

import json
import logging
from typing import Any

import anthropic

from ..secrets import anthropic_api_key
from .market_data import enrich_tickers
from .report_model import build_report_v2

logger = logging.getLogger(__name__)

OPPORTUNITY_SCHEMA = {
    "type": "object",
    "properties": {
        "opportunities": {
            "type": "array",
            "items": {
                "type": "object",
                "required": [
                    "rank",
                    "macro_theme",
                    "sector",
                    "thesis",
                    "conviction_score",
                    "time_horizon",
                    "etfs",
                    "stocks",
                    "supporting_signals",
                ],
                "properties": {
                    "rank": {"type": "integer", "minimum": 1},
                    "macro_theme": {
                        "type": "string",
                        "description": "High-level investment theme, e.g. 'AI Infrastructure Buildout'",
                    },
                    "sector": {
                        "type": "string",
                        "description": "Specific sector or sub-sector, e.g. 'Data Centers & Networking'",
                    },
                    "thesis": {
                        "type": "string",
                        "description": "2-4 sentence investment thesis grounded in what the speakers said",
                    },
                    "conviction_score": {
                        "type": "number",
                        "minimum": 1,
                        "maximum": 10,
                        "description": "Composite conviction score based on frequency and strength of signals",
                    },
                    "time_horizon": {
                        "type": "string",
                        "enum": ["near-term (0-12 months)", "mid-term (1-3 years)", "long-term (3+ years)"],
                    },
                    "etfs": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "required": ["ticker", "rationale"],
                            "properties": {
                                "ticker": {"type": "string"},
                                "rationale": {"type": "string"},
                            },
                        },
                        "description": "ETFs providing exposure to this theme",
                    },
                    "stocks": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "required": ["ticker", "company_name", "rationale"],
                            "properties": {
                                "ticker": {"type": "string"},
                                "company_name": {"type": "string"},
                                "rationale": {"type": "string"},
                            },
                        },
                        "description": "Individual stocks that are best positioned for this theme",
                    },
                    "private_plays": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Private companies or venture themes relevant to this opportunity",
                    },
                    "crypto_plays": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Crypto assets or protocols relevant to this opportunity",
                    },
                    "risks": {
                        "type": "string",
                        "description": "Key risks or counterarguments to this thesis",
                    },
                    "supporting_signals": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "required": ["speaker", "quote"],
                            "properties": {
                                "speaker": {"type": "string"},
                                "quote": {"type": "string"},
                                "source": {"type": "string"},
                            },
                        },
                        "description": "Direct quotes from transcripts that support this thesis",
                    },
                },
            },
        },
        "cross_cutting_themes": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Themes that appeared across multiple speakers/entities",
        },
        "contrarian_signals": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Any signals that go against consensus market thinking",
        },
    },
    "required": ["opportunities", "cross_cutting_themes"],
}

SYSTEM_PROMPT = """You are a top-tier investment analyst combining insights from multiple industry leaders.
Your job is to synthesise transcript signals into actionable investment opportunities.

Rules:
1. Every opportunity must be grounded in actual quotes/signals from the transcripts — no generic recommendations
2. Rank by composite conviction (frequency × strength of signals)
3. For ETFs and stocks: choose the most direct, liquid plays on each theme
4. For crypto/private: only include if speakers mentioned them explicitly or the theme strongly implies them
5. Be specific: "NVDA" is better than "AI chip companies", "$SMH" is better than "semiconductor ETFs"
6. Risks section must be honest and substantive, not boilerplate
7. Avoid redundancy: merge similar themes into one opportunity

IMPORTANT: Respond ONLY with a valid JSON object. No markdown, no code fences, no explanation.
Use this exact structure:
{
  "opportunities": [
    {
      "rank": 1,
      "macro_theme": "string",
      "sector": "string",
      "thesis": "string",
      "conviction_score": 7.5,
      "time_horizon": "near-term (0-12 months)" | "mid-term (1-3 years)" | "long-term (3+ years)",
      "etfs": [{"ticker": "string", "rationale": "string"}],
      "stocks": [{"ticker": "string", "company_name": "string", "rationale": "string"}],
      "private_plays": ["string"],
      "crypto_plays": ["string"],
      "risks": "string",
      "supporting_signals": [{"speaker": "string", "quote": "string", "source": "string"}]
    }
  ],
  "cross_cutting_themes": ["string"],
  "contrarian_signals": ["string"]
}"""


class InvestmentMapper:
    def __init__(
        self,
        model: str = "claude-opus-4-6",
        top_n: int = 10,
        group_by_entity: bool = False,
    ):
        self.model = model
        self.top_n = top_n
        self.group_by_entity = group_by_entity
        self.client = anthropic.Anthropic(api_key=anthropic_api_key())

    def map(self, analyses: list[dict[str, Any]]) -> dict[str, Any]:
        """
        Return a versioned report dict (see report_model) with sections of opportunities.
        Legacy callers expected a flat list — use normalize_investment_report() for display.
        """
        if not analyses:
            logger.warning("No analyses to map — returning empty opportunities list")
            return build_report_v2("combined", [])

        if self.group_by_entity:
            return self._map_by_entity(analyses)
        return self._map_combined(analyses)

    def _map_combined(self, analyses: list[dict[str, Any]]) -> dict[str, Any]:
        aggregated = self._aggregate_analyses(analyses)
        logger.info(
            "Mapping %d total signals from %d entities to investment opportunities (combined)",
            aggregated["total_signals"],
            aggregated["entity_count"],
        )
        opportunities = self._generate_opportunities(aggregated, focus_entity_name=None)
        opportunities = self._assign_ranks(opportunities[: self.top_n])
        enriched = self._enrich_with_market_data(opportunities)
        return build_report_v2(
            "combined",
            [{"entity_name": None, "opportunities": enriched}],
        )

    def _map_by_entity(self, analyses: list[dict[str, Any]]) -> dict[str, Any]:
        by_entity: dict[str, list[dict]] = {}
        for a in analyses:
            entity = a.get("entity_name", "Unknown")
            by_entity.setdefault(entity, []).append(a)

        sections: list[dict[str, Any]] = []
        for entity in sorted(by_entity.keys()):
            entity_analyses = by_entity[entity]
            block = self._entity_summary_block(entity, entity_analyses)
            if block["signal_count"] == 0:
                continue
            aggregated = {
                "entity_count": 1,
                "total_signals": block["signal_count"],
                "entity_summaries": [block],
                "top_n_requested": self.top_n,
            }
            logger.info(
                "Mapping %d signals for entity %r only (per-entity report)",
                block["signal_count"],
                entity,
            )
            opportunities = self._generate_opportunities(
                aggregated, focus_entity_name=entity
            )
            opportunities = self._assign_ranks(opportunities[: self.top_n])
            if not opportunities:
                continue
            enriched = self._enrich_with_market_data(opportunities)
            sections.append({"entity_name": entity, "opportunities": enriched})

        return build_report_v2("by_entity", sections)

    @staticmethod
    def _assign_ranks(opportunities: list[dict[str, Any]]) -> list[dict[str, Any]]:
        for i, o in enumerate(opportunities, start=1):
            o["rank"] = i
        return opportunities

    def _entity_summary_block(self, entity: str, entity_analyses: list[dict]) -> dict[str, Any]:
        signals: list[dict] = []
        themes: set[str] = set()
        summaries: list[str] = []
        for a in entity_analyses:
            signals.extend(a.get("signals", []))
            themes.update(a.get("themes", []))
            if a.get("summary"):
                summaries.append(a["summary"])
        return {
            "entity": entity,
            "signal_count": len(signals),
            "top_signals": sorted(
                signals, key=lambda s: s.get("conviction", 0), reverse=True
            )[:20],
            "themes": list(themes),
            "overall_summary": " ".join(summaries[:3]),
        }

    def _aggregate_analyses(self, analyses: list[dict]) -> dict[str, Any]:
        """Group analyses by entity and build a consolidated input for Claude."""
        by_entity: dict[str, list[dict]] = {}
        for a in analyses:
            entity = a.get("entity_name", "Unknown")
            by_entity.setdefault(entity, []).append(a)

        entity_summaries = []
        total_signals = 0

        for entity, entity_analyses in by_entity.items():
            block = self._entity_summary_block(entity, entity_analyses)
            entity_summaries.append(block)
            total_signals += block["signal_count"]

        return {
            "entity_count": len(by_entity),
            "total_signals": total_signals,
            "entity_summaries": entity_summaries,
            "top_n_requested": self.top_n,
        }

    def _generate_opportunities(
        self, aggregated: dict, *, focus_entity_name: str | None
    ) -> list[dict]:
        if focus_entity_name:
            focus_clause = f"""IMPORTANT — SINGLE WATCHLIST ENTITY:
The JSON below contains signals from ONLY: "{focus_entity_name}".
Every opportunity must be justified solely from this source's public remarks.
In supporting_signals, set speaker to "{focus_entity_name}" when quoting their views.
Do not attribute investment themes to other people or companies."""
            intro = (
                f'Below are investment signals from public remarks by "{focus_entity_name}" only '
                f"(last ~30 days, aggregated across transcripts)."
            )
        else:
            focus_clause = ""
            intro = (
                f"Below are investment signals extracted from {aggregated['entity_count']} "
                "industry leaders' public speeches in the last 30 days."
            )

        user_prompt = f"""{focus_clause}
{intro}

{json.dumps(aggregated['entity_summaries'], indent=2)}

Generate the top {aggregated['top_n_requested']} investment opportunities ranked by conviction.
Each opportunity must be backed by specific quotes from the signals above.
Be as specific as possible with ETF and stock tickers."""

        with self.client.messages.stream(
            model=self.model,
            max_tokens=16000,
            thinking={"type": "adaptive"},
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_prompt}],
        ) as stream:
            message = stream.get_final_message()
        import re
        for block in message.content:
            if hasattr(block, "text") and block.text:
                text = block.text.strip()
                text = re.sub(r"^```(?:json)?\s*", "", text)
                text = re.sub(r"\s*```$", "", text)
                try:
                    parsed = json.loads(text)
                    return parsed.get("opportunities", [])
                except json.JSONDecodeError:
                    match = re.search(r"\{[\s\S]*\}", text)
                    if match:
                        try:
                            parsed = json.loads(match.group())
                            return parsed.get("opportunities", [])
                        except json.JSONDecodeError:
                            pass
        return []

    def _enrich_with_market_data(self, opportunities: list[dict]) -> list[dict]:
        """Fetch market data for all tickers mentioned across opportunities."""
        all_tickers: set[str] = set()
        for opp in opportunities:
            for etf in opp.get("etfs", []):
                all_tickers.add(etf["ticker"].upper())
            for stock in opp.get("stocks", []):
                all_tickers.add(stock["ticker"].upper())

        if not all_tickers:
            return opportunities

        logger.info("Enriching %d tickers with market data", len(all_tickers))
        market_data = enrich_tickers(list(all_tickers))

        for opp in opportunities:
            for etf in opp.get("etfs", []):
                ticker = etf["ticker"].upper()
                etf["market_data"] = market_data.get(ticker, {})
            for stock in opp.get("stocks", []):
                ticker = stock["ticker"].upper()
                stock["market_data"] = market_data.get(ticker, {})

        return opportunities
