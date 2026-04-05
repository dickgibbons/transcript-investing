"""Normalize stored investment report JSON (v1 list vs v2 structured sections)."""

from __future__ import annotations

from typing import Any

REPORT_VERSION = 2


def build_report_v2(layout: str, sections: list[dict[str, Any]]) -> dict[str, Any]:
    """Wrap opportunities for storage and rendering. Each section: {entity_name, opportunities}."""
    return {
        "report_version": REPORT_VERSION,
        "layout": layout,
        "sections": sections,
    }


def normalize_investment_report(payload: Any) -> dict[str, Any]:
    """
    Returns:
      layout: \"combined\" | \"by_entity\"
      sections: list of {\"entity_name\": str | None, \"opportunities\": [...]}
      all_opportunities: flat list (for charts)
      total_count: int
    """
    if isinstance(payload, list):
        sections = [{"entity_name": None, "opportunities": payload}]
        return {
            "layout": "combined",
            "sections": sections,
            "all_opportunities": list(payload),
            "total_count": len(payload),
        }

    if isinstance(payload, dict) and payload.get("report_version") == REPORT_VERSION:
        sections = payload.get("sections") or []
        flat: list[dict[str, Any]] = []
        for s in sections:
            flat.extend(s.get("opportunities") or [])
        return {
            "layout": payload.get("layout") or "combined",
            "sections": sections,
            "all_opportunities": flat,
            "total_count": len(flat),
        }

    # Unknown dict — treat as empty
    return {
        "layout": "combined",
        "sections": [],
        "all_opportunities": [],
        "total_count": 0,
    }


def total_opportunity_count(payload: Any) -> int:
    return normalize_investment_report(payload)["total_count"]
