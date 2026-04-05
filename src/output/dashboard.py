"""
Build a self-contained HTML investment dashboard from analysis results.
All chart data is embedded as JSON inside the HTML — no server required.
"""

import json
import logging
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader

from src.investment.report_model import normalize_investment_report

logger = logging.getLogger(__name__)

TEMPLATE_DIR = Path(__file__).parent / "templates"


def build_dashboard(
    investment_report: list[dict[str, Any]] | dict[str, Any],
    analyses: list[dict[str, Any]],
    transcripts: list[dict[str, Any]],
    output_path: Path,
    run_date: str | None = None,
) -> Path:
    """
    Render the dashboard HTML and write it to `output_path`.
    `investment_report` is a v2 dict from the mapper or a legacy flat list of opportunities.
    Returns the path of the written file.
    """
    run_date = run_date or datetime.utcnow().strftime("%Y-%m-%d")

    normalized = normalize_investment_report(investment_report)
    opportunity_sections = normalized["sections"]
    all_opportunities = normalized["all_opportunities"]

    entity_data = _build_entity_data(analyses, transcripts)
    chart_data = _build_chart_data(all_opportunities, analyses, transcripts)
    heatmap_themes, heatmap_rows = _build_heatmap(analyses)

    cross_cutting = []
    if all_opportunities:
        # Use the cross_cutting_themes from the first opportunity's parent if available
        pass
    # Extract from raw mapper result if stored; otherwise derive from frequency
    theme_counter: Counter = Counter()
    for a in analyses:
        for theme in a.get("themes", []):
            theme_counter[theme] += 1
    cross_cutting = [t for t, c in theme_counter.most_common(8) if c > 1]

    total_signals = sum(len(a.get("signals", [])) for a in analyses)

    env = Environment(loader=FileSystemLoader(str(TEMPLATE_DIR)), autoescape=False)
    template = env.get_template("dashboard.html")
    html = template.render(
        run_date=run_date,
        total_transcripts=len(transcripts),
        entity_count=len({t["entity_name"] for t in transcripts}),
        total_signals=total_signals,
        opportunity_sections=opportunity_sections,
        report_layout=normalized["layout"],
        opportunity_total_count=normalized["total_count"],
        cross_cutting_themes=cross_cutting,
        entities=entity_data,
        heatmap_themes=heatmap_themes,
        heatmap_rows=heatmap_rows,
        chart_data_json=json.dumps(chart_data),
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")
    logger.info("Dashboard written to %s", output_path)
    return output_path


def _build_entity_data(
    analyses: list[dict], transcripts: list[dict]
) -> list[dict[str, Any]]:
    by_entity: dict[str, dict] = defaultdict(lambda: {
        "name": "",
        "signals": [],
        "themes": set(),
        "summaries": [],
        "sources": set(),
        "transcript_count": 0,
    })

    for t in transcripts:
        name = t["entity_name"]
        by_entity[name]["name"] = name
        by_entity[name]["sources"].add(t["source"])
        by_entity[name]["transcript_count"] += 1

    for a in analyses:
        name = a["entity_name"]
        by_entity[name]["name"] = name
        by_entity[name]["signals"].extend(a.get("signals", []))
        by_entity[name]["themes"].update(a.get("themes", []))
        if a.get("summary"):
            by_entity[name]["summaries"].append(a["summary"])

    result = []
    for name, data in sorted(by_entity.items(), key=lambda x: len(x[1]["signals"]), reverse=True):
        signals = sorted(data["signals"], key=lambda s: s.get("conviction", 0), reverse=True)
        avg_conv = 0.0
        if signals:
            avg_conv = round(sum(s.get("conviction", 0) for s in signals) / len(signals), 1)
        result.append({
            "name": name,
            "signal_count": len(signals),
            "avg_conviction": avg_conv,
            "top_signals": signals[:10],
            "sources": sorted(data["sources"]),
            "transcript_count": data["transcript_count"],
            "summary": " ".join(data["summaries"][:2]),
        })
    return result


def _build_chart_data(
    opportunities: list[dict],
    analyses: list[dict],
    transcripts: list[dict],
) -> dict[str, Any]:
    # Conviction chart: top 8 themes by conviction score
    opp_sorted = sorted(opportunities, key=lambda o: o.get("conviction_score", 0), reverse=True)[:8]
    conviction_labels = [o["macro_theme"][:25] for o in opp_sorted]
    conviction_values = [round(o.get("conviction_score", 0), 1) for o in opp_sorted]

    # Time horizon distribution
    horizon_counter: Counter = Counter()
    for o in opportunities:
        h = o.get("time_horizon", "")
        if "near" in h:
            horizon_counter["Near-term"] += 1
        elif "mid" in h:
            horizon_counter["Mid-term"] += 1
        else:
            horizon_counter["Long-term"] += 1

    # Source distribution
    source_counter: Counter = Counter(t["source"] for t in transcripts)
    source_labels = list(source_counter.keys())
    source_values = [source_counter[k] for k in source_labels]

    # Signals per entity
    entity_signal_count: Counter = Counter()
    for a in analyses:
        entity_signal_count[a["entity_name"]] += len(a.get("signals", []))
    top_entities = entity_signal_count.most_common(8)
    entity_labels = [e[0] for e in top_entities]
    entity_values = [e[1] for e in top_entities]

    return {
        "conviction": {"labels": conviction_labels, "values": conviction_values},
        "horizon": {
            "labels": list(horizon_counter.keys()),
            "values": list(horizon_counter.values()),
        },
        "sources": {"labels": source_labels, "values": source_values},
        "entities": {"labels": entity_labels, "values": entity_values},
    }


def _build_heatmap(
    analyses: list[dict],
) -> tuple[list[str], list[dict]]:
    """Build entity × theme conviction heatmap data."""
    # Collect all unique themes (top 12 by frequency)
    theme_counter: Counter = Counter()
    for a in analyses:
        for theme in a.get("themes", []):
            theme_counter[theme] += 1
    top_themes = [t for t, _ in theme_counter.most_common(12)]

    if not top_themes:
        return [], []

    # For each entity, compute avg conviction for each theme
    entity_theme_scores: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    for a in analyses:
        entity = a["entity_name"]
        entity_themes = set(a.get("themes", []))
        for sig in a.get("signals", []):
            topic_lower = sig.get("topic", "").lower()
            for theme in top_themes:
                if theme.lower() in topic_lower or topic_lower in theme.lower():
                    entity_theme_scores[entity][theme].append(sig.get("conviction", 0))

    # Also mark entities that mentioned a theme at all
    for a in analyses:
        entity = a["entity_name"]
        for theme in a.get("themes", []):
            if theme in top_themes and not entity_theme_scores[entity][theme]:
                entity_theme_scores[entity][theme].append(1)

    rows = []
    for entity in sorted(entity_theme_scores.keys()):
        cells = []
        for theme in top_themes:
            scores = entity_theme_scores[entity][theme]
            avg = round(sum(scores) / len(scores)) if scores else 0
            cells.append(avg)
        # Use "cells" not "values" — Jinja treats dict.values as the .values() method
        rows.append({"entity": entity, "cells": cells})

    return top_themes, rows
