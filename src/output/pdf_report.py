"""Generate a formatted PDF investment report using Playwright (Chromium or system Chrome)."""

import logging
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader

logger = logging.getLogger(__name__)

TEMPLATE_DIR = Path(__file__).parent / "templates"


def build_pdf(
    opportunities: list[dict[str, Any]],
    analyses: list[dict[str, Any]],
    transcripts: list[dict[str, Any]],
    output_path: Path,
    run_date: str | None = None,
) -> Path:
    """
    Render and write the PDF report to `output_path`. Returns the path.
    """
    run_date = run_date or datetime.utcnow().strftime("%Y-%m-%d")

    entity_data = _build_entity_table_data(analyses, transcripts)
    theme_counter: Counter = Counter()
    for a in analyses:
        for theme in a.get("themes", []):
            theme_counter[theme] += 1
    cross_cutting = [t for t, c in theme_counter.most_common(8) if c > 1]

    total_signals = sum(len(a.get("signals", [])) for a in analyses)

    env = Environment(loader=FileSystemLoader(str(TEMPLATE_DIR)), autoescape=False)
    template = env.get_template("pdf_report.html")
    html_content = template.render(
        run_date=run_date,
        total_transcripts=len(transcripts),
        entity_count=len({t["entity_name"] for t in transcripts}),
        total_signals=total_signals,
        opportunities=opportunities,
        cross_cutting_themes=cross_cutting,
        entities=entity_data,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Save the HTML to a temp file so Playwright can load it with full CSS support
    html_tmp = output_path.with_suffix(".pdf.html")
    html_tmp.write_text(html_content, encoding="utf-8")

    try:
        from playwright.sync_api import sync_playwright

        file_url = f"file://{html_tmp.resolve()}"
        pdf_opts = {
            "path": str(output_path),
            "format": "A4",
            "margin": {"top": "2cm", "bottom": "2.5cm", "left": "2.2cm", "right": "2.2cm"},
            "print_background": True,
        }

        def try_generate_pdf(launch_browser):
            browser = launch_browser()
            try:
                page = browser.new_page()
                page.goto(file_url, wait_until="networkidle")
                page.pdf(**pdf_opts)
            finally:
                browser.close()

        last_err: Exception | None = None
        with sync_playwright() as p:
            # 1) Bundled Chromium (requires: playwright install chromium)
            try:
                try_generate_pdf(lambda: p.chromium.launch(headless=True))
            except Exception as e1:
                last_err = e1
                # 2) System Google Chrome — no separate browser download
                try:
                    try_generate_pdf(
                        lambda: p.chromium.launch(headless=True, channel="chrome")
                    )
                    last_err = None
                except Exception as e2:
                    last_err = e2
                    # 3) Microsoft Edge on Mac/Windows
                    try:
                        try_generate_pdf(
                            lambda: p.chromium.launch(headless=True, channel="msedge")
                        )
                        last_err = None
                    except Exception as e3:
                        last_err = e3
                        raise last_err from e1

        if last_err is not None:
            raise last_err

        html_tmp.unlink(missing_ok=True)
        logger.info("PDF report written to %s", output_path)
        return output_path

    except Exception as exc:
        logger.warning(
            "PDF generation failed (%s). Install browsers: `playwright install chromium` "
            "or ensure Google Chrome is installed. HTML fallback: %s",
            exc,
            html_tmp,
        )
        # Rename the tmp HTML to the final path so the caller gets something useful
        fallback = output_path.with_suffix(".html")
        html_tmp.rename(fallback)
        return fallback


def _build_entity_table_data(
    analyses: list[dict], transcripts: list[dict]
) -> list[dict[str, Any]]:
    by_entity: dict[str, dict] = defaultdict(lambda: {
        "name": "",
        "signals": [],
        "themes": [],
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
        by_entity[name]["signals"].extend(a.get("signals", []))
        by_entity[name]["themes"].extend(a.get("themes", []))

    result = []
    for name, data in sorted(by_entity.items()):
        signals = data["signals"]
        avg_conv = 0.0
        if signals:
            avg_conv = round(sum(s.get("conviction", 0) for s in signals) / len(signals), 1)
        theme_counter: Counter = Counter(data["themes"])
        result.append({
            "name": name,
            "signal_count": len(signals),
            "avg_conviction": avg_conv,
            "top_themes": [t for t, _ in theme_counter.most_common(4)],
            "sources": sorted(data["sources"]),
            "transcript_count": data["transcript_count"],
        })
    return result
