"""
Main pipeline orchestration.

Run order:
  1. Load watchlist
  2. For each entity: run all scrapers in parallel
  3. Store raw transcripts in SQLite
  4. Analyze each transcript with Claude
  5. Map signals to investment opportunities
  6. Build HTML dashboard + PDF report
  7. Write output to timestamped folder
"""

import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn

from src import db
from src.db import get_transcripts_by_ids
from src.investment.mapper import InvestmentMapper
from src.output.dashboard import build_dashboard
from src.output.pdf_report import build_pdf
from src.processors.analyzer import TranscriptAnalyzer
from src.scrapers.news import NewsScraper
from src.scrapers.podcast import PodcastScraper
from src.scrapers.seeking_alpha import SeekingAlphaScraper
from src.scrapers.youtube import YouTubeScraper

load_dotenv()

logger = logging.getLogger(__name__)
console = Console()

ROOT = Path(__file__).parent.parent
CONFIG_DIR = ROOT / "config"
OUTPUT_DIR = ROOT / "output"


def load_config() -> dict[str, Any]:
    with open(CONFIG_DIR / "settings.yaml") as f:
        return yaml.safe_load(f)


def load_watchlist() -> list[dict[str, Any]]:
    with open(CONFIG_DIR / "watchlist.yaml") as f:
        data = yaml.safe_load(f)
    return data.get("watchlist", [])


def run(dry_run: bool = False, quick: bool = False) -> Path:
    """
    Execute the full pipeline. Returns the output folder path.
    `dry_run=True` skips Claude API calls (useful for testing scrapers).
    """
    config = load_config()
    watchlist = load_watchlist()
    run_date = datetime.utcnow().strftime("%Y-%m-%d")
    output_folder = OUTPUT_DIR / run_date
    output_folder.mkdir(parents=True, exist_ok=True)

    db.init_db()
    run_id = db.create_run()

    console.print(f"\n[bold blue]TranscriptInvest[/] — Run {run_date} (run_id={run_id})")
    console.print(f"Tracking [bold]{len(watchlist)}[/] entities\n")

    try:
        # ── Stage 1: Scrape ──────────────────────────────────────────────────────
        scraping_cfg = config.get("scraping", {})
        max_results = 2 if quick else scraping_cfg.get("max_results_per_source", 10)
        if quick:
            console.print("[yellow]Quick mode — limiting to 2 results per entity per source[/]\n")
        yt_queries = scraping_cfg.get("youtube_max_search_queries_per_entity", 8)
        if quick:
            yt_queries = min(yt_queries, 2)
        scrapers = [
            SeekingAlphaScraper(
                lookback_days=scraping_cfg.get("lookback_days", 30),
                max_results=max_results,
            ),
            YouTubeScraper(
                lookback_days=scraping_cfg.get("lookback_days", 30),
                max_results=max_results,
                enabled=scraping_cfg.get("youtube_enabled", True),
                max_search_queries=yt_queries,
            ),
            NewsScraper(
                lookback_days=scraping_cfg.get("lookback_days", 30),
                max_results=max_results,
            ),
            PodcastScraper(
                lookback_days=scraping_cfg.get("lookback_days", 30),
                max_results=max_results,
            ),
        ]

        all_transcript_ids: list[int] = []

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            scrape_task = progress.add_task("Scraping sources...", total=len(watchlist) * len(scrapers))

            def scrape_entity_source(entity, scraper):
                try:
                    return scraper.fetch(entity), entity["name"], scraper.source_name
                except Exception as exc:
                    logger.warning("Scraper %s failed for %s: %s", scraper.source_name, entity["name"], exc)
                    return [], entity["name"], scraper.source_name

            with ThreadPoolExecutor(max_workers=4) as pool:
                futures = {
                    pool.submit(scrape_entity_source, entity, scraper): (entity, scraper)
                    for entity in watchlist
                    for scraper in scrapers
                }
                for future in as_completed(futures):
                    results, entity_name, source = future.result()
                    progress.advance(scrape_task)
                    for r in results:
                        tid = db.upsert_transcript(
                            run_id=run_id,
                            entity_name=r.entity_name,
                            source=r.source,
                            url=r.url,
                            title=r.title,
                            published_at=r.published_at,
                            raw_text=r.raw_text,
                        )
                        if tid:
                            all_transcript_ids.append(tid)
                            console.print(
                                f"  [dim]{source:16}[/] {entity_name:20} {r.title[:60]}"
                            )

        # Use IDs collected this run (includes deduped URLs already in DB from prior runs)
        transcripts = get_transcripts_by_ids(list(set(all_transcript_ids)))
        console.print(f"\n[green]✓[/] Collected [bold]{len(transcripts)}[/] transcripts\n")

        if dry_run:
            console.print("[yellow]Dry run — skipping analysis. Exiting.[/]")
            db.finish_run(run_id, "success")
            return output_folder

        if not transcripts:
            console.print("[yellow]No transcripts found — check your API keys and watchlist.[/]")
            db.finish_run(run_id, "success")
            return output_folder

        # ── Stage 2: Analyze ─────────────────────────────────────────────────────
        analysis_cfg = config.get("analysis", {})
        analyzer = TranscriptAnalyzer(
            model=analysis_cfg.get("model", "claude-opus-4-6"),
            max_parallel=analysis_cfg.get("max_parallel_chunks", 5),
        )

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            analyze_task = progress.add_task("Analyzing transcripts with Claude...", total=len(transcripts))
            for t in transcripts:
                try:
                    result = analyzer.analyze(
                        entity_name=t["entity_name"],
                        title=t["title"] or t["url"],
                        raw_text=t["raw_text"],
                        source=t["source"],
                    )
                    db.save_analysis(
                        run_id=run_id,
                        transcript_id=t["id"],
                        entity_name=t["entity_name"],
                        signals=result.get("signals", []),
                        themes=result.get("themes", []),
                    )
                    progress.advance(analyze_task)
                    console.print(
                        f"  [dim]analyzed[/] {t['entity_name']:20} "
                        f"[{len(result.get('signals', []))} signals]"
                    )
                except Exception as exc:
                    logger.warning("Analysis failed for transcript %s: %s", t["id"], exc)
                    progress.advance(analyze_task)

        analyses = db.get_analyses_for_run(run_id)
        total_signals = sum(len(a["signals"]) for a in analyses)
        console.print(f"\n[green]✓[/] Extracted [bold]{total_signals}[/] signals\n")

        # ── Stage 3: Map to investments ──────────────────────────────────────────
        investment_cfg = config.get("investment", {})
        mapper = InvestmentMapper(
            model=investment_cfg.get("model", "claude-opus-4-6"),
            top_n=investment_cfg.get("top_opportunities", 10),
        )

        console.print("[bold]Mapping signals to investment opportunities...[/]")
        opportunities = mapper.map(analyses)
        console.print(f"[green]✓[/] Generated [bold]{len(opportunities)}[/] investment opportunities\n")

        # ── Stage 4: Output ──────────────────────────────────────────────────────
        html_path = output_folder / "index.html"
        pdf_path = output_folder / f"report_{run_date}.pdf"

        console.print("[bold]Building dashboard and PDF...[/]")
        build_dashboard(opportunities, analyses, transcripts, html_path, run_date)
        build_pdf(opportunities, analyses, transcripts, pdf_path, run_date)

        # Symlink output/latest → this run
        latest_link = OUTPUT_DIR / "latest"
        if latest_link.is_symlink():
            latest_link.unlink()
        latest_link.symlink_to(output_folder.resolve())

        db.save_investment_report(
            run_id=run_id,
            opportunities=opportunities,
            html_path=str(html_path),
            pdf_path=str(pdf_path),
        )
        db.finish_run(run_id, "success")

        # Prune old runs
        keep = config.get("output", {}).get("keep_runs", 10)
        db.prune_old_runs(keep)

        console.print(f"\n[bold green]✓ Run complete![/]")
        console.print(f"  Dashboard: [link]{html_path}[/link]")
        console.print(f"  PDF:       [link]{pdf_path}[/link]")
        console.print(f"  Latest:    [link]{latest_link}[/link]\n")

        return output_folder

    except Exception as exc:
        db.finish_run(run_id, "error", str(exc))
        console.print(f"[bold red]Pipeline failed:[/] {exc}")
        raise
