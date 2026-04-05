#!/usr/bin/env python3
"""
APScheduler-based cron runner for the TranscriptInvest pipeline.

Runs the full pipeline on the configured schedule (default: every Sunday at 8 PM).
Designed to be launched as a long-running process on a VPS.

Usage:
  python scheduler.py               # Start the scheduler (blocks forever)
  python scheduler.py --now         # Run immediately once, then follow schedule
"""

import logging
import sys
from pathlib import Path

import click
import yaml
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv
from rich.console import Console

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env", override=True)
console = Console()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(ROOT / "scheduler.log"),
    ],
)
logger = logging.getLogger("scheduler")


def _run_pipeline():
    logger.info("Scheduled pipeline run starting")
    try:
        from src.pipeline import run
        output = run()
        logger.info("Scheduled run complete → %s", output)
    except Exception as exc:
        logger.error("Scheduled run failed: %s", exc, exc_info=True)


def _load_schedule() -> dict:
    cfg_path = ROOT / "config" / "settings.yaml"
    with open(cfg_path) as f:
        cfg = yaml.safe_load(f)
    return cfg.get("scheduler", {})


@click.command()
@click.option("--now", is_flag=True, help="Run once immediately before following the schedule")
def main(now: bool):
    """Start the TranscriptInvest scheduler."""
    sched_cfg = _load_schedule()

    if not sched_cfg.get("enabled", True):
        console.print("[yellow]Scheduler is disabled in settings.yaml. Set scheduler.enabled: true to enable.[/]")
        sys.exit(0)

    day_of_week = sched_cfg.get("day_of_week", "sun")
    hour = sched_cfg.get("hour", 20)
    minute = sched_cfg.get("minute", 0)
    timezone = sched_cfg.get("timezone", "America/New_York")

    trigger = CronTrigger(
        day_of_week=day_of_week,
        hour=hour,
        minute=minute,
        timezone=timezone,
    )

    scheduler = BlockingScheduler(timezone=timezone)
    scheduler.add_job(_run_pipeline, trigger=trigger, id="pipeline", name="TranscriptInvest Pipeline")

    schedule_desc = f"every {day_of_week.upper()} at {hour:02d}:{minute:02d} {timezone}"
    console.print(f"\n[bold blue]TranscriptInvest Scheduler[/]")
    console.print(f"Schedule: [bold]{schedule_desc}[/]")
    console.print("Press Ctrl+C to stop.\n")

    if now:
        console.print("[bold]Running pipeline immediately (--now flag)...[/]\n")
        _run_pipeline()

    try:
        scheduler.start()
    except KeyboardInterrupt:
        console.print("\n[yellow]Scheduler stopped.[/]")


if __name__ == "__main__":
    main()
