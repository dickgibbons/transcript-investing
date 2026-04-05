#!/usr/bin/env python3
"""
TranscriptInvest CLI

Usage:
  python main.py run                   # Run the full pipeline
  python main.py run --dry-run         # Scrape only, skip Claude calls
  python main.py watchlist             # Print the current watchlist
  python main.py watchlist add         # Add an entity interactively
  python main.py history               # Show recent run history
  python main.py open                  # Open the latest dashboard in browser
"""

import os
import subprocess
import sys
import webbrowser
from pathlib import Path

import click
import yaml
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table

ROOT = Path(__file__).resolve().parent
# Always load project .env (not cwd). override=True so a stale empty env var can't win.
load_dotenv(ROOT / ".env", override=True)

console = Console()


@click.group()
def cli():
    """TranscriptInvest — AI investment intelligence from public speech."""


@cli.command()
@click.option("--dry-run", is_flag=True, help="Scrape only, skip Claude analysis")
@click.option("--quick", is_flag=True, help="Limit to 2 results per source for fast end-to-end testing")
def run(dry_run: bool, quick: bool):
    """Run the full pipeline: scrape → analyse → generate report."""
    _check_env()
    from src.pipeline import run as _run
    _run(dry_run=dry_run, quick=quick)


@cli.group()
def watchlist():
    """Manage the entity watchlist."""


@watchlist.command("list")
def watchlist_list():
    """Print the current watchlist."""
    wl_path = ROOT / "config" / "watchlist.yaml"
    with open(wl_path) as f:
        data = yaml.safe_load(f)
    entities = data.get("watchlist", [])
    table = Table(title="Watchlist", show_header=True)
    table.add_column("Name", style="bold")
    table.add_column("Type")
    table.add_column("Ticker")
    table.add_column("Aliases")
    for e in entities:
        table.add_row(
            e.get("name", ""),
            e.get("type", ""),
            e.get("ticker", "—"),
            ", ".join(e.get("aliases", [])),
        )
    console.print(table)


@watchlist.command("add")
@click.argument("name")
@click.option("--type", "entity_type", default="company", show_default=True,
              type=click.Choice(["company", "person"]))
@click.option("--ticker", default=None, help="Stock ticker (optional)")
@click.option("--alias", multiple=True, help="Additional search alias (repeatable)")
def watchlist_add(name: str, entity_type: str, ticker: str | None, alias: tuple[str, ...]):
    """Add an entity to the watchlist. Example: python main.py watchlist add 'Apple' --ticker AAPL"""
    wl_path = ROOT / "config" / "watchlist.yaml"
    with open(wl_path) as f:
        data = yaml.safe_load(f)

    entity: dict = {"name": name, "type": entity_type}
    if ticker:
        entity["ticker"] = ticker.upper()
        entity["seeking_alpha_slug"] = ticker.upper()
    if alias:
        entity["aliases"] = list(alias)

    data.setdefault("watchlist", []).append(entity)
    with open(wl_path, "w") as f:
        yaml.dump(data, f, default_flow_style=False, allow_unicode=True, sort_keys=False)
    console.print(f"[green]✓[/] Added [bold]{name}[/] to watchlist.")


@watchlist.command("remove")
@click.argument("name")
def watchlist_remove(name: str):
    """Remove an entity from the watchlist by name."""
    wl_path = ROOT / "config" / "watchlist.yaml"
    with open(wl_path) as f:
        data = yaml.safe_load(f)
    before = len(data.get("watchlist", []))
    data["watchlist"] = [e for e in data.get("watchlist", []) if e["name"].lower() != name.lower()]
    after = len(data["watchlist"])
    if before == after:
        console.print(f"[yellow]No entity named '{name}' found.[/]")
        return
    with open(wl_path, "w") as f:
        yaml.dump(data, f, default_flow_style=False, allow_unicode=True, sort_keys=False)
    console.print(f"[green]✓[/] Removed [bold]{name}[/] from watchlist.")


@cli.command()
def history():
    """Show recent pipeline run history."""
    from src import db
    db.init_db()
    with db.get_conn() as conn:
        rows = conn.execute(
            "SELECT id, started_at, finished_at, status, error_msg FROM runs ORDER BY id DESC LIMIT 20"
        ).fetchall()

    table = Table(title="Run History", show_header=True)
    table.add_column("ID", justify="right")
    table.add_column("Started")
    table.add_column("Duration")
    table.add_column("Status")
    table.add_column("Note")

    for row in rows:
        duration = "—"
        if row["started_at"] and row["finished_at"]:
            from datetime import datetime
            start = datetime.fromisoformat(row["started_at"])
            end = datetime.fromisoformat(row["finished_at"])
            secs = int((end - start).total_seconds())
            duration = f"{secs // 60}m {secs % 60}s"

        status_style = {"success": "green", "error": "red", "running": "yellow"}.get(row["status"], "")
        table.add_row(
            str(row["id"]),
            (row["started_at"] or "")[:19],
            duration,
            f"[{status_style}]{row['status']}[/]",
            row["error_msg"] or "",
        )
    console.print(table)


@cli.command("open")
def open_dashboard():
    """Open the latest dashboard in the default browser."""
    latest = ROOT / "output" / "latest" / "index.html"
    if not latest.exists():
        console.print("[red]No dashboard found. Run the pipeline first with: python main.py run[/]")
        sys.exit(1)
    webbrowser.open(f"file://{latest.resolve()}")
    console.print(f"[green]✓[/] Opened {latest}")


def _check_env():
    required = ["ANTHROPIC_API_KEY"]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        console.print(f"[red]Missing required environment variables: {', '.join(missing)}[/]")
        console.print("Copy [bold].env.example[/] to [bold].env[/] and fill in your keys.")
        sys.exit(1)

    optional_warnings = {
        "TAVILY_API_KEY": "news + podcast scraping disabled",
        "YOUTUBE_API_KEY": "YouTube scraping disabled",
        "SEEKING_ALPHA_EMAIL": "Seeking Alpha scraping disabled",
    }
    for k, msg in optional_warnings.items():
        if not os.environ.get(k):
            console.print(f"[yellow]Warning:[/] {k} not set — {msg}")


if __name__ == "__main__":
    cli()
