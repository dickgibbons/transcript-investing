"""SQLite database interface for storing transcripts, analyses, and run history."""

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any


DB_PATH = Path(__file__).parent.parent / "data" / "transcripts.db"


def get_db_path() -> Path:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return DB_PATH


@contextmanager
def get_conn():
    conn = sqlite3.connect(str(get_db_path()))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db() -> None:
    """Create all tables if they do not exist."""
    with get_conn() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS runs (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at  TEXT NOT NULL,
                finished_at TEXT,
                status      TEXT NOT NULL DEFAULT 'running',  -- running | success | error
                error_msg   TEXT
            );

            CREATE TABLE IF NOT EXISTS transcripts (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id       INTEGER REFERENCES runs(id),
                entity_name  TEXT NOT NULL,
                source       TEXT NOT NULL,   -- seeking_alpha | youtube | news | podcast
                url          TEXT UNIQUE NOT NULL,
                title        TEXT,
                published_at TEXT,
                fetched_at   TEXT NOT NULL,
                raw_text     TEXT NOT NULL,
                word_count   INTEGER
            );

            CREATE INDEX IF NOT EXISTS idx_transcripts_entity
                ON transcripts(entity_name);
            CREATE INDEX IF NOT EXISTS idx_transcripts_run
                ON transcripts(run_id);

            CREATE TABLE IF NOT EXISTS analyses (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id          INTEGER REFERENCES runs(id),
                transcript_id   INTEGER REFERENCES transcripts(id),
                entity_name     TEXT NOT NULL,
                signals_json    TEXT NOT NULL,   -- JSON array of signal objects
                themes_json     TEXT NOT NULL,   -- JSON array of theme strings
                analyzed_at     TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_analyses_run
                ON analyses(run_id);

            CREATE TABLE IF NOT EXISTS investment_reports (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id              INTEGER REFERENCES runs(id) UNIQUE,
                opportunities_json  TEXT NOT NULL,  -- JSON array of opportunity objects
                generated_at        TEXT NOT NULL,
                html_path           TEXT,
                pdf_path            TEXT
            );
        """)


# ── Run management ─────────────────────────────────────────────────────────────

def create_run() -> int:
    with get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO runs (started_at, status) VALUES (?, 'running')",
            (datetime.utcnow().isoformat(),),
        )
        return cur.lastrowid


def finish_run(run_id: int, status: str = "success", error_msg: str | None = None) -> None:
    with get_conn() as conn:
        conn.execute(
            "UPDATE runs SET finished_at=?, status=?, error_msg=? WHERE id=?",
            (datetime.utcnow().isoformat(), status, error_msg, run_id),
        )


# ── Transcript management ──────────────────────────────────────────────────────

def upsert_transcript(
    run_id: int,
    entity_name: str,
    source: str,
    url: str,
    title: str | None,
    published_at: str | None,
    raw_text: str,
) -> int | None:
    """Insert a transcript, ignoring duplicates by URL. Returns the row id."""
    word_count = len(raw_text.split())
    with get_conn() as conn:
        try:
            cur = conn.execute(
                """INSERT INTO transcripts
                   (run_id, entity_name, source, url, title, published_at, fetched_at, raw_text, word_count)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    run_id,
                    entity_name,
                    source,
                    url,
                    title,
                    published_at,
                    datetime.utcnow().isoformat(),
                    raw_text,
                    word_count,
                ),
            )
            return cur.lastrowid
        except sqlite3.IntegrityError:
            # Duplicate URL — already in DB from a previous run
            row = conn.execute("SELECT id FROM transcripts WHERE url=?", (url,)).fetchone()
            return row["id"] if row else None


def get_transcripts_for_run(run_id: int) -> list[dict[str, Any]]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM transcripts WHERE run_id=? ORDER BY entity_name, source",
            (run_id,),
        ).fetchall()
        return [dict(r) for r in rows]


def get_transcripts_by_ids(ids: list[int]) -> list[dict[str, Any]]:
    """Fetch transcripts by a list of IDs (used to include deduped results from prior runs)."""
    if not ids:
        return []
    placeholders = ",".join("?" * len(ids))
    with get_conn() as conn:
        rows = conn.execute(
            f"SELECT * FROM transcripts WHERE id IN ({placeholders}) ORDER BY entity_name, source",
            ids,
        ).fetchall()
        return [dict(r) for r in rows]


# ── Analysis management ────────────────────────────────────────────────────────

def save_analysis(
    run_id: int,
    transcript_id: int,
    entity_name: str,
    signals: list[dict],
    themes: list[str],
) -> int:
    with get_conn() as conn:
        cur = conn.execute(
            """INSERT INTO analyses
               (run_id, transcript_id, entity_name, signals_json, themes_json, analyzed_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                run_id,
                transcript_id,
                entity_name,
                json.dumps(signals),
                json.dumps(themes),
                datetime.utcnow().isoformat(),
            ),
        )
        return cur.lastrowid


def get_analyses_for_run(run_id: int) -> list[dict[str, Any]]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM analyses WHERE run_id=? ORDER BY entity_name",
            (run_id,),
        ).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            d["signals"] = json.loads(d["signals_json"])
            d["themes"] = json.loads(d["themes_json"])
            result.append(d)
        return result


# ── Investment report management ───────────────────────────────────────────────

def save_investment_report(
    run_id: int,
    opportunities: list[dict],
    html_path: str | None = None,
    pdf_path: str | None = None,
) -> int:
    with get_conn() as conn:
        cur = conn.execute(
            """INSERT OR REPLACE INTO investment_reports
               (run_id, opportunities_json, generated_at, html_path, pdf_path)
               VALUES (?, ?, ?, ?, ?)""",
            (
                run_id,
                json.dumps(opportunities),
                datetime.utcnow().isoformat(),
                html_path,
                pdf_path,
            ),
        )
        return cur.lastrowid


def get_investment_report(run_id: int) -> dict[str, Any] | None:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM investment_reports WHERE run_id=?", (run_id,)
        ).fetchone()
        if not row:
            return None
        d = dict(row)
        d["opportunities"] = json.loads(d["opportunities_json"])
        return d


def prune_old_runs(keep: int = 10) -> None:
    """Delete runs (and cascade data) beyond the `keep` most recent."""
    with get_conn() as conn:
        old_ids = conn.execute(
            "SELECT id FROM runs ORDER BY id DESC LIMIT -1 OFFSET ?", (keep,)
        ).fetchall()
        for row in old_ids:
            run_id = row["id"]
            conn.execute("DELETE FROM investment_reports WHERE run_id=?", (run_id,))
            conn.execute("DELETE FROM analyses WHERE run_id=?", (run_id,))
            conn.execute("DELETE FROM transcripts WHERE run_id=?", (run_id,))
            conn.execute("DELETE FROM runs WHERE id=?", (run_id,))
