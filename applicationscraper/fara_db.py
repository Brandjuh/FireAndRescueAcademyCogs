from __future__ import annotations

import logging
import re
import shutil
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def connect_database(db_path, *, timeout: float = 30.0) -> sqlite3.Connection:
    """Open a SQLite connection with the safety settings used by the cogs."""
    conn = sqlite3.connect(str(db_path), timeout=timeout)
    conn.execute("PRAGMA busy_timeout = 30000")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _safe_backup_reason(reason: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "-", reason.strip().lower())
    return cleaned.strip("-") or "schema-change"


def backup_database(db_path, reason: str, *, logger: Optional[logging.Logger] = None) -> Optional[Path]:
    """Copy a database before an additive migration changes its schema."""
    path = Path(db_path)
    if not path.exists() or path.stat().st_size == 0:
        return None

    backup_dir = path.parent / "backups"
    backup_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    suffix = _safe_backup_reason(reason)
    target = backup_dir / f"{path.stem}-{timestamp}-{suffix}{path.suffix}"
    counter = 2
    while target.exists():
        target = backup_dir / f"{path.stem}-{timestamp}-{suffix}-{counter}{path.suffix}"
        counter += 1

    shutil.copy2(path, target)
    if logger:
        logger.info("SQLite backup created before schema change: %s", target)
    return target


def table_columns(cursor: sqlite3.Cursor, table_name: str) -> set[str]:
    cursor.execute(f"PRAGMA table_info({table_name})")
    return {row[1] for row in cursor.fetchall()}


def add_column_if_missing(
    conn: sqlite3.Connection,
    db_path,
    table_name: str,
    column_name: str,
    alter_statement: str,
    *,
    logger: Optional[logging.Logger] = None,
) -> bool:
    """Run an additive ALTER TABLE only when needed, with a one-time backup."""
    cursor = conn.cursor()
    if column_name in table_columns(cursor, table_name):
        return False
    backup_database(db_path, f"add-{table_name}-{column_name}", logger=logger)
    cursor.execute(alter_statement)
    return True


def ensure_scrape_runs_table(cursor: sqlite3.Cursor) -> None:
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS scrape_runs (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            scraper TEXT NOT NULL,
            source TEXT NOT NULL DEFAULT 'live',
            source_timestamp TEXT,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            status TEXT NOT NULL,
            pages_attempted INTEGER NOT NULL DEFAULT 0,
            pages_succeeded INTEGER NOT NULL DEFAULT 0,
            rows_parsed INTEGER NOT NULL DEFAULT 0,
            rows_inserted INTEGER NOT NULL DEFAULT 0,
            duplicates INTEGER NOT NULL DEFAULT 0,
            errors INTEGER NOT NULL DEFAULT 0,
            message TEXT
        )
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_scrape_runs_lookup
        ON scrape_runs(scraper, source, status, finished_at)
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_scrape_runs_source_timestamp
        ON scrape_runs(scraper, source, source_timestamp)
        """
    )


def start_scrape_run(
    conn: sqlite3.Connection,
    scraper: str,
    *,
    source: str = "live",
    source_timestamp: Optional[str] = None,
    message: Optional[str] = None,
) -> int:
    cursor = conn.cursor()
    ensure_scrape_runs_table(cursor)
    cursor.execute(
        """
        INSERT INTO scrape_runs (
            scraper, source, source_timestamp, started_at, status, message
        ) VALUES (?, ?, ?, ?, 'running', ?)
        """,
        (scraper, source, source_timestamp, utc_now_iso(), message),
    )
    conn.commit()
    return int(cursor.lastrowid)


def finish_scrape_run(
    conn: sqlite3.Connection,
    run_id: Optional[int],
    status: str,
    *,
    pages_attempted: int = 0,
    pages_succeeded: int = 0,
    rows_parsed: int = 0,
    rows_inserted: int = 0,
    duplicates: int = 0,
    errors: int = 0,
    message: Optional[str] = None,
) -> None:
    if run_id is None:
        return
    conn.execute(
        """
        UPDATE scrape_runs
        SET finished_at = ?,
            status = ?,
            pages_attempted = ?,
            pages_succeeded = ?,
            rows_parsed = ?,
            rows_inserted = ?,
            duplicates = ?,
            errors = ?,
            message = ?
        WHERE run_id = ?
        """,
        (
            utc_now_iso(),
            status,
            int(pages_attempted),
            int(pages_succeeded),
            int(rows_parsed),
            int(rows_inserted),
            int(duplicates),
            int(errors),
            message,
            int(run_id),
        ),
    )
    conn.commit()


def latest_successful_source_timestamp(
    conn: sqlite3.Connection,
    scraper: str,
    *,
    source: str = "live",
) -> Optional[str]:
    row = conn.execute(
        """
        SELECT source_timestamp
        FROM scrape_runs
        WHERE scraper = ?
          AND source = ?
          AND status = 'success'
          AND source_timestamp IS NOT NULL
        ORDER BY finished_at DESC, run_id DESC
        LIMIT 1
        """,
        (scraper, source),
    ).fetchone()
    return row[0] if row else None


def start_scrape_run_for_path(
    db_path,
    scraper: str,
    *,
    source: str = "live",
    source_timestamp: Optional[str] = None,
    logger: Optional[logging.Logger] = None,
) -> Optional[int]:
    if not db_path:
        return None
    try:
        conn = connect_database(db_path)
        try:
            return start_scrape_run(
                conn,
                scraper,
                source=source,
                source_timestamp=source_timestamp,
            )
        finally:
            conn.close()
    except Exception:
        if logger:
            logger.exception("Failed to start %s scrape run", scraper)
        return None


def finish_scrape_run_for_path(
    db_path,
    run_id: Optional[int],
    status: str,
    *,
    logger: Optional[logging.Logger] = None,
    **kwargs,
) -> None:
    if not db_path or run_id is None:
        return
    try:
        conn = connect_database(db_path)
        try:
            finish_scrape_run(conn, run_id, status, **kwargs)
        finally:
            conn.close()
    except Exception:
        if logger:
            logger.exception("Failed to finish scrape run %s", run_id)
