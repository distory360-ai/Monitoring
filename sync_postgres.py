"""Push newly-crawled articles into MediaPulse's production Postgres `news`
table.

This pipeline (scraper.py) has always written to a local SQLite archive
(data/mediapulse.db) that's committed back into this repo via Git LFS. That
archive was never connected to the Postgres database the live MediaPulse
dashboard/API actually queries (NEWS_DATABASE_URL) — the two were separate
systems. This script is the bridge: it reads every article the crawler has
collected since the last sync and upserts it into `public.news`, using the
same column shapes Engine/api.py already expects.

Idempotent and resumable: the high-water mark (last synced SQLite article id)
is stored in the SQLite `meta` table, so a run that's interrupted or re-run
simply continues from where it left off. The *first* run naturally backfills
every article already sitting in the archive.
"""
from __future__ import annotations

import hashlib
import logging
import os
import sqlite3
import sys
from datetime import datetime, timezone

from dateutil import parser as dateparser

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("sync_postgres")

DB_PATH = os.getenv("MP_DB_PATH", "data/mediapulse.db")
BATCH_SIZE = int(os.getenv("MP_PG_BATCH", "500"))
WATERMARK_KEY = "pg_synced_article_id"


def _pg_url() -> str:
    url = os.getenv("NEWS_DATABASE_URL", "").strip()
    if not url:
        log.error("NEWS_DATABASE_URL is not set — nothing to sync to. Add it as a repo secret.")
        sys.exit(1)
    url = url.replace("postgres://", "postgresql://", 1).split("?")[0]
    return url


def _watermark(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT value FROM meta WHERE key = ?", (WATERMARK_KEY,)).fetchone()
    return int(row[0]) if row else 0


def _set_watermark(conn: sqlite3.Connection, article_id: int) -> None:
    conn.execute(
        "INSERT INTO meta(key, value) VALUES(?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (WATERMARK_KEY, str(article_id)),
    )
    conn.commit()


def _to_date(published_at: str) -> str | None:
    """Best-effort publish date, independent of whatever string format the
    source used (RFC-2822, ISO, etc. have all landed in this column over
    time) — return a plain YYYY-MM-DD for Postgres's native `date` column."""
    if not published_at:
        return None
    try:
        return dateparser.parse(published_at).date().isoformat()
    except (ValueError, OverflowError, TypeError):
        return None


def _row_to_news(row: sqlite3.Row, companies: str) -> dict:
    url = row["url"] or ""
    return {
        "id": hashlib.sha1(url.encode("utf-8")).hexdigest() if url else None,
        "source": row["source"],
        "title": row["title"],
        "summary": row["summary"],
        "full_text": row["text"],
        "link": url,
        "author": row["author"],
        "published_date": row["published_at"],
        "collected_date": row["collected_at"],
        "category": row["category"],
        "sentiment_score": row["sentiment_score"],
        "sentiment_label": row["sentiment_label"],
        "keywords": row["keywords"],
        "companies_mentioned": companies or None,
        "date": _to_date(row["published_at"]),
        "text_length": row["char_count"],
    }


def main() -> None:
    import psycopg2
    import psycopg2.extras

    if not os.path.exists(DB_PATH):
        log.info(f"No SQLite archive at {DB_PATH} — nothing to sync.")
        return

    sconn = sqlite3.connect(DB_PATH)
    sconn.row_factory = sqlite3.Row

    watermark = _watermark(sconn)
    log.info(f"Syncing articles with id > {watermark}")

    pconn = psycopg2.connect(_pg_url(), sslmode="require")
    pconn.autocommit = False

    total = 0
    try:
        while True:
            rows = sconn.execute(
                "SELECT id, url, source, title, summary, text, author, published_at, "
                "collected_at, category, sentiment_score, sentiment_label, keywords, char_count "
                "FROM articles WHERE id > ? ORDER BY id LIMIT ?",
                (watermark, BATCH_SIZE),
            ).fetchall()
            if not rows:
                break

            records = []
            for row in rows:
                orgs = [o[0] for o in sconn.execute(
                    "SELECT org FROM orgs WHERE article_id = ?", (row["id"],))]
                records.append(_row_to_news(row, ", ".join(orgs)))

            with pconn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO public.news
                        (id, source, title, summary, full_text, link, author,
                         published_date, collected_date, category, sentiment_score,
                         sentiment_label, keywords, companies_mentioned, date, text_length)
                    VALUES %s
                    ON CONFLICT (title, link) DO NOTHING
                    """,
                    [(
                        r["id"], r["source"], r["title"], r["summary"], r["full_text"],
                        r["link"], r["author"], r["published_date"], r["collected_date"],
                        r["category"], r["sentiment_score"], r["sentiment_label"],
                        r["keywords"], r["companies_mentioned"], r["date"], r["text_length"],
                    ) for r in records],
                )
            pconn.commit()

            watermark = rows[-1]["id"]
            _set_watermark(sconn, watermark)
            total += len(rows)
            log.info(f"Synced {len(rows)} articles (watermark now {watermark})")

    except Exception:
        pconn.rollback()
        raise
    finally:
        pconn.close()
        sconn.close()

    log.info(f"Pipeline sync complete — {total} article(s) pushed to Postgres.")


if __name__ == "__main__":
    main()
