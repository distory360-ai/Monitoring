
from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import os
import re
import sqlite3
import sys
from collections import Counter

import scraper as p

ml = p   # language helpers live in scraper.py

_OPERATOR = re.compile(r'"|\bAND\b|\bOR\b|\bNOT\b|\bNEAR\(|\*')


def to_fts_query(q: str) -> str:
    """Plain text -> exact phrase. Anything with operators or quotes is passed through as FTS5 syntax."""
    q = q.strip()
    return q if _OPERATOR.search(q) else p.fts_phrase(q)


def _date_expr() -> str:
    return "COALESCE(NULLIF(a.published_at, ''), a.collected_at)"


def _filters(countries, tiers, language, include_duplicates) -> tuple[list, list]:
    sql, args = [], []
    if countries:
        sql.append(f"AND a.source_country IN ({','.join('?' * len(countries))})")
        args += countries
    if tiers:
        sql.append(f"AND a.tier IN ({','.join('?' * len(tiers))})")
        args += tiers
    if language:
        sql.append("AND a.language = ?")
        args.append(ml.normalize_lang(language))
    if not include_duplicates:
        sql.append("AND a.duplicate_of IS NULL")
    return sql, args


def _hit(row: tuple, snippet: str) -> dict:
    (aid, date, source, country, tier, lang, title, url, dup) = row[:9]
    score, label = (None, "Not scored")
    clean = snippet.replace("[", "").replace("]", "")
    if clean and p.scorable(lang or ""):
        score, label = p.sentiment_for(clean, lang)
    return {"date": date, "source": source, "country": country, "tier": tier or "unlisted",
            "language": lang, "title": title, "url": url, "duplicate_of": dup,
            "sentiment_score": score, "sentiment_label": label, "snippet": snippet}


def search_script(conn, query: str, since: str, countries, tiers, language, include_duplicates) -> list[dict]:
    """Ge'ez / Arabic-script terms: prefixes are glued to words (የኢትዮ ቴሌኮም, والبنك), so token
    search misses them. Scan normalised text instead."""
    needle = ml.normalize_script(query.strip())
    extra, args = _filters(countries, tiers, language, include_duplicates)
    rows = conn.execute(f"""
        SELECT a.id, {_date_expr()} AS date, a.source, a.source_country, a.tier, a.language, a.title, a.url,
               a.duplicate_of, a.title, a.text, a.summary
        FROM articles a WHERE {_date_expr()} >= ? {' '.join(extra)} ORDER BY date DESC""", [since] + args)
    out = []
    for row in rows:
        title, text, summary = row[9] or "", row[10] or "", row[11] or ""
        body = ml.normalize_script(f"{title}\n{text or summary}")
        i = body.find(needle)
        if i < 0:
            continue
        j = i + len(needle)
        out.append(_hit(row, f"{body[max(0, i - 120):i]}[{body[i:j]}]{body[j:j + 120]}".replace("\n", " ")))
    return out


def search(conn: sqlite3.Connection, query: str, days: int = 30, countries=None, tiers=None,
           language: str = "", exact_case: bool = False, include_duplicates: bool = True) -> list[dict]:
    since = p.iso(p.now_utc() - dt.timedelta(days=days))
    if ml.is_nonlatin(query) and not _OPERATOR.search(query):
        return search_script(conn, query, since, countries, tiers, language, include_duplicates)
    sql = [f"""
        SELECT a.id, {_date_expr()} AS date, a.source, a.source_country, a.tier, a.language, a.title, a.url,
               a.duplicate_of, a.text, a.summary,
               snippet(articles_fts, 1, '[', ']', ' … ', 28) AS snip_text,
               snippet(articles_fts, 0, '[', ']', ' … ', 20) AS snip_title
        FROM articles_fts JOIN articles a ON a.id = articles_fts.rowid
        WHERE articles_fts MATCH ? AND {_date_expr()} >= ?"""]
    args: list = [to_fts_query(query), since]
    extra, extra_args = _filters(countries, tiers, language, include_duplicates)
    sql += extra
    args += extra_args
    sql.append("ORDER BY date DESC")
    try:
        rows = conn.execute(" ".join(sql), args).fetchall()
    except sqlite3.OperationalError as e:
        raise SystemExit(f"Query syntax error ({e}). Quote phrases, use uppercase AND/OR/NOT.")

    exact = re.compile(rf"(?<![\w-]){re.escape(query.strip())}(?![\w-])") if exact_case else None
    out = []
    for row in rows:
        title, text, summary, snip_text, snip_title = row[6], row[9], row[10], row[11], row[12]
        if exact and not exact.search(f"{title}\n{text or summary or ''}"):
            continue
        out.append(_hit(row, (snip_text or "").strip() or (snip_title or "").strip()))
    return out


def report(query: str, hits: list[dict], days: int):
    print(f"\n{'═' * 70}\n  \"{query}\" — last {days} days\n{'═' * 70}")
    if not hits:
        print("  No mentions in the archive. Try --collect to pull fresh coverage from GDELT and Google News,\n"
              "  or widen --days. Coverage is limited to the outlets crawled since the archive started.")
        return
    unique = [h for h in hits if not h["duplicate_of"]]
    print(f"  Mentions: {len(hits)}   Unique stories: {len(unique)}   "
          f"Outlets: {len({h['source'] for h in hits})}   "
          f"Countries: {len({h['country'] for h in hits if h['country']})}")

    def line(title, counter, n=8):
        print(f"\n  {title}")
        for k, v in counter.most_common(n):
            print(f"    {str(k or '—')[:34]:<34} {v:>5}")

    line("By tier", Counter(h["tier"] for h in hits))
    line("Top outlets", Counter(h["source"] for h in hits))
    line("By country", Counter(h["country"] for h in hits))
    line("By language", Counter(ml.LANGUAGE_NAMES.get(h["language"] or "und", h["language"]) for h in hits))
    scored = [h for h in hits if h["sentiment_score"] is not None]
    if scored:
        langs = sorted({ml.LANGUAGE_NAMES.get(h["language"], h["language"]) for h in scored})
        line(f"Sentiment (n={len(scored)}; scored languages: {', '.join(langs)})",
             Counter(h["sentiment_label"] for h in scored))
    unscored = len(hits) - len(scored)
    if unscored:
        print(f"    ({unscored} mentions in languages without a sentiment model are 'Not scored' — see MP_SENTIMENT_MODEL)")

    daily = Counter((h["date"] or "")[:10] for h in hits)
    peak = max(daily.values())
    print("\n  Daily volume")
    for day in sorted(daily)[-14:]:
        print(f"    {day}  {'█' * max(1, round(daily[day] / peak * 30))} {daily[day]}")

    print("\n  Latest")
    for h in hits[:8]:
        print(f"    {(h['date'] or '')[:10]}  {h['source'][:22]:<22} {h['title'][:70]}")
        if h["snippet"]:
            print(f"                {h['snippet'][:110]}")


def write_csv(path: str, hits: list[dict]):
    import csv
    with open(path, "w", newline="", encoding="utf-8") as fh:
        cols = ["date", "source", "country", "tier", "language", "title", "url", "duplicate_of",
                "sentiment_score", "sentiment_label", "snippet"]
        w = csv.DictWriter(fh, fieldnames=cols)
        w.writeheader()
        w.writerows(hits)
    print(f"\n  CSV: {path}")


def track(conn, name: str, aliases: list[str], context: list[str], country: str, sector: str) -> int:
    aliases = list(dict.fromkeys([name] + aliases))
    conn.execute("INSERT OR REPLACE INTO watchlist VALUES (?,?,?,?,?,?)",
                 (name, json.dumps(aliases), json.dumps(context), country, sector, p.iso(p.now_utc())))
    spec = p.load_watchlist(conn)[name]
    n = p.backfill_brand(conn, name, spec)
    conn.commit()
    return n


def untrack(conn, name: str) -> bool:
    cur = conn.execute("DELETE FROM watchlist WHERE name = ?", (name,))
    if cur.rowcount and name not in p.BRANDS:
        conn.execute("DELETE FROM mentions WHERE brand = ?", (name,))
        conn.execute("DELETE FROM brand_backfill WHERE name = ?", (name,))
    conn.commit()
    return bool(cur.rowcount)


_ORG_NOISE = {"reuters", "afp", "ap", "associated press", "bloomberg", "xinhua", "bbc", "cnn", "twitter", "x",
              "facebook", "instagram", "youtube", "tiktok", "whatsapp", "google", "government", "parliament",
              "senate", "ministry", "police", "court", "high court", "supreme court", "state house", "cabinet",
              "un", "united nations", "au", "african union", "eu", "european union", "nato", "who"}


def discover(conn, days: int, limit: int = 30, min_mentions: int = 2) -> list[tuple]:
    """ORG entities in recent coverage that don't match any tracked brand or known outlet,
    ranked by mentions this period, with the previous period for comparison."""
    now = p.now_utc()
    cur_start = p.iso(now - dt.timedelta(days=days))
    prev_start = p.iso(now - dt.timedelta(days=2 * days))
    rows = conn.execute(f"""
        SELECT o.org, {_date_expr()} >= ? AS is_cur, a.source
        FROM orgs o JOIN articles a ON a.id = o.article_id
        WHERE a.duplicate_of IS NULL AND {_date_expr()} >= ?""", (cur_start, prev_start)).fetchall()
    registry = p.active_registry(conn)
    registry_lower = {n.lower() for n in registry}
    matcher = p.TermMatcher(registry)
    outlets = {s["name"].lower() for s in p.SOURCES}
    display, cur, prev, srcs = {}, Counter(), Counter(), {}
    for org, is_cur, source in rows:
        key = org.lower()
        display.setdefault(key, org)
        (cur if is_cur else prev)[key] += 1
        srcs.setdefault(key, set()).add(source)
    out = []
    for key, n in cur.items():
        if n < min_mentions or key in _ORG_NOISE or key in outlets or key in registry_lower:
            continue
        if matcher.find(display[key]):          # "Safaricom PLC" etc. are already tracked
            continue
        out.append((display[key], n, prev[key], len(srcs[key])))
    out.sort(key=lambda r: (r[1], r[1] - r[2]), reverse=True)
    return out[:limit]


def import_legacy_csv(conn, path: str) -> dict:
    """One-time import of the v3 daily_news.csv (headlines/summaries, no full text) into the archive,
    so years of earlier collection stay searchable and get today's brand matching. Safe to re-run:
    rows already in the archive are skipped, and a finished import is remembered."""
    import csv
    key = f"legacy_import:{os.path.basename(path)}"
    if conn.execute("SELECT 1 FROM meta WHERE key = ?", (key,)).fetchone():
        return {"skipped": "already imported"}
    p.use_registry(p.active_registry(conn))
    counts = Counter()
    with open(path, newline="", encoding="utf-8", errors="replace") as fh:
        head = fh.read(200)
        if head.startswith("version https://git-lfs"):
            raise SystemExit(f"{path} is a Git LFS pointer, not the data. Check out with LFS enabled (git lfs pull).")
        fh.seek(0)
        for row in csv.DictReader(fh):
            url = p.canonicalize(row.get("link") or row.get("url") or "")
            title = p.clean_text(row.get("title") or "")
            if not url or not title:
                counts["no_url_or_title"] += 1
                continue
            c = p.Candidate(url=url, source=(row.get("source") or "").strip() or "legacy",
                            via="legacy_v3", published=p.parse_date(row.get("published_date")),
                            title_hint=title, summary_hint=p.clean_text(row.get("summary") or "")[:500])
            built = p.build_record("hint", c, None, url, p.EPOCH)
            if isinstance(built, str):
                counts[built] += 1
                continue
            record, mentions, targets, orgs = built
            collected = p.parse_date(row.get("collected_date"))
            if collected:
                record["collected_at"] = p.iso(collected)
            if p.save_article(conn, record, mentions, targets, orgs):
                counts["imported"] += 1
                counts["with_brand"] += bool(mentions)
            else:
                counts["already_in_archive"] += 1
            p.mark_seen(conn, url, "legacy")
    conn.execute("INSERT OR REPLACE INTO meta VALUES (?, ?)", (key, p.iso(p.now_utc())))
    conn.commit()
    return dict(counts)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("query", nargs="?", help="brand or boolean query")
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--country", action="append", help="ISO-2 source country, repeatable (e.g. UG)")
    ap.add_argument("--tier", action="append", help="national/trade/digital/syndication/international/unlisted")
    ap.add_argument("--language", default="", help="e.g. en, fr, sw")
    ap.add_argument("--case", action="store_true", help="require exact case (plain terms only)")
    ap.add_argument("--unique", action="store_true", help="drop syndicated duplicates")
    ap.add_argument("--csv", help="write results to this CSV")
    ap.add_argument("--collect", action="store_true", help="pull GDELT + Google News for the query first")
    ap.add_argument("--track", metavar="NAME", help="add a brand to the watchlist and backfill it")
    ap.add_argument("--aliases", default="", help="comma-separated extra names for --track")
    ap.add_argument("--context", default="", help="comma-separated context words for ambiguous names")
    ap.add_argument("--sector", default="")
    ap.add_argument("--untrack", metavar="NAME")
    ap.add_argument("--list", action="store_true", help="list watchlist brands")
    ap.add_argument("--discover", action="store_true", help="organisations in the news you aren't tracking")
    ap.add_argument("--import-legacy", metavar="CSV", help="one-time import of the v3 daily_news.csv")
    ap.add_argument("--db", default=p.DB_PATH)
    a = ap.parse_args(argv)
    split = lambda s: [x.strip() for x in s.split(",") if x.strip()]

    if a.import_legacy:
        conn = p.db_connect(a.db)
        print(f"Legacy import from {a.import_legacy}: {import_legacy_csv(conn, a.import_legacy)}")
        return

    if a.track:
        conn = p.db_connect(a.db)
        n = track(conn, a.track, split(a.aliases), split(a.context), (a.country or [""])[0], a.sector)
        print(f"Tracking '{a.track}'. Back-applied to the archive: {n} existing articles mention it.")
        return
    if a.untrack:
        conn = p.db_connect(a.db)
        print("Removed." if untrack(conn, a.untrack) else f"'{a.untrack}' is not on the watchlist "
              "(brands in config.py are removed by editing config.py).")
        return
    if a.list:
        conn = p.db_connect(a.db)
        rows = conn.execute("SELECT name, aliases, country, sector, added_at FROM watchlist ORDER BY name").fetchall()
        print(f"Watchlist ({len(rows)}), in addition to {len(p.BRANDS)} brands in config.py:")
        for name, aliases, country, sector, added in rows:
            print(f"  {name:<28} {country or '—':<4} {sector or '—':<12} {', '.join(json.loads(aliases))}")
        return
    if a.discover:
        conn = p.db_connect(a.db)
        rows = discover(conn, a.days)
        if not rows:
            print("No untracked organisations found (organisation detection needs spaCy's en_core_web_sm "
                  "and covers English articles only).")
            return
        print(f"\nUntracked organisations, last {a.days} days vs the {a.days} days before:")
        print(f"  {'Organisation':<40} {'Now':>5} {'Before':>7} {'Outlets':>8}")
        for org, n, prev, outlets in rows:
            print(f"  {org[:40]:<40} {n:>5} {prev:>7} {outlets:>8}")
        print('\n  Track one with:  python ingest.py --track "Name" --country XX')
        return
    if not a.query:
        ap.print_help()
        sys.exit(1)

    if a.collect:
        terms = [a.query] if not _OPERATOR.search(a.query) else re.findall(r'"([^"]+)"', a.query)
        if not terms:
            raise SystemExit("--collect needs a plain name or at least one \"quoted phrase\" in the query.")
        stats = asyncio.run(p.collect_terms(terms, a.days, db_path=a.db))
        print(f"Collected: {stats.get('saved', 0)} new articles "
              f"({stats.get('candidates', 0)} candidates, {stats.get('already_seen', 0)} already in archive).")

    conn = p.db_connect(a.db)
    hits = search(conn, a.query, a.days, a.country, a.tier, a.language, a.case, not a.unique)
    report(a.query, hits, a.days)
    if a.csv:
        write_csv(a.csv, hits)


if __name__ == "__main__":
    main()
