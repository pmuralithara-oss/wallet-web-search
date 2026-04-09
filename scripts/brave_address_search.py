"""Search exact wallet addresses on Brave, largest losses first. 30 parallel workers."""
import json
import logging
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import polars as pl
import requests

BRAVE_API_KEY = "BSAInM34_IPf8iRIQJ14p-9hnabTjVy"
DB_PATH = Path(__file__).parent.parent / "pipeline_social.db"
PARQUET_PATH = Path(__file__).parent.parent / "data" / "active_losers.parquet"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

SOCIAL_DOMAINS = {
    "reddit.com", "twitter.com", "x.com", "youtube.com", "medium.com",
    "substack.com", "mirror.xyz", "warpcast.com", "farcaster.xyz",
    "4plebs.org", "boards.4chan.org", "t.me", "discord.gg",
    "facebook.com", "instagram.com", "tiktok.com", "linkedin.com",
}


def init_db():
    conn = sqlite3.connect(str(DB_PATH), timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=60000")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS address_search (
            address TEXT PRIMARY KEY,
            n_results INTEGER DEFAULT 0,
            has_social_hit INTEGER DEFAULT 0,
            hit_urls TEXT,
            hit_titles TEXT,
            hit_snippets TEXT,
            searched_at TEXT,
            error TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_social ON address_search(has_social_hit);
    """)
    conn.commit()
    return conn


def is_social_url(url):
    try:
        domain = urlparse(url).netloc.lower().replace("www.", "").replace("old.", "")
        return any(sd in domain for sd in SOCIAL_DOMAINS)
    except Exception:
        return False


def search_one(address):
    try:
        resp = requests.get(
            "https://api.search.brave.com/res/v1/web/search",
            headers={"X-Subscription-Token": BRAVE_API_KEY, "Accept": "application/json"},
            params={"q": f'"{address}"', "count": 20, "safesearch": "off"},
            timeout=20,
        )
        if resp.status_code == 429:
            time.sleep(3)
            resp = requests.get(
                "https://api.search.brave.com/res/v1/web/search",
                headers={"X-Subscription-Token": BRAVE_API_KEY, "Accept": "application/json"},
                params={"q": f'"{address}"', "count": 20, "safesearch": "off"},
                timeout=20,
            )
        if resp.status_code != 200:
            return address, 0, False, None, None, None, f"HTTP {resp.status_code}"

        results = resp.json().get("web", {}).get("results", [])
        social_hits = [r for r in results if is_social_url(r.get("url", ""))]

        if social_hits:
            urls = json.dumps([h["url"] for h in social_hits])
            titles = json.dumps([h.get("title", "") for h in social_hits])
            snippets = json.dumps([h.get("description", "") for h in social_hits])
            return address, len(results), True, urls, titles, snippets, None
        return address, len(results), False, None, None, None, None

    except requests.exceptions.Timeout:
        return address, 0, False, None, None, None, "timeout"
    except Exception as e:
        return address, 0, False, None, None, None, str(e)[:200]


def main(workers=30):
    conn = init_db()

    df = pl.read_parquet(str(PARQUET_PATH))
    # Sort by loss (most negative first)
    df = df.sort("estimated_loss")
    all_addrs = df["address"].to_list()

    done = set(r[0] for r in conn.execute("SELECT address FROM address_search").fetchall())
    remaining = [a for a in all_addrs if a not in done]
    log.info(f"Total: {len(all_addrs):,}, done: {len(done):,}, remaining: {len(remaining):,}")

    if not remaining:
        return

    social_hits = 0
    total_done = 0
    errors = 0
    t0 = time.time()

    # Process in chunks to avoid spawning 739K futures at once
    CHUNK = 1000
    for chunk_start in range(0, len(remaining), CHUNK):
        chunk = remaining[chunk_start:chunk_start + CHUNK]
        batch = []

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(search_one, addr): addr for addr in chunk}
            for future in as_completed(futures):
                address, n_results, has_social, urls, titles, snippets, error = future.result()
                total_done += 1

                if has_social:
                    social_hits += 1
                    log.info(f"  SOCIAL HIT: {address} -> {urls}")
                if error:
                    errors += 1

                batch.append((address, n_results, 1 if has_social else 0, urls, titles, snippets,
                             datetime.now(timezone.utc).isoformat(), error))

        conn.executemany("INSERT OR IGNORE INTO address_search VALUES (?,?,?,?,?,?,?,?)", batch)
        conn.commit()
        elapsed = time.time() - t0
        rate = total_done / max(elapsed, 1)
        eta = (len(remaining) - total_done) / max(rate, 0.1) / 3600
        log.info(f"  {total_done:,}/{len(remaining):,} | {social_hits:,} social | {rate:.0f}/s | ETA: {eta:.1f}h | {errors} err")

    elapsed = time.time() - t0
    log.info(f"Done: {total_done:,} searched in {elapsed/3600:.1f}h, {social_hits:,} social hits")
    conn.close()


if __name__ == "__main__":
    w = int(sys.argv[1]) if len(sys.argv) > 1 else 30
    main(workers=w)
