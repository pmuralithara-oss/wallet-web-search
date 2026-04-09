"""Multi-engine wallet address search: SearXNG + Reddit + Bing. All free, no keys."""
import json
import logging
import re
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse, quote_plus

import requests
from bs4 import BeautifulSoup

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

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"


def is_social_url(url):
    try:
        domain = urlparse(url).netloc.lower().replace("www.", "").replace("old.", "")
        return any(sd in domain for sd in SOCIAL_DOMAINS)
    except Exception:
        return False


# ============ ENGINE 1: SearXNG ============

def get_searxng_instances():
    """Fetch and validate working SearXNG instances."""
    try:
        r = requests.get("https://searx.space/data/instances.json", timeout=15)
        data = r.json()
        instances = []
        for url, info in data.get("instances", {}).items():
            if info.get("network_type") == "normal" and info.get("version"):
                instances.append(url.rstrip("/"))
        return instances
    except Exception:
        return []


def search_searxng(address, instance):
    try:
        r = requests.get(
            f"{instance}/search",
            params={"q": f'"{address}"', "format": "json"},
            headers={"User-Agent": UA},
            timeout=10,
        )
        if r.status_code == 200:
            results = r.json().get("results", [])
            return [{"url": r.get("url", ""), "title": r.get("title", ""),
                      "snippet": r.get("content", "")} for r in results]
    except Exception:
        pass
    return None


# ============ ENGINE 2: Reddit Search ============

def search_reddit(address):
    try:
        r = requests.get(
            "https://www.reddit.com/search.json",
            params={"q": f'"{address}"', "limit": 25, "sort": "relevance"},
            headers={"User-Agent": "Academic-Research-PredictionMarket/1.0"},
            timeout=10,
        )
        if r.status_code == 200:
            posts = r.json().get("data", {}).get("children", [])
            results = []
            for p in posts:
                d = p.get("data", {})
                results.append({
                    "url": f"https://reddit.com{d.get('permalink', '')}",
                    "title": d.get("title", ""),
                    "snippet": d.get("selftext", "")[:300],
                })
            return results
    except Exception:
        pass
    return None


# ============ ENGINE 3: Bing ============

def search_bing(address):
    try:
        r = requests.get(
            f"https://www.bing.com/search?q={quote_plus(chr(34) + address + chr(34))}&count=20",
            headers={"User-Agent": UA},
            timeout=10,
        )
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, "lxml")
            results = []
            for li in soup.select("li.b_algo"):
                link = li.select_one("h2 a")
                snippet = li.select_one("p") or li.select_one("div.b_caption p")
                if link and link.get("href"):
                    results.append({
                        "url": link["href"],
                        "title": link.text,
                        "snippet": snippet.text if snippet else "",
                    })
            return results
    except Exception:
        pass
    return None


# ============ COMBINED SEARCH ============

def search_address(address, searxng_instances, searxng_idx):
    """Try all 3 engines, merge results."""
    all_results = []

    # Reddit (most valuable for us)
    reddit_results = search_reddit(address)
    if reddit_results:
        all_results.extend(reddit_results)

    # Bing
    bing_results = search_bing(address)
    if bing_results:
        all_results.extend(bing_results)

    # SearXNG (rotate through instances)
    if searxng_instances:
        idx = searxng_idx[0] % len(searxng_instances)
        searxng_idx[0] += 1
        sx_results = search_searxng(address, searxng_instances[idx])
        if sx_results:
            all_results.extend(sx_results)

    # Dedupe by URL
    seen = set()
    unique = []
    for r in all_results:
        if r["url"] not in seen:
            seen.add(r["url"])
            unique.append(r)

    social_hits = [r for r in unique if is_social_url(r["url"])]

    if social_hits:
        urls = json.dumps([h["url"] for h in social_hits])
        titles = json.dumps([h["title"] for h in social_hits])
        snippets = json.dumps([h["snippet"] for h in social_hits])
        return address, len(unique), True, urls, titles, snippets, None
    return address, len(unique), False, None, None, None, None


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
    """)
    conn.commit()
    return conn


def main(workers=50):
    conn = init_db()

    import polars as pl
    df = pl.read_parquet(str(PARQUET_PATH))
    df = df.sort("estimated_loss")
    all_addrs = df["address"].to_list()

    done = set(r[0] for r in conn.execute("SELECT address FROM address_search").fetchall())
    remaining = [a for a in all_addrs if a not in done]
    log.info(f"Total: {len(all_addrs):,}, done: {len(done):,}, remaining: {len(remaining):,}")

    if not remaining:
        return

    # Get SearXNG instances
    log.info("Fetching SearXNG instances...")
    searxng_instances = get_searxng_instances()
    log.info(f"Found {len(searxng_instances)} SearXNG instances")
    searxng_idx = [0]

    log.info(f"Starting multi-engine search with {workers} workers")
    social_hits = 0
    total_done = 0
    errors = 0
    t0 = time.time()

    CHUNK = 500
    for chunk_start in range(0, len(remaining), CHUNK):
        chunk = remaining[chunk_start:chunk_start + CHUNK]
        batch = []

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(search_address, addr, searxng_instances, searxng_idx): addr
                for addr in chunk
            }
            for future in as_completed(futures):
                try:
                    address, n_results, has_social, urls, titles, snippets, error = future.result()
                except Exception as e:
                    address = futures[future]
                    n_results, has_social, urls, titles, snippets = 0, False, None, None, None
                    error = str(e)[:100]

                total_done += 1
                if has_social:
                    social_hits += 1
                    log.info(f"  HIT: {address[:20]}... -> {urls}")
                if error:
                    errors += 1

                batch.append((address, n_results, 1 if has_social else 0, urls, titles, snippets,
                             datetime.now(timezone.utc).isoformat(), error))

        conn.executemany("INSERT OR IGNORE INTO address_search VALUES (?,?,?,?,?,?,?,?)", batch)
        conn.commit()
        elapsed = time.time() - t0
        rate = total_done / max(elapsed, 1)
        eta = (len(remaining) - total_done) / max(rate, 0.1) / 3600
        log.info(f"  {total_done:,}/{len(remaining):,} | {social_hits:,} social | {rate:.1f}/s | ETA: {eta:.1f}h | {errors} err")

    log.info(f"Done: {total_done:,} in {(time.time()-t0)/3600:.1f}h, {social_hits:,} social hits")
    conn.close()


if __name__ == "__main__":
    w = int(sys.argv[1]) if len(sys.argv) > 1 else 50
    main(workers=w)
