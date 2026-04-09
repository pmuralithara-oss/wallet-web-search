"""Search Google for exact wallet addresses using free proxy rotation."""
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

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0",
]


def fetch_proxies():
    """Fetch free proxy lists from multiple sources."""
    proxies = set()

    sources = [
        "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=5000&country=all&ssl=yes&anonymity=all",
        "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
        "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/http.txt",
        "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
    ]

    for url in sources:
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code == 200:
                for line in resp.text.strip().split("\n"):
                    line = line.strip()
                    if re.match(r"\d+\.\d+\.\d+\.\d+:\d+", line):
                        proxies.add(line)
        except Exception:
            pass

    log.info(f"Fetched {len(proxies)} raw proxies")
    return list(proxies)


def test_proxy(proxy):
    """Test if a proxy works with Google."""
    try:
        resp = requests.get(
            "https://www.google.com/search?q=test",
            proxies={"https": f"http://{proxy}"},
            headers={"User-Agent": USER_AGENTS[0]},
            timeout=8,
        )
        if resp.status_code == 200 and "google" in resp.text.lower():
            return proxy
    except Exception:
        pass
    return None


def validate_proxies(raw_proxies, max_workers=200, target=1000):
    """Test proxies in parallel, return working ones."""
    log.info(f"Testing {len(raw_proxies)} proxies with {max_workers} workers (target: {target})...")
    working = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(test_proxy, p): p for p in raw_proxies}
        for future in as_completed(futures):
            result = future.result()
            if result:
                working.append(result)
                if len(working) % 50 == 0:
                    log.info(f"  {len(working)} working proxies found...")
                if len(working) >= target:
                    # Cancel remaining
                    for f in futures:
                        f.cancel()
                    break

    log.info(f"Validated {len(working)} working proxies")
    return working


def is_social_url(url):
    try:
        domain = urlparse(url).netloc.lower().replace("www.", "").replace("old.", "")
        return any(sd in domain for sd in SOCIAL_DOMAINS)
    except Exception:
        return False


def google_search(address, proxy, ua):
    """Search Google for exact wallet address via proxy."""
    query = f'"{address}"'
    try:
        resp = requests.get(
            f"https://www.google.com/search?q={quote_plus(query)}&num=20",
            proxies={"https": f"http://{proxy}", "http": f"http://{proxy}"},
            headers={"User-Agent": ua, "Accept-Language": "en-US,en;q=0.9"},
            timeout=15,
        )

        if resp.status_code != 200:
            return address, 0, False, None, None, None, f"HTTP {resp.status_code}"

        soup = BeautifulSoup(resp.text, "lxml")
        results = []
        for g in soup.select("div.g"):
            link = g.select_one("a[href]")
            title = g.select_one("h3")
            snippet_div = g.select_one("div.VwiC3b") or g.select_one("span.st")
            if link and link.get("href", "").startswith("http"):
                results.append({
                    "url": link["href"],
                    "title": title.text if title else "",
                    "snippet": snippet_div.text if snippet_div else "",
                })

        social_hits = [r for r in results if is_social_url(r["url"])]

        if social_hits:
            urls = json.dumps([h["url"] for h in social_hits])
            titles = json.dumps([h["title"] for h in social_hits])
            snippets = json.dumps([h["snippet"] for h in social_hits])
            return address, len(results), True, urls, titles, snippets, None
        return address, len(results), False, None, None, None, None

    except requests.exceptions.Timeout:
        return address, 0, False, None, None, None, "timeout"
    except Exception as e:
        return address, 0, False, None, None, None, str(e)[:100]


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


def main(workers=200):
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

    # Fetch and validate proxies
    raw_proxies = fetch_proxies()
    working_proxies = validate_proxies(raw_proxies, max_workers=300, target=1000)

    if len(working_proxies) < 10:
        log.error("Not enough working proxies. Aborting.")
        return

    log.info(f"Starting search with {len(working_proxies)} proxies, {workers} workers")

    social_hits = 0
    total_done = 0
    errors = 0
    t0 = time.time()
    proxy_idx = [0]  # mutable counter for round-robin

    def get_next_proxy():
        idx = proxy_idx[0] % len(working_proxies)
        proxy_idx[0] += 1
        return working_proxies[idx]

    import random

    CHUNK = 2000
    for chunk_start in range(0, len(remaining), CHUNK):
        chunk = remaining[chunk_start:chunk_start + CHUNK]
        batch = []

        with ThreadPoolExecutor(max_workers=min(workers, len(working_proxies))) as executor:
            futures = {
                executor.submit(
                    google_search, addr, get_next_proxy(), random.choice(USER_AGENTS)
                ): addr
                for addr in chunk
            }

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
    log.info(f"Done: {total_done:,} in {elapsed/3600:.1f}h, {social_hits:,} social hits")
    conn.close()


if __name__ == "__main__":
    w = int(sys.argv[1]) if len(sys.argv) > 1 else 200
    main(workers=w)
