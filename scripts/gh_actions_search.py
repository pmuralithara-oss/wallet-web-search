"""GitHub Actions search worker. Takes chunk_id and total_chunks as args."""
import json
import logging
import re
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from urllib.parse import urlparse, quote_plus

import requests
from bs4 import BeautifulSoup
import polars as pl

CHUNK_ID = int(sys.argv[1])
TOTAL_CHUNKS = int(sys.argv[2])
WORKERS = 50

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

SOCIAL_DOMAINS = {
    "reddit.com", "twitter.com", "x.com", "youtube.com", "medium.com",
    "substack.com", "mirror.xyz", "warpcast.com", "farcaster.xyz",
    "4plebs.org", "boards.4chan.org", "t.me", "discord.gg",
    "facebook.com", "instagram.com", "tiktok.com", "linkedin.com",
}
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"


def is_social(url):
    try:
        d = urlparse(url).netloc.lower().replace("www.", "").replace("old.", "")
        return any(s in d for s in SOCIAL_DOMAINS)
    except:
        return False


def search_reddit(addr):
    try:
        r = requests.get("https://www.reddit.com/search.json",
            params={"q": f'"{addr}"', "limit": 25, "sort": "relevance"},
            headers={"User-Agent": "Academic-Research/1.0"}, timeout=10)
        if r.status_code == 200:
            return [{"url": f"https://reddit.com{p['data']['permalink']}",
                     "title": p["data"].get("title", ""),
                     "snippet": p["data"].get("selftext", "")[:300]}
                    for p in r.json().get("data", {}).get("children", [])]
    except:
        pass
    return None


def search_bing(addr):
    try:
        r = requests.get(
            f"https://www.bing.com/search?q={quote_plus(chr(34) + addr + chr(34))}&count=20",
            headers={"User-Agent": UA}, timeout=10)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, "lxml")
            results = []
            for li in soup.select("li.b_algo"):
                a = li.select_one("h2 a")
                snip = li.select_one("p") or li.select_one("div.b_caption p")
                if a and a.get("href", "").startswith("http"):
                    results.append({"url": a["href"], "title": a.text,
                                    "snippet": snip.text if snip else ""})
            return results
    except:
        pass
    return None


def search_address(addr):
    results = []
    for fn in [search_reddit, search_bing]:
        r = fn(addr)
        if r:
            results.extend(r)
    seen = set()
    unique = [r for r in results if r["url"] not in seen and not seen.add(r["url"])]
    social = [r for r in unique if is_social(r["url"])]
    if social:
        return addr, len(unique), True, json.dumps([h["url"] for h in social]), \
               json.dumps([h["title"] for h in social]), json.dumps([h["snippet"] for h in social]), None
    return addr, len(unique), False, None, None, None, None


def main():
    df = pl.read_parquet("data/active_losers.parquet").sort("estimated_loss")
    all_addrs = df["address"].to_list()

    chunk_size = len(all_addrs) // TOTAL_CHUNKS
    start = CHUNK_ID * chunk_size
    end = start + chunk_size if CHUNK_ID < TOTAL_CHUNKS - 1 else len(all_addrs)
    my_addrs = all_addrs[start:end]

    log.info(f"Chunk {CHUNK_ID}/{TOTAL_CHUNKS}: {len(my_addrs):,} addresses [{start:,}:{end:,}]")

    db = sqlite3.connect(f"results_chunk_{CHUNK_ID}.db")
    db.execute("CREATE TABLE IF NOT EXISTS hits (address TEXT PRIMARY KEY, n_results INT, has_social INT, urls TEXT, titles TEXT, snippets TEXT, searched_at TEXT, error TEXT)")
    db.commit()

    done = set(r[0] for r in db.execute("SELECT address FROM hits").fetchall())
    remaining = [a for a in my_addrs if a not in done]
    log.info(f"Done: {len(done):,}, remaining: {len(remaining):,}")

    social_hits = 0
    total = 0
    t0 = time.time()

    CHUNK_SZ = 500
    for i in range(0, len(remaining), CHUNK_SZ):
        chunk = remaining[i:i + CHUNK_SZ]
        batch = []
        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            futs = {ex.submit(search_address, a): a for a in chunk}
            for f in as_completed(futs):
                try:
                    addr, nr, hs, urls, titles, snips, err = f.result()
                except Exception as e:
                    addr = futs[f]
                    nr, hs, urls, titles, snips = 0, False, None, None, None
                    err = str(e)[:100]
                total += 1
                if hs:
                    social_hits += 1
                    log.info(f"HIT: {addr[:20]}... -> {urls}")
                batch.append((addr, nr, 1 if hs else 0, urls, titles, snips,
                             datetime.now(timezone.utc).isoformat(), err))
        db.executemany("INSERT OR IGNORE INTO hits VALUES (?,?,?,?,?,?,?,?)", batch)
        db.commit()
        elapsed = time.time() - t0
        rate = total / max(elapsed, 1)
        eta = (len(remaining) - total) / max(rate, 0.1) / 3600
        log.info(f"  {total:,}/{len(remaining):,} | {social_hits} social | {rate:.1f}/s | ETA: {eta:.1f}h")

    log.info(f"Chunk {CHUNK_ID} done: {social_hits} social hits in {total:,} searches ({(time.time()-t0)/60:.0f} min)")
    db.close()


if __name__ == "__main__":
    main()
