"""Stage 2: Search Brave for each wallet's username + polymarket. Parallel, 50 QPS."""
import argparse
import json
import logging
import sqlite3
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests

from src.config import BRAVE_API_KEY, SEARCH_RESULTS_DIR, LOGS_DIR, DB_PATH
from src.db import init_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOGS_DIR / "stage2.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

BRAVE_URL = "https://api.search.brave.com/res/v1/web/search"
BRAVE_HEADERS = {
    "X-Subscription-Token": BRAVE_API_KEY,
    "Accept": "application/json",
}

SOURCE_TYPE_MAP = {
    "reddit.com": "reddit", "old.reddit.com": "reddit",
    "twitter.com": "twitter", "x.com": "twitter", "mobile.twitter.com": "twitter",
    "warpcast.com": "farcaster", "farcaster.xyz": "farcaster",
    "4plebs.org": "forum", "archive.4plebs.org": "forum", "boards.4chan.org": "forum",
    "etherscan.io": "etherscan", "debank.com": "debank",
    "zapper.xyz": "dashboard", "dune.com": "dashboard",
    "polygonscan.com": "etherscan",
    "substack.com": "blog", "mirror.xyz": "blog", "medium.com": "blog",
    "polymarket.com": "polymarket",
}


def classify_source(url):
    try:
        domain = urlparse(url).netloc.lower()
    except Exception:
        return "other", 1
    if domain.startswith("www."):
        domain = domain[4:]
    source_type = SOURCE_TYPE_MAP.get(domain)
    if source_type is None:
        parts = domain.split(".")
        if len(parts) > 2:
            source_type = SOURCE_TYPE_MAP.get(".".join(parts[-2:]))
    if source_type is None:
        if any(k in domain for k in ["news", "cnn", "bbc", "reuters", "bloomberg",
                                      "coindesk", "decrypt", "theblock", "benzinga"]):
            source_type = "news"
        elif "youtube.com" in domain or "youtu.be" in domain:
            source_type = "youtube"
        else:
            source_type = "other"

    if source_type in ("reddit", "twitter", "farcaster", "forum", "youtube"):
        priority = 3
    elif source_type in ("blog", "news"):
        priority = 2
    elif source_type == "etherscan":
        priority = 1 if "/tx/" in url else 0
    elif source_type in ("dashboard", "polymarket"):
        priority = 0
    elif source_type == "debank":
        priority = 1
    else:
        priority = 1
    return source_type, priority


def search_one(address, username):
    """Search Brave for one wallet. Returns (address, results_list, error_or_None)."""
    query = f'"{username}" "polymarket"'
    try:
        resp = requests.get(
            BRAVE_URL, headers=BRAVE_HEADERS,
            params={"q": query, "count": 20, "safesearch": "off"},
            timeout=20,
        )
        if resp.status_code == 429:
            time.sleep(2)
            resp = requests.get(
                BRAVE_URL, headers=BRAVE_HEADERS,
                params={"q": query, "count": 20, "safesearch": "off"},
                timeout=20,
            )
        if resp.status_code != 200:
            return address, [], f"HTTP {resp.status_code}"

        data = resp.json()
        # Save raw
        out_path = SEARCH_RESULTS_DIR / f"{address}.json"
        with open(out_path, "w") as f:
            json.dump({"query": query, "username": username, "results": data.get("web", {}).get("results", [])}, f)

        return address, data.get("web", {}).get("results", []), None
    except requests.exceptions.Timeout:
        return address, [], "timeout"
    except Exception as e:
        return address, [], str(e)[:200]


def run(yes=False, workers=30):
    init_db()
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")

    rows = conn.execute(
        "SELECT address, estimated_loss, loss_rank, username FROM wallets WHERE selected=1 AND searched=0 AND username IS NOT NULL ORDER BY loss_rank"
    ).fetchall()

    if not rows:
        log.info("No unsearched wallets.")
        conn.close()
        return

    log.info(f"Found {len(rows)} wallets to search with {workers} workers (~{len(rows)//workers}s)")

    if not yes:
        print(f"\nAbout to search {len(rows)} wallets via Brave ({workers} parallel). ETA: ~{len(rows)//workers}s")
        if input("Proceed? [y/N] ").strip().lower() != "y":
            conn.close()
            return

    total_hits = 0
    errors = 0
    source_counts = defaultdict(int)
    priority_counts = defaultdict(int)
    done = 0

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(search_one, row["address"], row["username"]): row
            for row in rows
        }

        batch_updates = []
        batch_hits = []

        for future in as_completed(futures):
            row = futures[future]
            addr, results, error = future.result()
            done += 1

            now = datetime.now(timezone.utc).isoformat()

            if error:
                batch_updates.append(("UPDATE wallets SET searched=1, searched_at=?, search_error=? WHERE address=?",
                                      (now, error, addr)))
                errors += 1
            else:
                stored = 0
                for r in results:
                    url = r.get("url", "")
                    title = r.get("title", "")
                    snippet = r.get("description", "")
                    st, pri = classify_source(url)
                    if pri == 0:
                        continue
                    batch_hits.append((addr, url, title, snippet, st, pri))
                    source_counts[st] += 1
                    priority_counts[pri] += 1
                    stored += 1
                total_hits += stored
                batch_updates.append(("UPDATE wallets SET searched=1, searched_at=?, num_results=? WHERE address=?",
                                      (now, stored, addr)))

            # Flush every 200
            if len(batch_updates) >= 200:
                for sql, params in batch_updates:
                    conn.execute(sql, params)
                for h in batch_hits:
                    conn.execute("INSERT OR IGNORE INTO search_hits (address,url,title,snippet,source_type,priority) VALUES (?,?,?,?,?,?)", h)
                conn.commit()
                log.info(f"  {done}/{len(rows)} searched, {total_hits} hits, {errors} errors")
                batch_updates = []
                batch_hits = []

        # Final flush
        for sql, params in batch_updates:
            conn.execute(sql, params)
        for h in batch_hits:
            conn.execute("INSERT OR IGNORE INTO search_hits (address,url,title,snippet,source_type,priority) VALUES (?,?,?,?,?,?)", h)
        conn.commit()

    conn.close()

    print("\n" + "=" * 70)
    print("STAGE 2 REPORT: Brave Search")
    print("=" * 70)
    print(f"  Wallets searched:     {len(rows)}")
    print(f"  Search errors:        {errors}")
    print(f"  Total hits stored:    {total_hits}")
    print(f"\n  By priority:")
    for p in [3, 2, 1]:
        print(f"    Priority {p}:          {priority_counts.get(p, 0)}")
    print(f"\n  By source type:")
    for st, count in sorted(source_counts.items(), key=lambda x: -x[1]):
        print(f"    {st:25} {count:>5}")
    print(f"\nStage 2 complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--yes", "-y", action="store_true")
    parser.add_argument("--workers", "-w", type=int, default=30)
    args = parser.parse_args()
    run(yes=args.yes, workers=args.workers)
