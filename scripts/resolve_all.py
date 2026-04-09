"""Resolve all 739K Polymarket usernames. 200 parallel workers."""
import logging
import re
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import polars as pl
import requests

DB_PATH = Path(__file__).parent.parent / "pipeline_social.db"
PARQUET_PATH = Path(__file__).parent.parent / "data" / "active_losers.parquet"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def resolve_one(address):
    try:
        resp = requests.get(
            f"https://polymarket.com/profile/{address}",
            headers={"User-Agent": "Mozilla/5.0 (Academic Research)"},
            timeout=15,
        )
        if resp.status_code != 200:
            return address, None, None
        m = re.search(r'"username":"([^"]+)"', resp.text)
        username = m.group(1) if m else None
        if username and username.lower() == "username":
            username = None
        bio_m = re.search(r'"bio":"([^"]*)"', resp.text)
        bio = bio_m.group(1) if bio_m else ""
        return address, username, bio
    except:
        return address, None, None


def main(workers=200):
    conn = sqlite3.connect(str(DB_PATH), timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=60000")
    conn.execute("""CREATE TABLE IF NOT EXISTS profiles (
        address TEXT PRIMARY KEY, username TEXT, bio TEXT, social_links TEXT, resolved_at TEXT
    )""")
    conn.commit()

    df = pl.read_parquet(str(PARQUET_PATH))
    all_addrs = df.sort("estimated_loss")["address"].to_list()

    done = set(r[0] for r in conn.execute("SELECT address FROM profiles").fetchall())
    remaining = [a for a in all_addrs if a not in done]
    log.info(f"Total: {len(all_addrs):,}, done: {len(done):,}, remaining: {len(remaining):,}")

    if not remaining:
        return

    resolved = 0
    total = 0
    t0 = time.time()

    CHUNK = 2000
    for i in range(0, len(remaining), CHUNK):
        chunk = remaining[i:i + CHUNK]
        batch = []
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = {ex.submit(resolve_one, a): a for a in chunk}
            for f in as_completed(futs):
                addr, username, bio = f.result()
                total += 1
                if username:
                    resolved += 1
                batch.append((addr, username, bio, None, time.strftime("%Y-%m-%dT%H:%M:%SZ")))

        conn.executemany("INSERT OR IGNORE INTO profiles VALUES (?,?,?,?,?)", batch)
        conn.commit()
        elapsed = time.time() - t0
        rate = total / max(elapsed, 1)
        eta = (len(remaining) - total) / max(rate, 0.1) / 60
        log.info(f"  {total:,}/{len(remaining):,} | {resolved:,} resolved | {rate:.0f}/s | ETA: {eta:.0f}min")

    log.info(f"Done: {resolved:,}/{total:,} resolved in {(time.time()-t0)/60:.0f}min")
    conn.close()


if __name__ == "__main__":
    main()
