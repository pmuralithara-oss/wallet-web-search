"""GitHub Actions worker: check Polymarket usernames against Reddit."""
import json
import logging
import re
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import polars as pl
import requests

CHUNK_ID = int(sys.argv[1])
TOTAL_CHUNKS = int(sys.argv[2])

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def resolve_username(address):
    try:
        resp = requests.get(f"https://polymarket.com/profile/{address}",
            headers={"User-Agent": "Mozilla/5.0 (Academic Research)"}, timeout=15)
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


def is_real_handle(u):
    if not u: return False
    if u.startswith("0x"): return False
    if len(u) > 25 or len(u) < 3: return False
    if re.match(r"^[0-9a-f]{15,}", u): return False
    if re.search(r"\d{8,}", u): return False
    if sum(1 for c in u if c.isalpha()) < 2: return False
    return True


def check_reddit(username):
    try:
        r = requests.get(f"https://www.reddit.com/user/{username}/about.json",
            headers={"User-Agent": f"AcademicResearch:v1.0.{CHUNK_ID}"}, timeout=10)
        if r.status_code == 200:
            d = r.json().get("data", {})
            return username, True, d.get("total_karma", 0)
        return username, False, 0
    except:
        return username, False, 0


def main():
    df = pl.read_parquet("data/active_losers.parquet").sort("estimated_loss")
    all_addrs = df["address"].to_list()

    chunk_size = len(all_addrs) // TOTAL_CHUNKS
    start = CHUNK_ID * chunk_size
    end = start + chunk_size if CHUNK_ID < TOTAL_CHUNKS - 1 else len(all_addrs)
    my_addrs = all_addrs[start:end]
    log.info(f"Chunk {CHUNK_ID}: {len(my_addrs):,} addresses")

    db = sqlite3.connect(f"reddit_chunk_{CHUNK_ID}.db")
    db.execute("CREATE TABLE IF NOT EXISTS results (address TEXT PRIMARY KEY, username TEXT, bio TEXT, reddit_exists INT, reddit_karma INT)")
    db.commit()

    done = set(r[0] for r in db.execute("SELECT address FROM results").fetchall())
    remaining = [a for a in my_addrs if a not in done]
    log.info(f"Done: {len(done):,}, remaining: {len(remaining):,}")

    # Step 1: Resolve usernames (100 parallel)
    log.info("Resolving usernames...")
    resolved = {}
    t0 = time.time()
    CHUNK_SZ = 500
    for i in range(0, len(remaining), CHUNK_SZ):
        chunk = remaining[i:i + CHUNK_SZ]
        with ThreadPoolExecutor(max_workers=100) as ex:
            for f in as_completed({ex.submit(resolve_username, a): a for a in chunk}):
                addr, username, bio = f.result()
                resolved[addr] = (username, bio)
        if (i + CHUNK_SZ) % 5000 == 0:
            log.info(f"  Resolved {i + CHUNK_SZ:,}/{len(remaining):,}")

    log.info(f"Resolved {sum(1 for v in resolved.values() if v[0]):,} usernames in {time.time()-t0:.0f}s")

    # Step 2: Check Reddit (10 parallel, 1/sec rate limit)
    real_handles = [(addr, u, bio) for addr, (u, bio) in resolved.items() if is_real_handle(u)]
    log.info(f"Checking {len(real_handles):,} real handles on Reddit...")

    matches = 0
    batch = []
    t0 = time.time()

    for i in range(0, len(real_handles), 50):
        chunk = real_handles[i:i + 50]
        with ThreadPoolExecutor(max_workers=10) as ex:
            futs = {ex.submit(check_reddit, u): (addr, u, bio) for addr, u, bio in chunk}
            for f in as_completed(futs):
                addr, u, bio = futs[f]
                _, exists, karma = f.result()
                if exists:
                    matches += 1
                    log.info(f"  REDDIT MATCH: u/{u} karma={karma:,} wallet={addr[:20]}...")
                batch.append((addr, u, bio, 1 if exists else 0, karma))
        time.sleep(1)  # respect rate limit

        if len(batch) >= 500:
            db.executemany("INSERT OR IGNORE INTO results VALUES (?,?,?,?,?)", batch)
            db.commit()
            elapsed = time.time() - t0
            log.info(f"  {i+50:,}/{len(real_handles):,} checked, {matches} Reddit matches")
            batch = []

    # Also store non-real handles with reddit_exists=0
    for addr in remaining:
        if addr not in dict((b[0], True) for b in batch):
            u, bio = resolved.get(addr, (None, None))
            if not is_real_handle(u):
                batch.append((addr, u, bio, 0, 0))

    if batch:
        db.executemany("INSERT OR IGNORE INTO results VALUES (?,?,?,?,?)", batch)
        db.commit()

    log.info(f"Chunk {CHUNK_ID} done: {matches} Reddit matches out of {len(real_handles):,} checked")
    db.close()


if __name__ == "__main__":
    main()
