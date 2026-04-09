"""Resolve Polymarket usernames for all selected wallets — parallel version."""
import logging
import re
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

from src.config import DB_PATH, LOGS_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOGS_DIR / "resolve_usernames.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


def resolve_one(address):
    try:
        resp = requests.get(
            "https://polymarket.com/profile/" + address,
            headers={"User-Agent": "Mozilla/5.0 (Academic Research)"},
            timeout=15,
        )
        if resp.status_code != 200:
            return address, None
        m = re.search(r'"username":"([^"]+)"', resp.text)
        if m and m.group(1).lower() != "username":
            return address, m.group(1)
        return address, None
    except Exception:
        return address, None


def run(workers=20):
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")

    rows = conn.execute(
        "SELECT address FROM wallets WHERE selected=1 AND username IS NULL ORDER BY loss_rank"
    ).fetchall()

    if not rows:
        log.info("All done.")
        conn.close()
        return

    addrs = [r[0] for r in rows]
    log.info(f"Resolving {len(addrs)} wallets with {workers} workers")

    resolved = 0
    done = 0

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(resolve_one, addr): addr for addr in addrs}

        batch = []
        for future in as_completed(futures):
            addr, username = future.result()
            done += 1
            if username:
                batch.append((username, addr))
                resolved += 1

            if len(batch) >= 100:
                conn.executemany("UPDATE wallets SET username=? WHERE address=?", batch)
                conn.commit()
                batch = []
                log.info(f"  {done}/{len(addrs)} ({resolved} resolved)")

        if batch:
            conn.executemany("UPDATE wallets SET username=? WHERE address=?", batch)
            conn.commit()

    conn.close()
    log.info(f"Done: {resolved}/{len(addrs)} resolved")
    print(f"Resolved: {resolved}/{len(addrs)}")


if __name__ == "__main__":
    run()
