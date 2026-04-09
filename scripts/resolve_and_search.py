"""
Resolve Polymarket usernames + search exact wallet addresses via Brave.
Runs on GPU server. Saves results to SQLite.

Step 1: Resolve all wallet addresses to Polymarket usernames + bios (100 parallel)
Step 2: Search exact wallet address on Brave (30 parallel, ~50 QPS)
Step 3: Reddit handle check for resolved usernames (50 parallel)
"""
import json
import logging
import os
import re
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import requests

# Config
DB_PATH = Path(__file__).parent.parent / "pipeline.db"
DATA_DIR = Path(__file__).parent.parent / "data"
BRAVE_API_KEY = "BSAInM34_IPf8iRIQJ14p-9hnabTjVy"
PARQUET_PATH = DATA_DIR / "active_losers.parquet"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger(__name__)


def get_db():
    conn = sqlite3.connect(str(DB_PATH), timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=60000")
    return conn


def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS profiles (
            address TEXT PRIMARY KEY,
            username TEXT,
            bio TEXT,
            social_links TEXT,
            resolved_at TEXT
        );
        CREATE TABLE IF NOT EXISTS address_search (
            address TEXT PRIMARY KEY,
            has_social_hit INTEGER DEFAULT 0,
            hit_urls TEXT,
            hit_titles TEXT,
            hit_snippets TEXT,
            searched_at TEXT,
            error TEXT
        );
        CREATE TABLE IF NOT EXISTS reddit_matches (
            username TEXT PRIMARY KEY,
            address TEXT,
            reddit_exists INTEGER,
            reddit_karma INTEGER,
            reddit_created TEXT,
            checked_at TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_profiles_username ON profiles(username);
        CREATE INDEX IF NOT EXISTS idx_address_search_social ON address_search(has_social_hit);
    """)
    conn.commit()
    conn.close()


# ============ STEP 1: Resolve Polymarket profiles ============

def resolve_profile(address):
    """Scrape polymarket.com/profile/{address} for username + bio."""
    try:
        resp = requests.get(
            f"https://polymarket.com/profile/{address}",
            headers={"User-Agent": "Mozilla/5.0 (Academic Research)"},
            timeout=15,
        )
        if resp.status_code != 200:
            return address, None, None, None

        username_m = re.search(r'"username":"([^"]+)"', resp.text)
        username = username_m.group(1) if username_m else None
        if username and username.lower() == "username":
            username = None

        bio_m = re.search(r'"bio":"([^"]*)"', resp.text)
        bio = bio_m.group(1) if bio_m else ""

        # Extract social links from bio
        social_links = []
        # Twitter/X
        tw = re.findall(r'(?:twitter\.com|x\.com)/(\w+)', bio, re.IGNORECASE)
        for t in tw:
            social_links.append(f"twitter:{t}")
        # Reddit
        rd = re.findall(r'reddit\.com/u(?:ser)?/(\w+)', bio, re.IGNORECASE)
        for r in rd:
            social_links.append(f"reddit:{r}")
        # Generic @handles
        handles = re.findall(r'@(\w{3,20})', bio)
        for h in handles:
            social_links.append(f"handle:{h}")

        return address, username, bio, json.dumps(social_links) if social_links else None

    except Exception:
        return address, None, None, None


def step1_resolve_profiles(workers=100):
    log.info("=== STEP 1: Resolve Polymarket profiles ===")
    conn = get_db()

    # Load addresses
    import polars as pl
    df = pl.read_parquet(str(PARQUET_PATH))
    all_addrs = df["address"].to_list()
    log.info(f"Total addresses: {len(all_addrs):,}")

    # Skip already resolved
    done = set(r[0] for r in conn.execute("SELECT address FROM profiles").fetchall())
    remaining = [a for a in all_addrs if a not in done]
    log.info(f"Already resolved: {len(done):,}, remaining: {len(remaining):,}")

    if not remaining:
        conn.close()
        return

    resolved = 0
    total_done = 0

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(resolve_profile, addr): addr for addr in remaining}
        batch = []

        for future in as_completed(futures):
            address, username, bio, social_links = future.result()
            total_done += 1

            if username:
                resolved += 1

            batch.append((address, username, bio, social_links,
                         datetime.now(timezone.utc).isoformat()))

            if len(batch) >= 500:
                conn.executemany(
                    "INSERT OR IGNORE INTO profiles (address, username, bio, social_links, resolved_at) VALUES (?,?,?,?,?)",
                    batch,
                )
                conn.commit()
                log.info(f"  {total_done:,}/{len(remaining):,} done ({resolved:,} with username)")
                batch = []

        if batch:
            conn.executemany(
                "INSERT OR IGNORE INTO profiles (address, username, bio, social_links, resolved_at) VALUES (?,?,?,?,?)",
                batch,
            )
            conn.commit()

    conn.close()
    log.info(f"Step 1 complete: {resolved:,}/{len(remaining):,} usernames resolved")


# ============ STEP 2: Brave exact address search ============

SOCIAL_DOMAINS = {
    "reddit.com", "twitter.com", "x.com", "youtube.com", "medium.com",
    "substack.com", "mirror.xyz", "warpcast.com", "farcaster.xyz",
    "4plebs.org", "boards.4chan.org", "t.me", "discord.gg",
}

def is_social_url(url):
    try:
        from urllib.parse import urlparse
        domain = urlparse(url).netloc.lower().replace("www.", "").replace("old.", "")
        return any(sd in domain for sd in SOCIAL_DOMAINS)
    except Exception:
        return False


def brave_search_address(address):
    """Search Brave for exact wallet address. Returns social hits."""
    try:
        resp = requests.get(
            "https://api.search.brave.com/res/v1/web/search",
            headers={
                "X-Subscription-Token": BRAVE_API_KEY,
                "Accept": "application/json",
            },
            params={"q": f'"{address}"', "count": 20, "safesearch": "off"},
            timeout=20,
        )
        if resp.status_code == 429:
            time.sleep(2)
            resp = requests.get(
                "https://api.search.brave.com/res/v1/web/search",
                headers={
                    "X-Subscription-Token": BRAVE_API_KEY,
                    "Accept": "application/json",
                },
                params={"q": f'"{address}"', "count": 20, "safesearch": "off"},
                timeout=20,
            )
        if resp.status_code != 200:
            return address, False, None, None, None, f"HTTP {resp.status_code}"

        results = resp.json().get("web", {}).get("results", [])
        social_hits = [r for r in results if is_social_url(r.get("url", ""))]

        if social_hits:
            urls = [h["url"] for h in social_hits]
            titles = [h.get("title", "") for h in social_hits]
            snippets = [h.get("description", "") for h in social_hits]
            return address, True, json.dumps(urls), json.dumps(titles), json.dumps(snippets), None
        else:
            return address, False, None, None, None, None

    except requests.exceptions.Timeout:
        return address, False, None, None, None, "timeout"
    except Exception as e:
        return address, False, None, None, None, str(e)[:200]


def step2_brave_search(workers=30):
    log.info("=== STEP 2: Brave exact address search ===")
    conn = get_db()

    # Get all addresses not yet searched
    import polars as pl
    df = pl.read_parquet(str(PARQUET_PATH))
    all_addrs = df["address"].to_list()

    done = set(r[0] for r in conn.execute("SELECT address FROM address_search").fetchall())
    remaining = [a for a in all_addrs if a not in done]
    log.info(f"Total: {len(all_addrs):,}, already searched: {len(done):,}, remaining: {len(remaining):,}")

    if not remaining:
        conn.close()
        return

    social_hits = 0
    total_done = 0
    errors = 0

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(brave_search_address, addr): addr for addr in remaining}
        batch = []

        for future in as_completed(futures):
            address, has_social, urls, titles, snippets, error = future.result()
            total_done += 1

            if has_social:
                social_hits += 1
            if error:
                errors += 1

            batch.append((address, 1 if has_social else 0, urls, titles, snippets,
                         datetime.now(timezone.utc).isoformat(), error))

            if len(batch) >= 500:
                conn.executemany(
                    "INSERT OR IGNORE INTO address_search (address, has_social_hit, hit_urls, hit_titles, hit_snippets, searched_at, error) VALUES (?,?,?,?,?,?,?)",
                    batch,
                )
                conn.commit()
                log.info(f"  {total_done:,}/{len(remaining):,} searched, {social_hits:,} social hits, {errors} errors")
                batch = []

        if batch:
            conn.executemany(
                "INSERT OR IGNORE INTO address_search (address, has_social_hit, hit_urls, hit_titles, hit_snippets, searched_at, error) VALUES (?,?,?,?,?,?,?)",
                batch,
            )
            conn.commit()

    conn.close()
    log.info(f"Step 2 complete: {social_hits:,} social hits out of {len(remaining):,} searched")


# ============ STEP 3: Reddit handle check ============

def check_reddit(username, address):
    """Check if a Reddit user exists with this username."""
    try:
        resp = requests.get(
            f"https://www.reddit.com/user/{username}/about.json",
            headers={"User-Agent": "Academic-Research-PredictionMarket/1.0"},
            timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json().get("data", {})
            karma = data.get("total_karma", 0)
            created = data.get("created_utc", 0)
            return username, address, True, karma, created
        else:
            return username, address, False, 0, 0
    except Exception:
        return username, address, False, 0, 0


def step3_reddit_check(workers=50):
    log.info("=== STEP 3: Reddit handle check ===")
    conn = get_db()

    # Get resolved usernames not yet checked
    rows = conn.execute("""
        SELECT p.username, p.address FROM profiles p
        LEFT JOIN reddit_matches r ON p.username = r.username
        WHERE p.username IS NOT NULL AND r.username IS NULL
    """).fetchall()

    log.info(f"Usernames to check on Reddit: {len(rows):,}")

    if not rows:
        conn.close()
        return

    matches = 0
    total_done = 0

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(check_reddit, r[0], r[1]): r for r in rows}
        batch = []

        for future in as_completed(futures):
            username, address, exists, karma, created = future.result()
            total_done += 1

            if exists:
                matches += 1

            created_str = datetime.fromtimestamp(created, tz=timezone.utc).isoformat() if created else None
            batch.append((username, address, 1 if exists else 0, karma, created_str,
                         datetime.now(timezone.utc).isoformat()))

            if len(batch) >= 500:
                conn.executemany(
                    "INSERT OR IGNORE INTO reddit_matches (username, address, reddit_exists, reddit_karma, reddit_created, checked_at) VALUES (?,?,?,?,?,?)",
                    batch,
                )
                conn.commit()
                log.info(f"  {total_done:,}/{len(rows):,} checked, {matches:,} Reddit matches")
                batch = []

        if batch:
            conn.executemany(
                "INSERT OR IGNORE INTO reddit_matches (username, address, reddit_exists, reddit_karma, reddit_created, checked_at) VALUES (?,?,?,?,?,?)",
                batch,
            )
            conn.commit()

    conn.close()
    log.info(f"Step 3 complete: {matches:,} Reddit matches out of {len(rows):,}")


# ============ MAIN ============

if __name__ == "__main__":
    init_db()

    step = sys.argv[1] if len(sys.argv) > 1 else "all"

    if step in ("1", "all"):
        step1_resolve_profiles()
    if step in ("2", "all"):
        step2_brave_search()
    if step in ("3", "all"):
        step3_reddit_check()

    # Summary
    conn = get_db()
    profiles = conn.execute("SELECT count(*) FROM profiles WHERE username IS NOT NULL").fetchone()[0]
    social = conn.execute("SELECT count(*) FROM address_search WHERE has_social_hit=1").fetchone()[0]
    reddit = conn.execute("SELECT count(*) FROM reddit_matches WHERE reddit_exists=1").fetchone()[0]
    conn.close()

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"  Polymarket usernames resolved: {profiles:,}")
    print(f"  Wallet addresses with social hits: {social:,}")
    print(f"  Reddit account matches: {reddit:,}")
