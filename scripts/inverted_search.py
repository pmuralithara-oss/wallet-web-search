"""Inverted search: find gambling disclosures mentioning Polymarket, then match to wallets."""
import json
import logging
import re
import sqlite3
import time
from pathlib import Path
from urllib.parse import quote_plus

import polars as pl
import requests
from bs4 import BeautifulSoup

DB_PATH = Path(__file__).parent.parent / "pipeline_social.db"
PARQUET_PATH = Path(__file__).parent.parent / "data" / "active_losers.parquet"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"

# Keyword queries — distress + polymarket combinations
QUERIES = [
    # Direct distress
    'polymarket "I lost"',
    'polymarket "lost everything"',
    'polymarket "blew up"',
    'polymarket "addicted"',
    'polymarket "gambling problem"',
    'polymarket "gambling addiction"',
    'polymarket "can\'t stop"',
    'polymarket "ruined me"',
    'polymarket "ruined my life"',
    'polymarket "lost my savings"',
    'polymarket "lost so much"',
    'polymarket "down bad"',
    'polymarket "in debt"',
    'polymarket "help me"',
    'polymarket "I\'m broke"',
    'polymarket "broke me"',
    'polymarket "destroyed"',
    'polymarket "regret"',
    'polymarket "biggest mistake"',
    'polymarket "never again"',
    'polymarket "lost money"',
    'polymarket "problem gambling"',
    'polymarket "gamblers anonymous"',
    'polymarket "compulsive"',
    'polymarket "withdrawal"',
    'polymarket "chasing losses"',
    'polymarket "degen"',
    'polymarket "degenerate"',
    'polymarket "rekt"',
    'polymarket "liquidated"',
    # Wallet-specific distress
    'polymarket wallet "I lost"',
    'polymarket "0x" "lost"',
    'polymarket "0x" "addicted"',
    'polymarket "0x" "gambling"',
    # Platform-specific
    'site:reddit.com polymarket lost money',
    'site:reddit.com polymarket gambling addiction',
    'site:reddit.com polymarket "I lost"',
    'site:reddit.com polymarket regret',
    'site:reddit.com polymarket "down bad"',
    'site:reddit.com polymarket degen rekt',
    'site:reddit.com polymarket "blew up"',
    'site:reddit.com r/problemgambling polymarket',
    'site:reddit.com r/gambling polymarket',
    'site:reddit.com r/GamblingAddiction polymarket',
    'site:reddit.com r/wallstreetbets polymarket lost',
    # Twitter
    'site:x.com polymarket "I lost"',
    'site:x.com polymarket addicted',
    'site:x.com polymarket "down bad"',
    'site:x.com polymarket rekt',
    # Forums
    'polymarket "prediction market" addiction',
    'polymarket "prediction market" "gambling disorder"',
    'polymarket "bet too much"',
    'polymarket "lost my shirt"',
    'polymarket "financial ruin"',
    'polymarket "life savings"',
    # Broader
    '"prediction market" "gambling addiction"',
    '"prediction market" "I lost everything"',
    '"prediction market" "problem gambling"',
    '"prediction market" "can\'t stop betting"',
    # Subreddit searches via Reddit API
]

REDDIT_QUERIES = [
    "polymarket lost",
    "polymarket addiction",
    "polymarket gambling",
    "polymarket regret",
    "polymarket blew up",
    "polymarket down bad",
    "polymarket rekt",
    "polymarket ruined",
    "polymarket broke",
    "polymarket degen",
    "polymarket help",
    "polymarket mistake",
    "polymarket savings",
    "polymarket debt",
    "polymarket chasing",
    "polymarket can't stop",
    "prediction market gambling",
    "prediction market addiction",
    "prediction market lost",
]

SUBREDDITS = [
    "problemgambling", "gambling", "GamblingAddiction", "Polymarket",
    "PolymarketTrading", "PolymarketHQ", "PredictionsMarkets",
    "wallstreetbets", "CryptoCurrency", "defi", "ethtrader",
    "personalfinance", "addiction", "selfimprovement",
]


def search_bing(query):
    """Search Bing, return list of results."""
    try:
        r = requests.get(
            f"https://www.bing.com/search?q={quote_plus(query)}&count=50",
            headers={"User-Agent": UA}, timeout=15,
        )
        if r.status_code != 200:
            return []
        soup = BeautifulSoup(r.text, "lxml")
        results = []
        for li in soup.select("li.b_algo"):
            a = li.select_one("h2 a")
            snip = li.select_one("p") or li.select_one("div.b_caption p")
            if a and a.get("href", "").startswith("http"):
                results.append({
                    "url": a["href"],
                    "title": a.text,
                    "snippet": snip.text if snip else "",
                    "source": "bing",
                    "query": query,
                })
        return results
    except Exception:
        return []


def search_reddit_api(query, subreddit=None):
    """Search Reddit API."""
    try:
        url = f"https://www.reddit.com/r/{subreddit}/search.json" if subreddit else "https://www.reddit.com/search.json"
        params = {"q": query, "limit": 100, "sort": "relevance", "t": "all"}
        if subreddit:
            params["restrict_sr"] = "on"
        r = requests.get(url, params=params,
                         headers={"User-Agent": "Academic-Research-PredictionMarket/1.0"}, timeout=10)
        if r.status_code != 200:
            return []
        posts = r.json().get("data", {}).get("children", [])
        results = []
        for p in posts:
            d = p.get("data", {})
            results.append({
                "url": f"https://reddit.com{d.get('permalink', '')}",
                "title": d.get("title", ""),
                "snippet": d.get("selftext", "")[:500],
                "subreddit": d.get("subreddit", ""),
                "author": d.get("author", ""),
                "score": d.get("score", 0),
                "source": "reddit",
                "query": query,
            })
        return results
    except Exception:
        return []


def extract_addresses(text):
    """Extract Ethereum addresses from text."""
    return list(set(re.findall(r"0x[a-fA-F0-9]{40}", text)))


def init_db():
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS inverted_hits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT,
            title TEXT,
            snippet TEXT,
            subreddit TEXT,
            author TEXT,
            score INTEGER,
            source TEXT,
            query TEXT,
            addresses_found TEXT,
            matched_wallet TEXT,
            created_at TEXT,
            UNIQUE(url, query)
        );
        CREATE INDEX IF NOT EXISTS idx_inv_matched ON inverted_hits(matched_wallet);
        CREATE INDEX IF NOT EXISTS idx_inv_url ON inverted_hits(url);
    """)
    conn.commit()
    return conn


def main():
    conn = init_db()

    # Load wallet set for matching
    df = pl.read_parquet(str(PARQUET_PATH))
    wallet_set = set(df["address"].str.to_lowercase().to_list())
    log.info(f"Loaded {len(wallet_set):,} wallet addresses for matching")

    all_results = []

    # 1. Bing keyword searches
    log.info(f"=== Bing searches: {len(QUERIES)} queries ===")
    for i, q in enumerate(QUERIES):
        results = search_bing(q)
        all_results.extend(results)
        log.info(f"  [{i+1}/{len(QUERIES)}] {q[:50]:50} => {len(results)} results")
        time.sleep(1)

    # 2. Reddit API searches (global)
    log.info(f"\n=== Reddit global searches: {len(REDDIT_QUERIES)} queries ===")
    for i, q in enumerate(REDDIT_QUERIES):
        results = search_reddit_api(q)
        all_results.extend(results)
        log.info(f"  [{i+1}/{len(REDDIT_QUERIES)}] {q[:50]:50} => {len(results)} results")
        time.sleep(1)

    # 3. Reddit subreddit-specific searches
    log.info(f"\n=== Reddit subreddit searches: {len(SUBREDDITS)} subs ===")
    for sub in SUBREDDITS:
        for q in ["polymarket", "prediction market", "lost money", "gambling"]:
            results = search_reddit_api(q, subreddit=sub)
            all_results.extend(results)
        log.info(f"  r/{sub}: searched 4 queries")
        time.sleep(1)

    # Dedupe by URL
    seen = set()
    unique = []
    for r in all_results:
        key = r["url"]
        if key not in seen:
            seen.add(key)
            unique.append(r)

    log.info(f"\n=== Total unique results: {len(unique):,} ===")

    # Extract addresses and match to wallet set
    matched = 0
    for r in unique:
        text = f"{r.get('title', '')} {r.get('snippet', '')} {r.get('url', '')}"
        addrs = extract_addresses(text)
        matched_addrs = [a.lower() for a in addrs if a.lower() in wallet_set]

        r["addresses_found"] = json.dumps(addrs) if addrs else None
        r["matched_wallet"] = matched_addrs[0] if matched_addrs else None

        if matched_addrs:
            matched += 1
            log.info(f"  WALLET MATCH: {matched_addrs[0]} in {r['url'][:60]}")

    # Store all results
    for r in unique:
        try:
            conn.execute(
                "INSERT OR IGNORE INTO inverted_hits (url, title, snippet, subreddit, author, score, source, query, addresses_found, matched_wallet, created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (r.get("url"), r.get("title"), r.get("snippet"), r.get("subreddit"),
                 r.get("author"), r.get("score", 0), r.get("source"), r.get("query"),
                 r.get("addresses_found"), r.get("matched_wallet"),
                 time.strftime("%Y-%m-%dT%H:%M:%SZ")),
            )
        except Exception:
            pass
    conn.commit()

    # Summary
    total = conn.execute("SELECT count(*) FROM inverted_hits").fetchone()[0]
    with_addr = conn.execute("SELECT count(*) FROM inverted_hits WHERE addresses_found IS NOT NULL").fetchone()[0]
    with_match = conn.execute("SELECT count(*) FROM inverted_hits WHERE matched_wallet IS NOT NULL").fetchone()[0]

    print("\n" + "=" * 60)
    print("INVERTED SEARCH RESULTS")
    print("=" * 60)
    print(f"  Total unique results:     {len(unique):,}")
    print(f"  Contains wallet address:  {with_addr}")
    print(f"  Matched to our wallets:   {with_match}")
    print(f"  Stored in DB:             {total:,}")

    # Show all results (for classification) — even those without wallet matches
    # These are posts about Polymarket + distress, valuable for the study
    print(f"\n=== DISTRESS POSTS (sample) ===")
    distress = conn.execute(
        "SELECT url, title, snippet, subreddit, author FROM inverted_hits WHERE snippet != '' ORDER BY score DESC LIMIT 30"
    ).fetchall()
    for r in distress:
        print(f"  [{r[3] or 'web'}] u/{r[4] or '?'}: {r[1][:80]}")
        print(f"    {r[0][:80]}")
        print(f"    {(r[2] or '')[:120]}")
        print()

    conn.close()


if __name__ == "__main__":
    main()
