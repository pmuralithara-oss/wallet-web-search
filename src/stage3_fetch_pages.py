"""Stage 3: Fetch HTML pages for priority >= 2 search hits."""
import argparse
import logging
import time
from collections import defaultdict
from datetime import datetime, timezone

import requests

from src.config import FETCHED_PAGES_DIR, LOGS_DIR
from src.db import get_connection, init_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOGS_DIR / "stage3.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

USER_AGENT = "Academic-Research-PredictionMarket-Ethics/1.0"


def run(min_priority: int = 2, yes: bool = False):
    init_db()
    conn = get_connection()

    rows = conn.execute(
        "SELECT hit_id, address, url, source_type FROM search_hits WHERE fetched=FALSE AND priority >= ? ORDER BY priority DESC",
        (min_priority,),
    ).fetchall()

    if not rows:
        log.info("No unfetched hits to process.")
        conn.close()
        return

    log.info(f"Found {len(rows)} hits to fetch (priority >= {min_priority})")

    if not yes:
        print(f"\nAbout to fetch {len(rows)} URLs. Proceed? [y/N]")
        if input().strip().lower() != "y":
            print("Aborted.")
            conn.close()
            return

    domain_last_request = defaultdict(float)
    stats = {"success": 0, "error": 0, "errors_by_type": defaultdict(int)}

    for i, row in enumerate(rows):
        hit_id = row["hit_id"]
        url = row["url"]
        source_type = row["source_type"]

        # Per-domain rate limit: 1s between requests to same domain
        try:
            from urllib.parse import urlparse
            domain = urlparse(url).netloc.lower()
        except Exception:
            domain = "unknown"

        elapsed = time.time() - domain_last_request[domain]
        if elapsed < 1.0:
            time.sleep(1.0 - elapsed)

        log.info(f"[{i+1}/{len(rows)}] [{source_type}] {url[:80]}")

        try:
            resp = requests.get(
                url,
                headers={"User-Agent": USER_AGENT},
                timeout=20,
                allow_redirects=True,
            )
            domain_last_request[domain] = time.time()

            if resp.status_code == 200:
                out_path = FETCHED_PAGES_DIR / f"{hit_id}.html"
                with open(out_path, "w", encoding="utf-8", errors="replace") as f:
                    f.write(resp.text)

                conn.execute(
                    "UPDATE search_hits SET fetched=TRUE, fetched_at=?, raw_html_path=? WHERE hit_id=?",
                    (datetime.now(timezone.utc).isoformat(), str(out_path), hit_id),
                )
                stats["success"] += 1
                log.info(f"  OK ({len(resp.text):,} chars)")
            else:
                error_msg = f"HTTP {resp.status_code}"
                conn.execute(
                    "UPDATE search_hits SET fetched=TRUE, fetched_at=?, fetch_error=? WHERE hit_id=?",
                    (datetime.now(timezone.utc).isoformat(), error_msg, hit_id),
                )
                stats["error"] += 1
                stats["errors_by_type"][error_msg] += 1
                log.warning(f"  {error_msg}")

        except requests.exceptions.Timeout:
            conn.execute(
                "UPDATE search_hits SET fetched=TRUE, fetched_at=?, fetch_error=? WHERE hit_id=?",
                (datetime.now(timezone.utc).isoformat(), "timeout", hit_id),
            )
            stats["error"] += 1
            stats["errors_by_type"]["timeout"] += 1
            log.warning(f"  Timeout")

        except Exception as e:
            error_msg = str(e)[:200]
            conn.execute(
                "UPDATE search_hits SET fetched=TRUE, fetched_at=?, fetch_error=? WHERE hit_id=?",
                (datetime.now(timezone.utc).isoformat(), error_msg, hit_id),
            )
            stats["error"] += 1
            stats["errors_by_type"]["exception"] += 1
            log.warning(f"  Error: {error_msg}")

        conn.commit()

    conn.close()

    # Disk usage
    import os
    total_bytes = sum(
        os.path.getsize(FETCHED_PAGES_DIR / f)
        for f in os.listdir(FETCHED_PAGES_DIR)
        if f.endswith(".html")
    )

    print("\n" + "=" * 70)
    print("STAGE 3 REPORT: Fetch Pages")
    print("=" * 70)
    print(f"  Total fetch attempts: {len(rows)}")
    print(f"  Successful:           {stats['success']}")
    print(f"  Errors:               {stats['error']}")
    if stats["errors_by_type"]:
        print(f"\n  Errors by type:")
        for etype, count in sorted(stats["errors_by_type"].items(), key=lambda x: -x[1]):
            print(f"    {etype:30} {count:>5}")
    print(f"\n  Disk usage:           {total_bytes / 1024 / 1024:.1f} MB")
    print(f"\nStage 3 complete. Review before proceeding to Stage 4.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stage 3: Fetch pages for search hits")
    parser.add_argument("--min-priority", type=int, default=2, help="Minimum priority to fetch")
    parser.add_argument("--yes", "-y", action="store_true", help="Skip confirmation")
    args = parser.parse_args()
    run(min_priority=args.min_priority, yes=args.yes)
