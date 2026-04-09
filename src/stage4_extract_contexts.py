"""Stage 4: Extract context windows around wallet address/username mentions.

Strategy:
1. For fetched HTML pages: extract readable text, find username/address, get context window
2. For pages where HTML extraction failed (JS-rendered SPAs like Reddit/Twitter): use Brave snippet
3. For unfetched hits (errors, login walls): use Brave snippet as fallback
"""
import logging
import os
import re

from readability import Document
from bs4 import BeautifulSoup

from src.config import CONTEXTS_DIR, LOGS_DIR
from src.db import get_connection, init_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOGS_DIR / "stage4.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

CONTEXT_WINDOW = 500

# Minimum length for a context to be considered "real content" (not SVG/boilerplate)
MIN_CONTEXT_QUALITY = 50


def extract_text(html: str) -> str:
    """Extract readable text from HTML."""
    try:
        doc = Document(html)
        content_html = doc.summary()
        soup = BeautifulSoup(content_html, "lxml")
        return soup.get_text(separator="\n", strip=True)
    except Exception:
        try:
            soup = BeautifulSoup(html, "lxml")
            return soup.get_text(separator="\n", strip=True)
        except Exception:
            return ""


def find_contexts(text: str, needle: str, window: int = CONTEXT_WINDOW) -> list[str]:
    """Find all occurrences of needle in text and extract context windows."""
    contexts = []
    pattern = re.compile(re.escape(needle), re.IGNORECASE)
    for m in pattern.finditer(text):
        start = max(0, m.start() - window)
        end = min(len(text), m.end() + window)
        ctx = text[start:end]
        contexts.append(ctx)
    return contexts


def is_quality_context(ctx: str) -> bool:
    """Check if context is real text, not SVG paths or boilerplate."""
    # Filter out SVG path data
    if re.search(r'[MmZzLlHhVvCcSsQqTtAa]\d', ctx) and ctx.count(",") > 10:
        return False
    # Filter out mostly non-alpha
    alpha_ratio = sum(1 for c in ctx if c.isalpha()) / max(len(ctx), 1)
    if alpha_ratio < 0.3:
        return False
    # Must have some minimum readable content
    words = ctx.split()
    if len(words) < 8:
        return False
    return True


def run():
    init_db()
    conn = get_connection()

    # Process ALL priority >= 2 hits, not just fetched ones
    rows = conn.execute("""
        SELECT h.hit_id, h.address, h.raw_html_path, h.source_type,
               h.snippet, h.title, h.url, h.fetch_error, w.username
        FROM search_hits h
        JOIN wallets w ON h.address = w.address
        WHERE h.context_extracted=FALSE AND h.priority >= 2
        ORDER BY h.priority DESC
    """).fetchall()

    if not rows:
        log.info("No hits to extract contexts from.")
        conn.close()
        return

    log.info(f"Extracting contexts from {len(rows)} hits")

    stats = {"html_good": 0, "snippet_fallback": 0, "empty": 0}

    for i, row in enumerate(rows):
        hit_id = row["hit_id"]
        address = row["address"]
        username = row["username"]
        html_path = row["raw_html_path"]
        snippet = row["snippet"] or ""
        title = row["title"] or ""
        url = row["url"] or ""
        source_type = row["source_type"]

        context_text = None
        context_source = None

        # Try 1: Extract from fetched HTML
        if html_path and os.path.exists(html_path):
            try:
                with open(html_path, "r", encoding="utf-8", errors="replace") as f:
                    html = f.read()

                text = extract_text(html)

                needles = []
                if username:
                    needles.append(username)
                needles.append(address)

                for needle in needles:
                    contexts = find_contexts(text, needle)
                    quality = [c for c in contexts if is_quality_context(c)]
                    if quality:
                        context_text = f"\n{'='*60}\n".join(quality)
                        context_source = "html"
                        break

            except Exception as e:
                log.warning(f"[{hit_id}] HTML read error: {e}")

        # Try 2: Fall back to Brave snippet
        if not context_text and snippet:
            # Build context from title + snippet + URL
            fallback = f"Source: {url}\nTitle: {title}\n\n{snippet}"
            if len(snippet) > 20:
                context_text = fallback
                context_source = "snippet"

        # Write context file
        ctx_path = CONTEXTS_DIR / f"{hit_id}.txt"
        if context_text:
            with open(ctx_path, "w", encoding="utf-8") as f:
                f.write(context_text)
            if context_source == "html":
                stats["html_good"] += 1
            else:
                stats["snippet_fallback"] += 1
        else:
            with open(ctx_path, "w", encoding="utf-8") as f:
                f.write("")
            stats["empty"] += 1

        conn.execute(
            "UPDATE search_hits SET context_extracted=TRUE, context_path=? WHERE hit_id=?",
            (str(ctx_path), hit_id),
        )
        conn.commit()

        if (i + 1) % 20 == 0:
            log.info(f"  Processed {i+1}/{len(rows)}")

    conn.close()

    total = stats["html_good"] + stats["snippet_fallback"]
    log.info(f"Done: {stats['html_good']} from HTML, {stats['snippet_fallback']} from snippets, {stats['empty']} empty")
    print(f"\nStage 4 complete:")
    print(f"  Context from HTML:     {stats['html_good']}")
    print(f"  Context from snippet:  {stats['snippet_fallback']}")
    print(f"  Empty (no context):    {stats['empty']}")
    print(f"  Total classifiable:    {total}")


if __name__ == "__main__":
    run()
