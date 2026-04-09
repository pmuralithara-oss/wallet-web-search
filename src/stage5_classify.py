"""Stage 5: Classify contexts using DeepSeek API (OpenAI-compatible). 200 concurrent."""
import argparse
import json
import logging
import os
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests as req

from src.config import CLASSIFIER_PROMPT_PATH, CLASSIFICATION_PROMPTS_DIR, CLASSIFICATION_RESULTS_DIR, LOGS_DIR, DB_PATH

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOGS_DIR / "stage5.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

DEEPSEEK_API_KEY = "sk-9ac0e21c0b4a4c87bc064ef8ae112c3b"
DEEPSEEK_URL = "https://api.deepseek.com/v1/chat/completions"


def build_prompt(template, address, url, source_type, context):
    return (
        template
        .replace("{ADDRESS}", address)
        .replace("{URL}", url)
        .replace("{SOURCE_TYPE}", source_type)
        .replace("{CONTEXT_TEXT}", context)
    )


def parse_json_response(text):
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        text = "\n".join(lines[1:])
        if text.endswith("```"):
            text = text[:-3]
        text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # Try to find JSON object in the text
        start = text.find("{")
        end = text.rfind("}") + 1
        if start >= 0 and end > start:
            try:
                return json.loads(text[start:end])
            except json.JSONDecodeError:
                return None
        return None


def classify_one(hit_id, prompt_text, result_path):
    """Call DeepSeek API for one classification. Returns (hit_id, parsed_dict or None)."""
    try:
        resp = req.post(
            DEEPSEEK_URL,
            headers={
                "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": "deepseek-chat",
                "messages": [{"role": "user", "content": prompt_text}],
                "temperature": 0.1,
                "max_tokens": 1000,
            },
            timeout=60,
        )

        if resp.status_code != 200:
            log.warning(f"[{hit_id}] DeepSeek HTTP {resp.status_code}: {resp.text[:200]}")
            return hit_id, None

        data = resp.json()
        raw = data["choices"][0]["message"]["content"]

        with open(result_path, "w") as f:
            f.write(raw)

        parsed = parse_json_response(raw)
        if parsed:
            return hit_id, parsed

        # Retry once
        log.warning(f"[{hit_id}] JSON parse failed, retrying...")
        retry_resp = req.post(
            DEEPSEEK_URL,
            headers={
                "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": "deepseek-chat",
                "messages": [
                    {"role": "user", "content": prompt_text},
                    {"role": "assistant", "content": raw},
                    {"role": "user", "content": "That was not valid JSON. Return ONLY the JSON object, no preamble, no markdown fences."},
                ],
                "temperature": 0.0,
                "max_tokens": 1000,
            },
            timeout=60,
        )
        if retry_resp.status_code == 200:
            raw2 = retry_resp.json()["choices"][0]["message"]["content"]
            with open(result_path, "w") as f:
                f.write(raw2)
            return hit_id, parse_json_response(raw2)

        return hit_id, None

    except Exception as e:
        log.warning(f"[{hit_id}] error: {e}")
        return hit_id, None


def run(yes=False, workers=200):
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")

    with open(CLASSIFIER_PROMPT_PATH) as f:
        template = f.read()

    rows = conn.execute("""
        SELECT h.hit_id, h.address, h.url, h.source_type, h.context_path
        FROM search_hits h
        WHERE h.context_extracted=1 AND h.classified=0 AND h.context_path IS NOT NULL
        ORDER BY h.priority DESC
    """).fetchall()

    classifiable = []
    for row in rows:
        ctx_path = row["context_path"]
        if ctx_path and os.path.exists(ctx_path) and os.path.getsize(ctx_path) > 0:
            classifiable.append(dict(row))

    if not classifiable:
        log.info("No hits to classify.")
        conn.close()
        return

    log.info(f"Found {len(classifiable)} hits to classify with {workers} workers")

    if not yes:
        print(f"\nAbout to classify {len(classifiable)} hits via DeepSeek API ({workers} parallel). Proceed? [y/N]")
        if input().strip().lower() != "y":
            conn.close()
            return

    # Build all prompts
    tasks = []
    for row in classifiable:
        hit_id = row["hit_id"]
        with open(row["context_path"]) as f:
            context = f.read()
        prompt = build_prompt(template, row["address"], row["url"], row["source_type"], context)
        prompt_path = str(CLASSIFICATION_PROMPTS_DIR / f"{hit_id}.txt")
        with open(prompt_path, "w") as f:
            f.write(prompt)
        result_path = str(CLASSIFICATION_RESULTS_DIR / f"{hit_id}.json")
        tasks.append((hit_id, prompt, result_path, row))

    stats = {
        "total": 0, "yes_yes": 0, "errors": 0,
        "disclosure": {"yes": 0, "no": 0, "unclear": 0},
        "owner": {"yes": 0, "no": 0, "unclear": 0},
        "review": 0,
    }
    done = 0

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(classify_one, hit_id, prompt, rp): (hit_id, row)
            for hit_id, prompt, rp, row in tasks
        }

        batch = []
        for future in as_completed(futures):
            hit_id, row = futures[future]
            done += 1
            _, parsed = future.result()

            if parsed is None:
                batch.append(("UPDATE search_hits SET classified=1, classification_error=? WHERE hit_id=?",
                              ("parse_error", hit_id)))
                stats["errors"] += 1
            else:
                disclosure = parsed.get("is_first_person_disclosure", "unclear")
                owner = parsed.get("is_wallet_owner_the_speaker", "unclear")
                evidence = json.dumps(parsed.get("disclosure_evidence", []))
                emotional = parsed.get("emotional_context", "neutral")
                confidence = parsed.get("confidence", 0.0)
                review = parsed.get("needs_human_review", True)
                reasoning = parsed.get("reasoning", "")

                batch.append((
                    """INSERT OR REPLACE INTO classifications
                       (hit_id, address, is_first_person_disclosure, is_wallet_owner_the_speaker,
                        disclosure_evidence, emotional_context, confidence, needs_human_review,
                        reasoning, raw_response, classified_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (hit_id, row["address"], disclosure, owner, evidence, emotional,
                     confidence, review, reasoning, json.dumps(parsed),
                     datetime.now(timezone.utc).isoformat()),
                ))
                batch.append(("UPDATE search_hits SET classified=1 WHERE hit_id=?", (hit_id,)))

                stats["total"] += 1
                stats["disclosure"][disclosure] = stats["disclosure"].get(disclosure, 0) + 1
                stats["owner"][owner] = stats["owner"].get(owner, 0) + 1
                if disclosure == "yes" and owner == "yes":
                    stats["yes_yes"] += 1
                if review:
                    stats["review"] += 1

            if len(batch) >= 100 or done == len(tasks):
                for sql, params in batch:
                    conn.execute(sql, params)
                conn.commit()
                batch = []
                log.info(f"  {done}/{len(tasks)} classified, {stats['yes_yes']} ground-truth, {stats['errors']} errors")

    conn.close()

    print("\n" + "=" * 70)
    print("STAGE 5 REPORT: Classification (DeepSeek)")
    print("=" * 70)
    print(f"  Total classified:         {stats['total']}")
    print(f"  Parse errors:             {stats['errors']}")
    print(f"\n  is_first_person_disclosure:")
    for k, v in stats["disclosure"].items():
        print(f"    {k:10} {v:>5}")
    print(f"\n  is_wallet_owner_the_speaker:")
    for k, v in stats["owner"].items():
        print(f"    {k:10} {v:>5}")
    print(f"\n  Ground-truth hits (yes/yes): {stats['yes_yes']}")
    print(f"  Flagged for review:          {stats['review']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--yes", "-y", action="store_true")
    parser.add_argument("--workers", "-w", type=int, default=200)
    args = parser.parse_args()
    run(yes=args.yes, workers=args.workers)
