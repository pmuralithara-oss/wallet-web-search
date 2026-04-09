#!/usr/bin/env python3
"""Quick connectivity test for Brave Search API and claude CLI."""
import subprocess
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from src.config import BRAVE_API_KEY

import requests

print("=" * 60)
print("CONNECTIVITY TEST")
print("=" * 60)

# Test 1: Brave Search API
print("\n[1/2] Brave Search API...")
if not BRAVE_API_KEY:
    print("  FAIL: BRAVE_API_KEY not set in .env")
    sys.exit(1)

resp = requests.get(
    "https://api.search.brave.com/res/v1/web/search",
    headers={
        "X-Subscription-Token": BRAVE_API_KEY,
        "Accept": "application/json",
    },
    params={"q": "polymarket wallet 0x", "count": 3},
    timeout=15,
)
print(f"  Status: {resp.status_code}")
if resp.status_code == 200:
    data = resp.json()
    n = len(data.get("web", {}).get("results", []))
    print(f"  Results: {n}")
    print("  PASS")
else:
    print(f"  Body: {resp.text[:200]}")
    print("  FAIL")
    sys.exit(1)

# Test 2: claude CLI
print("\n[2/2] claude CLI...")
result = subprocess.run(
    ["claude", "-p", "--model", "sonnet", "respond with just: ok"],
    capture_output=True, text=True, timeout=30,
)
output = result.stdout.strip()
print(f"  Exit code: {result.returncode}")
print(f"  Output: {output}")
if result.returncode == 0 and "ok" in output.lower():
    print("  PASS")
else:
    print(f"  Stderr: {result.stderr[:200]}")
    print("  FAIL")
    sys.exit(1)

print("\n" + "=" * 60)
print("ALL TESTS PASSED")
print("=" * 60)
