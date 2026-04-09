"""Central configuration for the wallet web search pipeline."""
from pathlib import Path
from dotenv import load_dotenv
import os

PROJECT_ROOT = Path(__file__).resolve().parent.parent

load_dotenv(PROJECT_ROOT / ".env")

BRAVE_API_KEY = os.getenv("BRAVE_API_KEY", "")

DB_PATH = PROJECT_ROOT / "pipeline.db"

DATA_DIR = PROJECT_ROOT / "data"
SEARCH_RESULTS_DIR = DATA_DIR / "search_results"
FETCHED_PAGES_DIR = DATA_DIR / "fetched_pages"
CONTEXTS_DIR = DATA_DIR / "contexts"
CLASSIFICATION_PROMPTS_DIR = DATA_DIR / "classification_prompts"
CLASSIFICATION_RESULTS_DIR = DATA_DIR / "classification_results"

LOGS_DIR = PROJECT_ROOT / "logs"
PROMPTS_DIR = PROJECT_ROOT / "prompts"

CLASSIFIER_PROMPT_PATH = PROMPTS_DIR / "wallet_search_classifier.txt"

# Ensure directories exist
for d in [SEARCH_RESULTS_DIR, FETCHED_PAGES_DIR, CONTEXTS_DIR,
          CLASSIFICATION_PROMPTS_DIR, CLASSIFICATION_RESULTS_DIR, LOGS_DIR]:
    d.mkdir(parents=True, exist_ok=True)
