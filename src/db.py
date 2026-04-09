"""SQLite database initialization and helpers."""
import sqlite3
from src.config import DB_PATH

SCHEMA = """
CREATE TABLE IF NOT EXISTS wallets (
    address TEXT PRIMARY KEY,
    total_notional REAL,
    total_trades INTEGER,
    estimated_loss REAL,
    loss_rank INTEGER,
    selected BOOLEAN DEFAULT FALSE,
    searched BOOLEAN DEFAULT FALSE,
    searched_at TIMESTAMP,
    search_error TEXT,
    num_results INTEGER
);

CREATE TABLE IF NOT EXISTS search_hits (
    hit_id INTEGER PRIMARY KEY AUTOINCREMENT,
    address TEXT,
    url TEXT,
    title TEXT,
    snippet TEXT,
    source_type TEXT,
    priority INTEGER,
    fetched BOOLEAN DEFAULT FALSE,
    fetched_at TIMESTAMP,
    fetch_error TEXT,
    raw_html_path TEXT,
    context_extracted BOOLEAN DEFAULT FALSE,
    context_path TEXT,
    classified BOOLEAN DEFAULT FALSE,
    classification_error TEXT,
    UNIQUE(address, url),
    FOREIGN KEY (address) REFERENCES wallets(address)
);

CREATE TABLE IF NOT EXISTS classifications (
    hit_id INTEGER PRIMARY KEY,
    address TEXT,
    is_first_person_disclosure TEXT,
    is_wallet_owner_the_speaker TEXT,
    disclosure_evidence TEXT,
    emotional_context TEXT,
    confidence REAL,
    needs_human_review BOOLEAN,
    reasoning TEXT,
    raw_response TEXT,
    classified_at TIMESTAMP,
    FOREIGN KEY (hit_id) REFERENCES search_hits(hit_id)
);

CREATE INDEX IF NOT EXISTS idx_wallets_searched ON wallets(searched);
CREATE INDEX IF NOT EXISTS idx_hits_fetched ON search_hits(fetched);
CREATE INDEX IF NOT EXISTS idx_hits_classified ON search_hits(classified);
CREATE INDEX IF NOT EXISTS idx_hits_priority ON search_hits(priority);
"""


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    conn = get_connection()
    conn.executescript(SCHEMA)
    conn.close()


if __name__ == "__main__":
    init_db()
    print(f"Database initialized at {DB_PATH}")
