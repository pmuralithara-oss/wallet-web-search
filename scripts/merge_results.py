"""Merge results from multiple Colab chunk DBs into pipeline_social.db."""
import glob
import sqlite3
from pathlib import Path

MAIN_DB = Path(__file__).parent.parent / "pipeline_social.db"

def merge():
    main = sqlite3.connect(str(MAIN_DB))
    main.execute("PRAGMA journal_mode=WAL")

    # Find all chunk DBs in Downloads or current dir
    chunk_dbs = glob.glob(str(Path.home() / "Downloads" / "results_chunk_*.db"))
    chunk_dbs += glob.glob("results_chunk_*.db")

    total = 0
    for db_path in chunk_dbs:
        chunk = sqlite3.connect(db_path)
        rows = chunk.execute("SELECT * FROM hits").fetchall()
        for row in rows:
            try:
                main.execute("INSERT OR IGNORE INTO address_search VALUES (?,?,?,?,?,?,?,?)", row)
            except:
                pass
        main.commit()
        total += len(rows)
        print(f"Merged {len(rows):,} from {db_path}")
        chunk.close()

    n = main.execute("SELECT count(*) FROM address_search").fetchone()[0]
    s = main.execute("SELECT count(*) FROM address_search WHERE has_social_hit=1").fetchone()[0]
    print(f"\nTotal in DB: {n:,}, Social hits: {s}")
    main.close()

if __name__ == "__main__":
    merge()
