"""
Builds the inverted index using:
- SQLite
- MongoDB (if available)
Each document is one book_id; postings are stored per term.
"""
from pathlib import Path
import logging
from collections import defaultdict
import json
import sqlite3
from ..utils.text_utils import tokenize

try:
    from pymongo import MongoClient
    MONGO_AVAILABLE = True
except Exception:
    MONGO_AVAILABLE = False

def build_index_sqlite(inverted_index, sqlite_path: Path):
    """Create/update inverted index in SQLite (single table with JSON postings)."""
    sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(sqlite_path)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS inverted (
            term TEXT PRIMARY KEY,
            postings TEXT
        )
    """)
    # Bulk upserts inside a single transaction
    to_update = []
    to_insert = []
    for term, book_ids in inverted_index.items():
        book_list = sorted(list(book_ids))
        cur.execute("SELECT postings FROM inverted WHERE term = ?", (term,))
        row = cur.fetchone()
        if row:
            existing = set(json.loads(row[0]) or [])
            combined = sorted(existing | set(book_list))
            to_update.append((json.dumps(combined), term))
        else:
            to_insert.append((term, json.dumps(book_list)))

    if to_update:
        cur.executemany("UPDATE inverted SET postings = ? WHERE term = ?", to_update)
    if to_insert:
        cur.executemany("INSERT INTO inverted (term, postings) VALUES (?, ?)", to_insert)

    conn.commit()
    conn.close()
    logging.info(f"[INDEXER] SQLite index updated at {sqlite_path} "
                 f"(terms touched: {len(inverted_index)})")

def build_index_mongo(inverted_index, mongo_db="search_engine", collection_name="inverted_index"):
    """Create/update inverted index in MongoDB."""
    if not MONGO_AVAILABLE:
        logging.warning("[INDEXER] MongoDB not available; skipping.")
        return False
    try:
        client = MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=2000)
        db = client[mongo_db]
        col = db[collection_name]
        col.create_index("term", unique=True)
        # $addToSet with $each avoids duplicates
        for term, book_ids in inverted_index.items():
            col.update_one(
                {"term": term},
                {"$addToSet": {"postings": {"$each": list(book_ids)}}},
                upsert=True
            )
        client.close()
        logging.info(f"[INDEXER] MongoDB index updated (db={mongo_db}, col={collection_name})")
        return True
    except Exception as e:
        logging.warning(f"[INDEXER] MongoDB connection error: {e}")
        return False

def build_index_from_paths(paths, datamart_root: Path = Path("data/datamarts")):
    """Incrementally update the inverted index (SQLite + optional MongoDB)."""
    datamart_root.mkdir(parents=True, exist_ok=True)
    inverted = defaultdict(set)

    for p in paths:
        book_id = p.stem.split(".")[0]
        try:
            text = p.read_text(encoding="utf-8")
        except Exception as e:
            logging.warning(f"[INDEXER] Error reading {p}: {e}")
            continue
        for w in tokenize(text, remove_stopwords=True):
            inverted[w].add(book_id)

    sqlite_path = datamart_root / "inverted_index.sqlite"
    build_index_sqlite(inverted, sqlite_path)
    build_index_mongo(inverted)
