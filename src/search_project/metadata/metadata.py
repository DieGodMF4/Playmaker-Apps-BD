import re
from pathlib import Path
import sqlite3
import csv
import logging

META_DB = Path("data/datamarts/metadata.sqlite")
META_CSV = Path("data/datamarts/metadata.csv")
META_DB.parent.mkdir(parents=True, exist_ok=True)

TITLE_RE = re.compile(r"^\s*Title:\s*(.+)$", re.IGNORECASE | re.MULTILINE)
AUTHOR_RE = re.compile(r"^\s*Author:\s*(.+)$", re.IGNORECASE | re.MULTILINE)
LANG_RE = re.compile(r"^\s*Language:\s*(.+)$", re.IGNORECASE | re.MULTILINE)

try:
    from pymongo import MongoClient
    MONGO_AVAILABLE = True
except Exception:
    MONGO_AVAILABLE = False


def extract_basic(header: str):
    title = TITLE_RE.search(header)
    author = AUTHOR_RE.search(header)
    lang = LANG_RE.search(header)
    return {
        "title": title.group(1).strip() if title else "",
        "author": author.group(1).strip() if author else "",
        "language": lang.group(1).strip() if lang else ""
    }


def ensure_sqlite():
    conn = sqlite3.connect(META_DB)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS books (
            book_id INTEGER PRIMARY KEY,
            title TEXT,
            author TEXT,
            language TEXT,
            body_path TEXT
        )
    """)
    conn.commit()
    return conn


def save_metadata(book_id: int, meta: dict, body_path: Path):
    # --- SQLite ---
    conn = ensure_sqlite()
    cur = conn.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO books (book_id, title, author, language, body_path) VALUES (?, ?, ?, ?, ?)",
        (book_id, meta.get("title", ""), meta.get("author", ""), meta.get("language", ""), str(body_path))
    )
    conn.commit()
    conn.close()

    # --- CSV ---
    META_CSV.parent.mkdir(parents=True, exist_ok=True)
    write_header = not META_CSV.exists()
    with open(META_CSV, "a", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        if write_header:
            w.writerow(["book_id", "title", "author", "language", "body_path"])
        w.writerow([book_id, meta.get("title",""), meta.get("author",""), meta.get("language",""), str(body_path)])

    # --- MongoDB ---
    if MONGO_AVAILABLE:
        try:
            client = MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=2000)
            db = client["search_engine"]
            col = db["metadata"]
            col.update_one(
                {"book_id": book_id},
                {"$set": {
                    "title": meta.get("title", ""),
                    "author": meta.get("author", ""),
                    "language": meta.get("language", ""),
                    "body_path": str(body_path)
                }},
                upsert=True
            )
            client.close()
        except Exception as e:
            logging.warning(f"[META] Error guardando metadatos en MongoDB: {e}")


def extract_metadata_for_book(book_id: int, header: str, body_path: Path):
    meta = extract_basic(header)
    try:
        save_metadata(book_id, meta, body_path)
    except Exception as e:
        logging.exception(f"[META] Could not save metadata for {book_id}: {e}")
    return meta
