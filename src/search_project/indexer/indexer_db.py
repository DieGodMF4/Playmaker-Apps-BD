"""
Construye el índice invertido únicamente usando:
- SQLite
- MongoDB (si está disponible)
Cada documento es un libro (book_id) y se guarda un posting list por palabra.
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
    """Crea o actualiza índice invertido en SQLite."""
    sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(sqlite_path)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS inverted (
            term TEXT PRIMARY KEY,
            postings TEXT
        )
    """)
    conn.commit()
    for term, book_ids in inverted_index.items():
        cur.execute("SELECT postings FROM inverted WHERE term = ?", (term,))
        row = cur.fetchone()
        if row:
            existing = set(json.loads(row[0]) or [])
            combined = sorted(existing | book_ids)
            cur.execute("UPDATE inverted SET postings = ? WHERE term = ?", (json.dumps(combined), term))
        else:
            cur.execute("INSERT INTO inverted (term, postings) VALUES (?, ?)", (term, json.dumps(sorted(book_ids))))
    conn.commit()
    conn.close()
    logging.info(f"[INDEXER] Índice SQLite actualizado en {sqlite_path}")

def build_index_mongo(inverted_index, mongo_db="search_engine", collection_name="inverted_index"):
    """Crea o actualiza índice invertido en MongoDB."""
    if not MONGO_AVAILABLE:
        logging.warning("[INDEXER] MongoDB no disponible; se omite.")
        return False
    try:
        client = MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=2000)
        db = client[mongo_db]
        col = db[collection_name]
        col.create_index("term", unique=True)
        for term, book_ids in inverted_index.items():
            col.update_one(
                {"term": term},
                {"$addToSet": {"postings": {"$each": list(book_ids)}}},
                upsert=True
            )
        client.close()
        logging.info(f"[INDEXER] Índice MongoDB actualizado en DB={mongo_db}, colección={collection_name}")
        return True
    except Exception as e:
        logging.warning(f"[INDEXER] Error al conectar con MongoDB: {e}")
        return False

def build_index_from_paths(paths, datamart_root: Path = Path("data/datamarts")):
    """Construye índice invertido solo en SQLite y MongoDB."""
    datamart_root.mkdir(parents=True, exist_ok=True)
    inverted = defaultdict(set)
    for p in paths:
        book_id = p.stem.split(".")[0]
        try:
            text = p.read_text(encoding="utf-8")
        except Exception as e:
            logging.warning(f"[INDEXER] Error leyendo {p}: {e}")
            continue
        words = tokenize(text)
        for w in words:
            inverted[w].add(book_id)

    # Guardar en SQLite
    sqlite_path = datamart_root / "inverted_index.sqlite"
    build_index_sqlite(inverted, sqlite_path)
    # Guardar en MongoDB
    build_index_mongo(inverted)
