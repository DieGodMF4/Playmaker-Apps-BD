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


# ---------------------------
# JSON helpers (acumulativo)
# ---------------------------

def _load_existing_json_index(json_path: Path) -> dict[str, set[str]]:
    """
    Carga el JSON existente y lo normaliza a dict[term] -> set[str(book_id)].
    Soporta el formato antiguo { "term": ["id", ...] } y el nuevo
    [ {"term": "...", "postings": ["id", ...]}, ... ].
    """
    acc: dict[str, set[str]] = defaultdict(set)
    if not json_path.exists():
        return acc
    try:
        data = json.loads(json_path.read_text(encoding="utf-8"))
    except Exception:
        return acc

    if isinstance(data, dict):
        # Formato antiguo: { term: [ids] }
        for term, ids in data.items():
            for i in ids or []:
                acc[term].add(str(i))
    elif isinstance(data, list):
        # Formato nuevo: [{term, postings}]
        for row in data:
            term = row.get("term")
            ids = row.get("postings", [])
            if not term:
                continue
            for i in ids:
                acc[term].add(str(i))
    return acc


def _dump_json_index(merged: dict[str, set[str]], json_path: Path) -> None:
    """
    Escribe el índice en formato lista de objetos:
      [{ "term": <str>, "postings": [<str>, ...] }, ...]
    con orden alfabético por término y postings.
    """
    rows = [
        {"term": term, "postings": sorted(list(ids), key=lambda x: (len(x), x))}
        for term, ids in sorted(merged.items(), key=lambda kv: kv[0])
    ]
    with open(json_path, "w", encoding="utf-8") as jf:
        json.dump(rows, jf, indent=2, ensure_ascii=False)


# ---------------------------
# SQLite
# ---------------------------

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

    # Bulk upserts dentro de una transacción
    to_update = []
    to_insert = []
    for term, book_ids in inverted_index.items():
        # Garantizamos strings para ser consistentes con Mongo/JSON
        book_list = sorted([str(x) for x in book_ids], key=lambda x: (len(x), x))
        cur.execute("SELECT postings FROM inverted WHERE term = ?", (term,))
        row = cur.fetchone()
        if row:
            existing = set(json.loads(row[0]) or [])
            combined = sorted(existing | set(book_list), key=lambda x: (len(x), x))
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


# ---------------------------
# MongoDB
# ---------------------------

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
        # $addToSet with $each evita duplicados
        for term, book_ids in inverted_index.items():
            str_ids = [str(x) for x in book_ids]
            col.update_one(
                {"term": term},
                {"$addToSet": {"postings": {"$each": str_ids}}},
                upsert=True
            )
        client.close()
        logging.info(f"[INDEXER] MongoDB index updated (db={mongo_db}, col={collection_name})")
        return True
    except Exception as e:
        logging.warning(f"[INDEXER] MongoDB connection error: {e}")
        return False


# ---------------------------
# Builder principal
# ---------------------------

def build_index_from_paths(paths, datamart_root: Path = Path("data/datamarts")):
    """
    Incrementally update the inverted index (SQLite + optional MongoDB + JSON acumulativo).

    - Lee los textos de 'paths'
    - Construye 'inverted' para ESTE lote
    - Fusiona con el JSON previo (si existe)
    - Escribe JSON acumulado
    - Upsert en SQLite y Mongo con SOLO los términos de este lote (ahorramos trabajo)
    """
    datamart_root.mkdir(parents=True, exist_ok=True)
    inverted: dict[str, set[str]] = defaultdict(set)

    # 1) Construir índice de este lote
    for p in paths:
        # "11155.body.txt" -> "11155"
        book_id = p.stem.split(".")[0]
        try:
            text = p.read_text(encoding="utf-8")
        except Exception as e:
            logging.warning(f"[INDEXER] Error reading {p}: {e}")
            continue
        for w in tokenize(text, remove_stopwords=True):
            inverted[w].add(str(book_id))

    # 2) Cargar JSON existente y fusionar
    json_path = datamart_root / "inverted_index.json"
    merged = _load_existing_json_index(json_path)
    for term, ids in inverted.items():
        for i in ids:
            merged[term].add(str(i))

    # 3) Guardar JSON acumulado en formato {term, postings}
    _dump_json_index(merged, json_path)
    logging.info(f"[INDEXER] JSON index saved at {json_path} (rows: {len(merged)})")

    # 4) Actualizar SQLite/Mongo con el lote (ellas mismas hacen upsert)
    sqlite_path = datamart_root / "inverted_index.sqlite"
    build_index_sqlite(inverted, sqlite_path)
    build_index_mongo(inverted)
