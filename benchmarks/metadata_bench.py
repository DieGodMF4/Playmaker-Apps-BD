# benchmarks/metadata_bench.py
"""
Database Engine Comparison (METADATA layer)
- SQLite (en memoria)
- MongoDB (opcional, si está disponible)

Mide:
1) Velocidad de inserción
2) Rendimiento de consultas:
   - Buscar libros por autor
   - Recuperar body_path por book_id
3) Escalabilidad: tamaños crecientes

Resultados: SOLO por pantalla. No se crea ningún archivo.
Fuente de datos: data/datamarts/metadata.sqlite (tabla books), resuelta desde la raíz del proyecto.
"""

from pathlib import Path
import sys
import os
import random
import sqlite3
import time
import logging
from typing import List, Dict, Any, Tuple

# --- Localiza la raíz del proyecto aunque ejecutes desde otra carpeta ---
THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parents[1]            # .../Playmaker-Apps-BD-/
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from pymongo import MongoClient  # type: ignore
    HAS_MONGO = True
except Exception:
    HAS_MONGO = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

SRC_SQLITE = PROJECT_ROOT / "data" / "datamarts" / "metadata.sqlite"

# -------------------------
# Helpers de carga
# -------------------------
def load_source(limit: int | None = None) -> List[Dict[str, Any]]:
    if not SRC_SQLITE.exists():
        raise FileNotFoundError(f"No existe {SRC_SQLITE}. Ejecuta el pipeline primero.")
    con = sqlite3.connect(SRC_SQLITE)
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    q = "SELECT book_id, title, author, language, body_path FROM books"
    if limit:
        q += f" LIMIT {int(limit)}"
    rows = [dict(r) for r in cur.execute(q)]
    con.close()
    return rows

def subset(rows: List[Dict[str, Any]], n: int) -> List[Dict[str, Any]]:
    if n >= len(rows):
        return rows
    return random.sample(rows, n)

# -------------------------
# SQLite (EN MEMORIA)
# -------------------------
def bench_sqlite(rows: List[Dict[str, Any]]) -> Tuple[float, float, float]:
    """
    Devuelve: (insert_time_s, avg_author_query_s, avg_id_query_s)
    """
    con = sqlite3.connect(":memory:")
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE books(
            book_id INTEGER PRIMARY KEY,
            title TEXT, author TEXT, language TEXT, body_path TEXT
        );
    """)
    cur.execute("CREATE INDEX idx_books_author ON books(author);")
    con.commit()

    t0 = time.perf_counter()
    cur.executemany(
        "INSERT INTO books(book_id, title, author, language, body_path) VALUES (?,?,?,?,?)",
        [(r["book_id"], r["title"], r["author"], r["language"], r["body_path"]) for r in rows]
    )
    con.commit()
    t_insert = time.perf_counter() - t0

    # Queries
    authors = [r["author"] for r in subset(rows, min(50, len(rows)))]
    ids = [r["book_id"] for r in subset(rows, min(200, len(rows)))]

    t0 = time.perf_counter()
    for a in authors:
        list(cur.execute("SELECT book_id FROM books WHERE author = ?", (a,)).fetchall())
    t_author = (time.perf_counter() - t0) / max(1, len(authors))

    t0 = time.perf_counter()
    for i in ids:
        cur.execute("SELECT body_path FROM books WHERE book_id = ?", (i,))
        cur.fetchone()
    t_id = (time.perf_counter() - t0) / max(1, len(ids))

    con.close()
    return t_insert, t_author, t_id

# -------------------------
# MongoDB (colección temporal)
# -------------------------
def bench_mongo(rows: List[Dict[str, Any]]) -> Tuple[float, float, float]:
    if not HAS_MONGO:
        raise RuntimeError("MongoDB/pymongo no disponible.")
    import uuid
    client = MongoClient(os.getenv("MONGODB_URI", "mongodb://localhost:27017"), serverSelectionTimeoutMS=2000)  # type: ignore
    db = client[os.getenv("MONGODB_DB", "bench_search")]
    col_name = f"bench_books_{uuid.uuid4().hex[:8]}"
    col = db[col_name]
    col.create_index("author")
    col.create_index("book_id", unique=True)

    # Inserción
    t0 = time.perf_counter()
    docs = [{
        "book_id": int(r["book_id"]),
        "title": r["title"],
        "author": r["author"],
        "language": r["language"],
        "body_path": r["body_path"]
    } for r in rows]
    if docs:
        col.insert_many(docs, ordered=False)
    t_insert = time.perf_counter() - t0

    # Queries
    authors = [r["author"] for r in subset(rows, min(50, len(rows)))]
    ids = [int(r["book_id"]) for r in subset(rows, min(200, len(rows)))]

    t0 = time.perf_counter()
    for a in authors:
        list(col.find({"author": a}, {"_id": 0, "book_id": 1}))
    t_author = (time.perf_counter() - t0) / max(1, len(authors))

    t0 = time.perf_counter()
    for i in ids:
        col.find_one({"book_id": i}, {"_id": 0, "body_path": 1})
    t_id = (time.perf_counter() - t0) / max(1, len(ids))

    # Limpieza (no persistir nada)
    col.drop()
    client.close()
    return t_insert, t_author, t_id

# -------------------------
# Runner
# -------------------------
def main():
    random.seed(42)
    all_rows = load_source()
    total = len(all_rows)
    if total == 0:
        print("No hay filas en metadata.sqlite:books")
        return

    sizes = [100, 1000, 5000, 10000]
    sizes = [n for n in sizes if n <= total] or [min(100, total)]

    results = []
    engines = [("SQLite", bench_sqlite)]
    if HAS_MONGO:
        engines.append(("MongoDB", bench_mongo))

    for n in sizes:
        rows = subset(all_rows, n)
        for name, fn in engines:
            try:
                ins, q_auth, q_id = fn(rows)
                results.append({
                    "engine": name,
                    "records": n,
                    "insert_time_s": ins,
                    "avg_author_query_s": q_auth,
                    "avg_id_query_s": q_id,
                })
            except Exception as e:
                results.append({"engine": name, "records": n, "error": str(e)})

    # Mostrar por pantalla
    print("\n========= METADATA BENCHMARK RESULTS =========")
    print(f"{'Engine':<10} {'Records':<8} {'Insert(s)':<12} {'Q(author)s':<12} {'Q(id)s':<12}")
    print("-"*60)
    for r in results:
        if "error" in r:
            print(f"{r['engine']:<10} {r['records']:<8} ERROR: {r['error']}")
        else:
            print(f"{r['engine']:<10} {r['records']:<8} {r['insert_time_s']:<12.6f} {r['avg_author_query_s']:<12.6f} {r['avg_id_query_s']:<12.6f}")
    print("==============================================\n")

if __name__ == "__main__":
    main()
