"""
Benchmark: Comparación de rendimiento entre tres enfoques de índice invertido (solo en memoria):
1. JSON (estructura Python en memoria)
2. SQLite (base temporal en memoria)
3. MongoDB (colección temporal)

Carga:
- Textos desde el datalake
- Metadatos desde SQLite y MongoDB (si existen)

Mide:
- Indexing speed (tiempo total de construcción)
- Query performance (tiempo promedio de consultas)
- Scalability (cómo cambia con el número de libros y términos)
"""

import time
import json
import sqlite3
import random
import statistics
import logging
from pathlib import Path
from collections import defaultdict

from src.search_project.utils.text_utils import tokenize

try:
    from pymongo import MongoClient
    MONGO_AVAILABLE = True
except Exception:
    MONGO_AVAILABLE = False


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATALAKE = PROJECT_ROOT / "data" / "datalake"
DATAMART = PROJECT_ROOT / "data" / "datamarts"


# --- Utilidades generales ---
def load_books_from_datalake(limit=None):
    """Lee libros del datalake y devuelve [(book_id, text)]"""
    books = []
    for p in sorted(DATALAKE.rglob("*.body.txt")):
        try:
            text = p.read_text(encoding="utf-8")
            books.append((p.stem.split(".")[0], text))
            if limit and len(books) >= limit:
                break
        except Exception as e:
            logging.warning(f"Error leyendo {p}: {e}")
    return books


def load_metadata_sqlite():
    """Carga metadatos desde SQLite si existe el archivo."""
    db_path = DATAMART / "metadata.sqlite"
    if not db_path.exists():
        logging.warning("No se encontró metadata.sqlite")
        return []
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT book_id, title, author, language, body_path FROM books")
    rows = cur.fetchall()
    conn.close()
    return rows


def load_metadata_mongo():
    """Carga metadatos desde MongoDB si está disponible."""
    if not MONGO_AVAILABLE:
        logging.warning("MongoDB no disponible.")
        return []
    try:
        client = MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=2000)
        db = client["search_engine"]
        col = db["books_metadata"]
        docs = list(col.find({}, {"_id": 0}))
        client.close()
        return docs
    except Exception as e:
        logging.warning(f"No se pudo conectar a MongoDB: {e}")
        return []


def build_inverted_index(docs):
    """Construye un índice invertido {term -> set(book_id)}"""
    inverted = defaultdict(set)
    for book_id, text in docs:
        for w in tokenize(text, remove_stopwords=True):
            inverted[w].add(book_id)
    return inverted


def representative_queries(inverted, n=5):
    """Selecciona n términos aleatorios del índice para medir consultas."""
    terms = list(inverted.keys())
    if not terms:
        return []
    return random.sample(terms, min(n, len(terms)))


# --- BENCHMARKS ---
def benchmark_json(docs, runs=10):
    """Mide rendimiento del índice invertido en memoria tipo JSON"""
    times_index = []
    times_query = []

    for _ in range(runs):
        t0 = time.time()
        inverted = build_inverted_index(docs)
        times_index.append(time.time() - t0)

        queries = representative_queries(inverted)
        q_times = []
        for q in queries:
            start = time.time()
            _ = inverted.get(q, [])
            q_times.append(time.time() - start)
        if q_times:
            times_query.append(statistics.mean(q_times))

    return {
        "engine": "json",
        "index_time": statistics.mean(times_index),
        "avg_query_time": statistics.mean(times_query) if times_query else 0.0,
        "num_books": len(docs),
        "num_terms": len(inverted),
    }


def benchmark_sqlite(docs, runs=10):
    """Benchmark en memoria con SQLite"""
    times_index = []
    times_query = []

    for _ in range(runs):
        inverted = build_inverted_index(docs)
        conn = sqlite3.connect(":memory:")
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE inverted (
                term TEXT PRIMARY KEY,
                postings TEXT
            )
        """)

        t0 = time.time()
        for term, ids in inverted.items():
            cur.execute("INSERT INTO inverted VALUES (?, ?)", (term, json.dumps(sorted(list(ids)))))
        conn.commit()
        times_index.append(time.time() - t0)

        queries = representative_queries(inverted)
        q_times = []
        for q in queries:
            start = time.time()
            cur.execute("SELECT postings FROM inverted WHERE term = ?", (q,))
            _ = cur.fetchone()
            q_times.append(time.time() - start)
        if q_times:
            times_query.append(statistics.mean(q_times))
        conn.close()

    return {
        "engine": "sqlite",
        "index_time": statistics.mean(times_index),
        "avg_query_time": statistics.mean(times_query) if times_query else 0.0,
        "num_books": len(docs),
        "num_terms": len(inverted),
    }


def benchmark_mongo(docs, runs=10):
    """Benchmark temporal con MongoDB"""
    if not MONGO_AVAILABLE:
        return {"engine": "mongodb", "error": "MongoDB no disponible"}

    try:
        client = MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=2000)
        db = client["search_engine"]
        col = db["temp_inverted_index"]
        times_index = []
        times_query = []

        for _ in range(runs):
            col.drop()
            col.create_index("term", unique=True)
            inverted = build_inverted_index(docs)

            t0 = time.time()
            for term, ids in inverted.items():
                col.insert_one({"term": term, "postings": sorted(list(ids))})
            times_index.append(time.time() - t0)

            queries = representative_queries(inverted)
            q_times = []
            for q in queries:
                start = time.time()
                col.find_one({"term": q}, {"_id": 0})
                q_times.append(time.time() - start)
            if q_times:
                times_query.append(statistics.mean(q_times))

        client.close()
        return {
            "engine": "mongodb",
            "index_time": statistics.mean(times_index),
            "avg_query_time": statistics.mean(times_query) if times_query else 0.0,
            "num_books": len(docs),
            "num_terms": len(inverted),
        }
    except Exception as e:
        return {"engine": "mongodb", "error": str(e)}


# --- MAIN ---
if __name__ == "__main__":
    logging.info("Cargando libros reales desde datalake...")
    books = load_books_from_datalake(limit=None)
    if not books:
        logging.warning("No se encontraron libros. Ejecuta run_pipeline.py primero.")
        exit(0)

    logging.info("Cargando metadatos desde SQLite y MongoDB (si existen)...")
    metadata_sqlite = load_metadata_sqlite()
    metadata_mongo = load_metadata_mongo()
    logging.info(f"SQLite: {len(metadata_sqlite)} registros | MongoDB: {len(metadata_mongo)} registros")

    sizes = [5, 10, 20, 40, len(books)]
    results = []

    for size in sizes:
        subset = books[:min(size, len(books))]
        logging.info(f"=== Benchmark con {len(subset)} libros ===")
        res_sqlite = benchmark_sqlite(subset)
        res_mongo = benchmark_mongo(subset)
        res_json = benchmark_json(subset)
        results.extend([res_sqlite, res_mongo, res_json])

    # --- Imprimir resumen ---
    print("\n========= BENCHMARK RESULTS =========")
    print(f"{'Engine':<10} {'Books':<6} {'Terms':<8} {'IndexTime(s)':<15} {'AvgQuery(s)':<12}")
    print("-" * 60)
    for r in results:
        if "error" in r:
            print(f"{r['engine']:<10} ERROR: {r['error']}")
        else:
            print(f"{r['engine']:<10} {r['num_books']:<6} {r['num_terms']:<8} "
                  f"{r['index_time']:<15.8f} {r['avg_query_time']:<12.8f}")
    print("=====================================\n")

    logging.info("Benchmark completo sin archivos persistentes.")
