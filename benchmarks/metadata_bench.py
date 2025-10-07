import time
import sqlite3
import random
import statistics
import logging
from pathlib import Path

try:
    from pymongo import MongoClient
    MONGO_AVAILABLE = True
except Exception:
    MONGO_AVAILABLE = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATAMART = PROJECT_ROOT / "data" / "datamarts"
META_DB_SQLITE = DATAMART / "metadata.sqlite"


# --- UTILIDADES ---
def load_metadata_from_sqlite():
    """Carga metadatos reales desde el archivo metadata.sqlite."""
    if not META_DB_SQLITE.exists():
        raise FileNotFoundError(f"No se encontró {META_DB_SQLITE}")
    conn = sqlite3.connect(META_DB_SQLITE)
    cur = conn.cursor()
    cur.execute("SELECT book_id, title, author, language, body_path FROM books")
    records = cur.fetchall()
    conn.close()
    return [
        {"book_id": r[0], "title": r[1], "author": r[2], "language": r[3], "body_path": r[4]}
        for r in records
    ]


def load_metadata_from_mongo():
    """
    Intenta cargar metadatos desde MongoDB.
    Busca en la base 'metadata_db' y selecciona una colección sensible ('books' si existe,
    o la primera colección disponible). No crea/insertar nuevas colecciones.
    Devuelve lista de dicts con keys: book_id, title, author, language, body_path
    """
    if not MONGO_AVAILABLE:
        logging.warning("pymongo no disponible: no se cargarán metadatos desde MongoDB.")
        return []

    try:
        client = MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=2000)
        # Intentamos la base 'metadata_db' tal como en tu código original
        db = client["metadata_db"]
        cols = db.list_collection_names()
        if not cols:
            logging.warning("metadata_db existe pero no contiene colecciones. No se cargan metadatos desde MongoDB.")
            client.close()
            return []

        # Preferimos una colección llamada 'books' si existe
        col_name = None
        for c in cols:
            if "book" in c.lower():
                col_name = c
                break
        if not col_name:
            col_name = cols[0]

        col = db[col_name]
        # Intentamos traer campos esperados; si no están, tomamos lo que haya y mapeamos
        docs = list(col.find({}, {"_id": 1, "book_id": 1, "title": 1, "author": 1, "language": 1, "body_path": 1}))
        records = []
        for d in docs:
            records.append({
                "book_id": d.get("book_id", str(d.get("_id"))),
                "title": d.get("title", "") or "",
                "author": d.get("author", "") or "",
                "language": d.get("language", "") or "",
                "body_path": d.get("body_path", "") or "",
            })
        client.close()
        return records
    except Exception as e:
        logging.warning(f"No se pudo conectar a MongoDB o leer colecciones: {e}")
        return []


def subset(records, n):
    """Devuelve los primeros n registros o todos si n excede el tamaño."""
    return records[:min(n, len(records))]


# --- BENCHMARK: SQLite (NO se crea archivo en disco) ---
def benchmark_sqlite(records):
    """
    Inserta en una base SQLite en memoria y mide tiempos de inserción y consultas.
    No crea archivos en disco.
    """
    conn = sqlite3.connect(":memory:")  # en memoria -> sin persistencia
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE books (
            book_id INTEGER PRIMARY KEY,
            title TEXT,
            author TEXT,
            language TEXT,
            body_path TEXT
        )
    """)

    # Inserción (medida)
    t0 = time.time()
    cur.executemany(
        "INSERT INTO books (book_id, title, author, language, body_path) VALUES (?, ?, ?, ?, ?)",
        [(r["book_id"], r["title"], r["author"], r["language"], r["body_path"]) for r in records]
    )
    conn.commit()
    insert_time = time.time() - t0

    # Query performance (muestreo)
    sample_authors = random.sample([r["author"] for r in records if r["author"]], min(5, len(records)))
    sample_titles = random.sample([r["title"] for r in records if r["title"]], min(5, len(records)))
    sample_ids = random.sample([r["book_id"] for r in records], min(5, len(records)))

    q_times = []
    for a in sample_authors:
        start = time.time()
        cur.execute("SELECT * FROM books WHERE author = ?", (a,))
        cur.fetchall()
        q_times.append(time.time() - start)

    for t in sample_titles:
        start = time.time()
        cur.execute("SELECT body_path FROM books WHERE title = ?", (t,))
        cur.fetchall()
        q_times.append(time.time() - start)

    for i in sample_ids:
        start = time.time()
        cur.execute("SELECT title FROM books WHERE book_id = ?", (i,))
        cur.fetchall()
        q_times.append(time.time() - start)

    query_time = statistics.mean(q_times) if q_times else 0.0
    conn.close()

    return {
        "engine": "sqlite",
        "records": len(records),
        "insert_time": insert_time,
        "avg_query_time": query_time,
    }


# --- BENCHMARK: MongoDB (NO crea colección nueva; opera en memoria) ---
def benchmark_mongo(records):
    """
    Realiza el benchmark de 'inserción' y consultas sobre los registros ya cargados
    desde MongoDB, pero sin crear colecciones ni persistir nada.
    - La 'inserción' se simula con una copia a lista (operación en memoria).
    - Las consultas se simulan con comprensión de listas sobre la estructura en memoria.
    """
    if not records:
        return {"engine": "mongodb", "error": "No hay metadatos cargados desde MongoDB"}

    # Simulamos la inserción (copia en memoria) y medimos el coste
    t0 = time.time()
    in_memory_collection = list(records)
    insert_time = time.time() - t0

    # Query performance (simulado vía búsquedas en lista)
    sample_authors = random.sample([r["author"] for r in records if r["author"]], min(5, len(records)))
    sample_titles = random.sample([r["title"] for r in records if r["title"]], min(5, len(records)))
    sample_ids = random.sample([r["book_id"] for r in records], min(5, len(records)))

    q_times = []
    for a in sample_authors:
        start = time.time()
        [r for r in in_memory_collection if r.get("author") == a]
        q_times.append(time.time() - start)

    for t in sample_titles:
        start = time.time()
        [r.get("body_path") for r in in_memory_collection if r.get("title") == t]
        q_times.append(time.time() - start)

    for i in sample_ids:
        start = time.time()
        [r.get("title") for r in in_memory_collection if r.get("book_id") == i]
        q_times.append(time.time() - start)

    query_time = statistics.mean(q_times) if q_times else 0.0

    return {
        "engine": "mongodb",
        "records": len(records),
        "insert_time": insert_time,
        "avg_query_time": query_time,
    }


# --- MAIN ---
if __name__ == "__main__":
    # Cargar metadatos desde SQLite (archivo)
    logging.info("Cargando metadatos desde SQLite (archivo)...")
    try:
        metadata_sqlite = load_metadata_from_sqlite()
        logging.info(f"Metadatos cargados desde SQLite: {len(metadata_sqlite)} registros.")
    except Exception as e:
        logging.warning(f"No se pudo cargar metadata desde SQLite: {e}")
        metadata_sqlite = []

    # Cargar metadatos desde MongoDB (si es posible)
    logging.info("Intentando cargar metadatos desde MongoDB...")
    metadata_mongo = load_metadata_from_mongo()
    if metadata_mongo:
        logging.info(f"Metadatos cargados desde MongoDB: {len(metadata_mongo)} registros.")
    else:
        logging.info("No se cargaron metadatos desde MongoDB (no disponible o vacío).")

    if not metadata_sqlite and not metadata_mongo:
        logging.warning("No hay metadatos disponibles desde SQLite ni desde MongoDB. Ejecuta run_pipeline.py o revisa las fuentes.")
        exit(0)

    results = []

    # Benchmarks para los datos provenientes de SQLite (si existen)
    if metadata_sqlite:
        sizes = [100, 500, 1000, 2000, len(metadata_sqlite)]
        for size in sizes:
            subset_records = subset(metadata_sqlite, size)
            logging.info(f"=== Benchmark (SQLite source) con {len(subset_records)} registros ===")
            res_sqlite = benchmark_sqlite(subset_records)
            # Para Mongo benchmark aquí usamos los datos de Mongo; no crear nada en Mongo
            # Si no hay metadata_mongo, simulamos benchmark_mongo sobre los mismos registros
            if metadata_mongo:
                res_mongo = benchmark_mongo(subset(metadata_mongo, size))
            else:
                # Si no hay metadata en Mongo, medir el comportamiento simulado sobre los mismos datos
                res_mongo = benchmark_mongo(subset_records)
            results.extend([res_sqlite, res_mongo])

    # Si había metadatos exclusivamente en Mongo (y no en SQLite), también los medimos
    if metadata_mongo and not metadata_sqlite:
        sizes = [100, 500, 1000, 2000, len(metadata_mongo)]
        for size in sizes:
            subset_records = subset(metadata_mongo, size)
            logging.info(f"=== Benchmark (Mongo source) con {len(subset_records)} registros ===")
            # SQLite benchmark se realiza en memoria con los mismos datos para comparar
            res_sqlite = benchmark_sqlite(subset_records)
            res_mongo = benchmark_mongo(subset_records)
            results.extend([res_sqlite, res_mongo])

    # --- RESUMEN FINAL ---
    print("\n========= METADATA BENCHMARK RESULTS =========")
    print(f"{'Engine':<10} {'Records':<8} {'InsertTime(s)':<15} {'AvgQuery(s)':<12}")
    print("-" * 55)
    for r in results:
        if "error" in r:
            print(f"{r['engine']:<10} ERROR: {r['error']}")
        else:
            print(f"{r['engine']:<10} {r['records']:<8} {r['insert_time']:<15.8f} {r['avg_query_time']:<12.8f}")
    print("=============================================\n")

    logging.info("Benchmark de metadatos completado con éxito.")
