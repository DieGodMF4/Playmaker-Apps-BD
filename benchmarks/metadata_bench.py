"""
Benchmark: compara rendimiento de SQLite vs MongoDB para la metadata de libros reales.
"""

import time
import sqlite3
from pathlib import Path
import logging

try:
    from pymongo import MongoClient
    MONGO_AVAILABLE = True
except Exception:
    MONGO_AVAILABLE = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# --- Rutas absolutas relativas a la raíz del proyecto ---
PROJECT_ROOT = Path(__file__).resolve().parent.parent  # Dos niveles arriba de benchmarks/
META_DB = PROJECT_ROOT / "data" / "datamarts" / "metadata.sqlite"

# --- Funciones de carga ---
def load_metadata_from_sqlite():
    if not META_DB.exists():
        raise FileNotFoundError(f"No se encontró {META_DB}")
    conn = sqlite3.connect(META_DB)
    cur = conn.cursor()
    cur.execute("SELECT book_id, title, author, language, body_path FROM books")
    records = cur.fetchall()
    conn.close()
    return records

def load_metadata_from_mongo():
    if not MONGO_AVAILABLE:
        logging.warning("MongoDB no disponible")
        return []
    try:
        client = MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=2000)
        db = client["metadata_db"]
        col = db["books"]
        records = list(col.find({}, {"_id": 0}))
        client.close()
        return records
    except Exception as e:
        logging.warning(f"No se pudo conectar a MongoDB: {e}")
        return []

# --- Benchmarking ---
def benchmark_sqlite(records):
    t0 = time.time()
    # Inserciones (no se borran, solo medimos tiempo de lectura)
    for r in records:
        pass  # ya están insertados
    t1 = time.time()
    # Consultas de ejemplo
    q0 = time.time()
    # Buscar todos los libros de un autor específico
    conn = sqlite3.connect(META_DB)
    cur = conn.cursor()
    cur.execute("SELECT * FROM books WHERE author LIKE 'A%' LIMIT 10")
    _ = cur.fetchall()
    conn.close()
    q1 = time.time()
    return {"engine": "sqlite", "records": len(records), "query_time": q1 - q0, "insert_time": t1 - t0}

def benchmark_mongo(records):
    if not MONGO_AVAILABLE:
        return {"engine": "mongodb", "error": "MongoDB no disponible"}
    try:
        client = MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=2000)
        db = client["metadata_db"]
        col = db["books"]
        t0 = time.time()
        # Inserciones (no se borran, solo medimos tiempo de lectura)
        for r in records:
            pass
        t1 = time.time()
        # Consultas de ejemplo
        q0 = time.time()
        list(col.find({"author": {"$regex": "^A"}}, {"_id": 0}).limit(10))
        q1 = time.time()
        client.close()
        return {"engine": "mongodb", "records": len(records), "query_time": q1 - q0, "insert_time": t1 - t0}
    except Exception as e:
        return {"engine": "mongodb", "error": str(e)}

# --- Main ---
if __name__ == "__main__":
    logging.info("Cargando metadata de SQLite...")
    metadata_records = load_metadata_from_sqlite()
    logging.info(f"{len(metadata_records)} registros cargados desde SQLite.")

    logging.info("Ejecutando benchmark SQLite...")
    sqlite_result = benchmark_sqlite(metadata_records)
    logging.info(f"SQLite: {sqlite_result}")

    logging.info("Ejecutando benchmark MongoDB...")
    mongo_result = benchmark_mongo(metadata_records)
    logging.info(f"MongoDB: {mongo_result}")

    if MONGO_AVAILABLE:
        if sqlite_result["query_time"] < mongo_result.get("query_time", float("inf")):
            logging.info("SQLite es más rápido en consultas.")
        else:
            logging.info("MongoDB es más rápido en consultas.")
