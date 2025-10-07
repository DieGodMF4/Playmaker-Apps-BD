# benchmarks/db_bench.py
"""
Benchmark: compara rendimiento de SQLite vs MongoDB
en inserciones y consultas simples.
"""

import time
import sqlite3
from pathlib import Path
import random
import string
import logging

try:
    from pymongo import MongoClient
    MONGO_AVAILABLE = True
except Exception:
    MONGO_AVAILABLE = False

DATA_DIR = Path("data/datamarts")
DATA_DIR.mkdir(parents=True, exist_ok=True)


def random_string(n=20):
    return ''.join(random.choice(string.ascii_lowercase) for _ in range(n))


def benchmark_sqlite(records=5000):
    dbp = DATA_DIR / "bench_index.sqlite"
    if dbp.exists():
        dbp.unlink()
    conn = sqlite3.connect(dbp)
    cur = conn.cursor()
    cur.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, term TEXT, postings TEXT)")
    conn.commit()
    t0 = time.time()
    for i in range(records):
        term = random_string(10)
        postings = ','.join(str(random.randint(1, 10000)) for _ in range(5))
        cur.execute("INSERT INTO items (term, postings) VALUES (?, ?)", (term, postings))
    conn.commit()
    t1 = time.time()

    q0 = time.time()
    cur.execute("SELECT postings FROM items WHERE term LIKE 'a%' LIMIT 10")
    _ = cur.fetchall()
    q1 = time.time()
    conn.close()
    return {"engine": "sqlite", "inserts_time": t1 - t0, "query_time": q1 - q0, "records": records}


def benchmark_mongo(records=5000):
    if not MONGO_AVAILABLE:
        return {"engine": "mongodb", "error": "MongoDB no disponible"}

    try:
        client = MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=2000)
        db = client["benchmarks"]
        col = db["index_test"]
        col.drop()
        t0 = time.time()
        for i in range(records):
            term = random_string(10)
            postings = [random.randint(1, 10000) for _ in range(5)]
            col.insert_one({"term": term, "postings": postings})
        t1 = time.time()

        q0 = time.time()
        list(col.find({"term": {"$regex": "^a"}}, {"_id": 0}).limit(10))
        q1 = time.time()
        client.close()
        return {"engine": "mongodb", "inserts_time": t1 - t0, "query_time": q1 - q0, "records": records}
    except Exception as e:
        return {"engine": "mongodb", "error": str(e)}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(benchmark_sqlite(5000))
    print(benchmark_mongo(5000))
