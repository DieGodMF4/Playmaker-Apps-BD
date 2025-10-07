# benchmarks/indexer_bench.py
"""
4.2 Inverted Index Benchmark

Compara tres estrategias:
1) Single Monolithic File (JSON en memoria)
2) SQLite (en memoria)
3) MongoDB (colección temporal)

Mide:
- Build time del índice
- Query performance (postings de ~50 términos frecuentes)
- Escalabilidad (nº de documentos)

Resultados: SOLO por pantalla. No se crean archivos.
Funciona desde cualquier carpeta: resuelve rutas y hace fallback si no encuentra 'src'.
"""

from pathlib import Path
import sys
import time
import json
import logging
from collections import defaultdict, Counter
from typing import List, Dict, Set
import random
import os

# --- Localiza la raíz del proyecto aunque ejecutes desde otra carpeta ---
THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parents[1]            # .../Playmaker-Apps-BD-/
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Import de tokenize con fallback por ruta si no existe el paquete
try:
    from src.search_project.utils.text_utils import tokenize  # type: ignore
except Exception:
    import importlib.util
    utils_path = PROJECT_ROOT / "src" / "search_project" / "utils" / "text_utils.py"
    spec = importlib.util.spec_from_file_location("text_utils_fallback", utils_path)
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader, f"No se pudo cargar {utils_path}"
    spec.loader.exec_module(mod)  # type: ignore
    tokenize = mod.tokenize  # type: ignore

try:
    from pymongo import MongoClient, UpdateOne  # type: ignore
    HAS_PYMONGO = True
except Exception:
    HAS_PYMONGO = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

DATALAKE = PROJECT_ROOT / "data" / "datalake"

def collect_bodies(limit_docs: int | None = None) -> List[Path]:
    paths = sorted(DATALAKE.rglob("*.body.txt"))
    if limit_docs:
        paths = paths[:limit_docs]
    return paths

def build_inverted(paths: List[Path]) -> Dict[str, Set[str]]:
    inv: Dict[str, Set[str]] = defaultdict(set)
    for p in paths:
        book_id = p.stem.split(".")[0]
        try:
            text = p.read_text(encoding="utf-8")
        except Exception:
            continue
        for w in tokenize(text, remove_stopwords=True):
            inv[w].add(str(book_id))
    return inv

def pick_terms(inverted: Dict[str, Set[str]], k: int = 50) -> List[str]:
    cnt = Counter({t: len(ids) for t, ids in inverted.items()})
    return [t for t, _ in cnt.most_common(k)] or list(cnt.keys())[:k]

# ----------------------
# JSON (en memoria)
# ----------------------
def build_json_index(inverted: Dict[str, Set[str]]):
    rows = [{"term": t, "postings": sorted(list(ids))} for t, ids in inverted.items()]
    return {row["term"]: row["postings"] for row in rows}

def query_json_terms(index: Dict[str, List[str]], terms: List[str]) -> float:
    t0 = time.perf_counter()
    for t in terms:
        _ = index.get(t, [])
    return time.perf_counter() - t0

# ----------------------
# SQLite (en memoria)
# ----------------------
def build_sqlite_index(inverted: Dict[str, Set[str]]):
    import sqlite3
    con = sqlite3.connect(":memory:")
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE inverted (
            term TEXT PRIMARY KEY,
            postings TEXT
        );
    """)
    cur.execute("CREATE INDEX idx_term ON inverted(term);")
    rows = [(t, json.dumps(sorted(list(ids)))) for t, ids in inverted.items()]
    cur.executemany("INSERT INTO inverted(term, postings) VALUES (?, ?)", rows)
    con.commit()
    return con

def query_sqlite_terms(con, terms: List[str]) -> float:
    cur = con.cursor()
    t0 = time.perf_counter()
    for t in terms:
        cur.execute("SELECT postings FROM inverted WHERE term = ?", (t,))
        cur.fetchone()
    return time.perf_counter() - t0

# ----------------------
# MongoDB (colección temporal)
# ----------------------
def build_mongo_index(inverted: Dict[str, Set[str]]):
    if not HAS_PYMONGO:
        raise RuntimeError("MongoDB no disponible.")
    import uuid
    client = MongoClient(os.getenv("MONGODB_URI", "mongodb://localhost:27017"), serverSelectionTimeoutMS=2000)  # type: ignore
    db = client[os.getenv("MONGODB_DB", "bench_search")]
    col_name = f"bench_inverted_{uuid.uuid4().hex[:8]}"
    col = db[col_name]
    col.create_index("term", unique=True)
    ops = []
    for term, ids in inverted.items():
        ops.append(UpdateOne({"term": term},
                             {"$set": {"postings": sorted(list(ids))}},
                             upsert=True))
    if ops:
        col.bulk_write(ops, ordered=False)
    return client, col

def query_mongo_terms(col, terms: List[str]) -> float:
    t0 = time.perf_counter()
    for t in terms:
        col.find_one({"term": t}, {"_id": 0, "postings": 1})
    return time.perf_counter() - t0

# ----------------------
# Caso de prueba
# ----------------------
def run_case(n_docs: int) -> List[dict]:
    paths = collect_bodies(n_docs)
    if not paths:
        return []
    inverted = build_inverted(paths)
    terms = pick_terms(inverted, k=min(50, max(5, len(inverted)//100)))

    results = []

    # JSON
    t0 = time.perf_counter()
    json_index = build_json_index(inverted)
    build_time = time.perf_counter() - t0
    q_time = query_json_terms(json_index, terms)
    results.append({"engine": "JSON", "docs": n_docs, "build_s": build_time, "query_50_terms_s": q_time})

    # SQLite
    t0 = time.perf_counter()
    sqlite_con = build_sqlite_index(inverted)
    build_time = time.perf_counter() - t0
    q_time = query_sqlite_terms(sqlite_con, terms)
    results.append({"engine": "SQLite", "docs": n_docs, "build_s": build_time, "query_50_terms_s": q_time})
    sqlite_con.close()

    # Mongo
    try:
        t0 = time.perf_counter()
        client, col = build_mongo_index(inverted)
        build_time = time.perf_counter() - t0
        q_time = query_mongo_terms(col, terms)
        results.append({"engine": "MongoDB", "docs": n_docs, "build_s": build_time, "query_50_terms_s": q_time})
        # limpieza
        col.drop()
        client.close()
    except Exception as e:
        results.append({"engine": "MongoDB", "docs": n_docs, "error": str(e)})

    return results

def main():
    random.seed(42)
    total_docs = len(collect_bodies())
    if total_docs == 0:
        print(f"No hay documentos en {DATALAKE}")
        return

    sizes = [100, 500, 1000, 2000, 5000]
    sizes = [n for n in sizes if n <= total_docs] or [min(100, total_docs)]

    all_results: List[dict] = []
    for n in sizes:
        all_results.extend(run_case(n))

    # Mostrar por pantalla
    print("\n========= INVERTED INDEX BENCHMARK =========")
    print(f"{'Engine':<10} {'Docs':<8} {'Build(s)':<12} {'Query50(s)':<12}")
    print("-"*50)
    for r in all_results:
        if "error" in r:
            print(f"{r['engine']:<10} {r['docs']:<8} ERROR: {r['error']}")
        else:
            print(f"{r['engine']:<10} {r['docs']:<8} {r['build_s']:<12.6f} {r['query_50_terms_s']:<12.6f}")
    print("============================================\n")

if __name__ == "__main__":
    main()
