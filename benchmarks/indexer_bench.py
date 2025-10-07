# indexer_variants.py
"""
Implementación de diferentes estrategias de índice invertido (Stage 1, sección 4.2):
1. Monolithic JSON
2. Hierarchical Folder Structure
3. MongoDB (si está disponible)
4. SQLite (ya lo tienes en indexer_core/indexer_db)

Cada clase tiene métodos:
- build(docs) → construye el índice
- query(term) → devuelve postings (lista de book_id)
"""

import json
import shutil
import string
from pathlib import Path
from collections import defaultdict

try:
    from pymongo import MongoClient
    MONGO_AVAILABLE = True
except Exception:
    MONGO_AVAILABLE = False


# ------------------ Monolithic JSON ------------------

class MonolithicJSONIndex:
    def __init__(self, out_dir: Path = Path("data/datamarts")):
        self.path = out_dir / "inverted_index.json"
        self.index = {}

    def build(self, docs):
        """docs = iterable de (book_id, text)"""
        postings = defaultdict(set)
        for book_id, text in docs:
            for term in text.split():
                postings[term.lower()].add(book_id)
        self.index = {t: sorted(list(ids)) for t, ids in postings.items()}
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self.index, indent=2), encoding="utf-8")

    def load(self):
        if self.path.exists():
            self.index = json.loads(self.path.read_text(encoding="utf-8"))

    def query(self, term: str):
        if not self.index:
            self.load()
        return self.index.get(term.lower(), [])


# ------------------ Hierarchical Folder ------------------

class HierarchicalFolderIndex:
    def __init__(self, out_dir: Path = Path("data/datamarts/inverted_index")):
        self.root = out_dir

    def _term_path(self, term: str) -> Path:
        first = (term[0].lower() if term else "_")
        safe = first if first in string.ascii_lowercase + string.digits else "_"
        return self.root / safe / f"{term.lower()}.txt"

    def build(self, docs):
        if self.root.exists():
            shutil.rmtree(self.root)
        for book_id, text in docs:
            seen = set()
            for term in text.split():
                t = term.lower()
                if t in seen:
                    continue
                seen.add(t)
                p = self._term_path(t)
                p.parent.mkdir(parents=True, exist_ok=True)
                with p.open("a", encoding="utf-8") as f:
                    f.write(f"{book_id}\n")

    def query(self, term: str):
        p = self._term_path(term)
        if not p.exists():
            return []
        return [line.strip() for line in p.read_text(encoding="utf-8").splitlines()]


# ------------------ MongoDB ------------------

class MongoDBIndex:
    def __init__(self, db_name="search_engine", collection="inverted_index"):
        if not MONGO_AVAILABLE:
            raise RuntimeError("MongoDB no disponible (instala pymongo).")
        self.client = MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=2000)
        self.db = self.client[db_name]
        self.col = self.db[collection]

    def build(self, docs):
        self.col.drop()
        self.col.create_index("term", unique=True)
        postings = defaultdict(set)
        for book_id, text in docs:
            for term in text.split():
                postings[term.lower()].add(book_id)
        for t, ids in postings.items():
            self.col.insert_one({"term": t, "postings": sorted(list(ids))})

    def query(self, term: str):
        r = self.col.find_one({"term": term.lower()}, {"postings": 1, "_id": 0})
        return r["postings"] if r else []
