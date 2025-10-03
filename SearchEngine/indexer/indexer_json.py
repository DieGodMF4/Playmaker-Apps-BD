import json
from pathlib import Path
from collections import defaultdict

from .utils import tokenize

def build_index(datalake_path: Path, output_file: Path) -> object:
    """Builds an inverted index from .body .txt files from datalake and stores it as JSON"""
    inverted_index = defaultdict(set)

    for book_path in datalake_path.rglob("*.txt"):
        book_id = book_path.stem
        try:
            text = book_path.read_text(encoding="utf-8")
            words = tokenize(text)
            for word in words:
                inverted_index[word].add(book_id)
        except Exception as err:
            print(f"Error procesando {book_path.name}: {err}")

    """Transforming sets as lists to be serialized in JSON"""
    index_dict = {word: sorted(list(book_ids)) for word, book_ids in inverted_index.items()}

    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "w", encoding= "utf-8") as f:
        json.dump(index_dict, f, indent=2, ensure_ascii=False)

    print(f"índice invertido guardado en {output_file}")
