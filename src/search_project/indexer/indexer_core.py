from pathlib import Path
import logging
from .indexer_db import build_index_from_paths
from ..metadata.metadata import extract_metadata_for_book

def schedule_index_for_book(book_id: int, datalake_root: Path = Path("data/datalake"),
                            control_dir: Path = Path("control"),
                            datamart_root: Path = Path("data/datamarts")):
    """ Indexa un solo libro (solo SQLite y MongoDB). """
    for p in datalake_root.rglob(f"{book_id}.body.txt"):
        body_path = p
        header_path = p.with_name(f"{book_id}.header.txt")
        text = body_path.read_text(encoding="utf-8")
        header = header_path.read_text(encoding="utf-8") if header_path.exists() else ""
        meta = extract_metadata_for_book(book_id, header, body_path)
        logging.info(f"[INDEXER] Metadata para {book_id}: {meta}")
        build_index_from_paths([body_path], datamart_root=datamart_root)
        return True
    logging.warning(f"[INDEXER] No se encontró el archivo de cuerpo para {book_id}")
    return False
