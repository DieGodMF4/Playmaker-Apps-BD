# src/search_project/control/orchestrator.py
from pathlib import Path
import random
import logging
from typing import Set

from ..crawler.downloader import download_book
from src.search_project.indexer.indexer_core import schedule_index_for_book

CONTROL_PATH = Path("control")
DOWNLOADS = CONTROL_PATH / "downloaded_books.txt"
INDEXINGS = CONTROL_PATH / "indexed_books.txt"

DEFAULT_TOTAL_TRIES = 100000
DEFAULT_DOWNLOAD_TARGET = 50  # descargar al menos 50 nuevos (configurable)

def _read_ids(path: Path) -> Set[str]:
    if not path.exists():
        return set()
    return {line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()}

def control_pipeline(target_new_downloads: int = DEFAULT_DOWNLOAD_TARGET,
                     datalake_root: Path = Path("data/datalake"),
                     raw_root: Path = Path("data/raw"),
                     total_tries: int = DEFAULT_TOTAL_TRIES):
    CONTROL_PATH.mkdir(parents=True, exist_ok=True)
    downloaded = _read_ids(DOWNLOADS)
    indexed = _read_ids(INDEXINGS)

    # 1) If there are downloaded but not indexed -> index them
    ready_to_index = downloaded - indexed
    if ready_to_index:
        for bid in list(ready_to_index):
            logging.info(f"[CONTROL] Scheduling index for {bid}")
            try:
                schedule_index_for_book(int(bid),
                                        datalake_root=datalake_root,
                                        control_dir=CONTROL_PATH)
                with open(INDEXINGS, "a", encoding="utf-8") as f:
                    f.write(f"{bid}\n")
                indexed.add(bid)
            except Exception as e:
                logging.exception(f"[CONTROL] Error indexing {bid}: {e}")
        return

    # 2) If nothing to index, attempt downloads until we get target_new_downloads new entries
    new_downloaded = 0
    tries = 0
    while new_downloaded < target_new_downloads and tries < total_tries:
        tries += 1
        candidate_id = str(random.randint(1, 70000))
        if candidate_id in downloaded:
            continue
        logging.info(f"[CONTROL] Attempting download ID {candidate_id} (try {tries})")
        ok = download_book(int(candidate_id),
                           datalake_root=datalake_root,
                           control_dir=CONTROL_PATH,
                           alt_raw_root=raw_root,
                           max_retries=3)
        if ok:
            downloaded.add(candidate_id)
            new_downloaded += 1
            logging.info(f"[CONTROL] Downloaded new book {candidate_id} ({new_downloaded}/{target_new_downloads})")
        else:
            logging.info(f"[CONTROL] Skipped book {candidate_id}")
    logging.info(f"[CONTROL] Finished downloads: {new_downloaded} new books")
