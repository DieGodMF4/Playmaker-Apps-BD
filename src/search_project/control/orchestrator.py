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

def _read_ids(path: Path) -> Set[str]:
    if not path.exists():
        return set()
    return {line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()}

def _append_id(path: Path, book_id: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(f"{book_id}\n")

def control_pipeline(
    target_new_downloads: int = 10,
    datalake_root: Path = Path("data/datalake"),
    raw_root: Path = Path("data/raw"),
    total_tries: int = DEFAULT_TOTAL_TRIES,
) -> bool:
    """
    Runs one CONTROL 'tick':
      1) Index any downloaded-but-not-indexed books (index ALL of them).
      2) If nothing to index, try to download up to 'target_new_downloads' NEW books.
         As soon as we download at least one, we return True (so the caller can loop and index them).
    Returns:
      True  -> some progress was made (indexed or downloaded)
      False -> nothing happened (no candidates; downloads all failed/duplicates)
    """
    CONTROL_PATH.mkdir(parents=True, exist_ok=True)

    downloaded = _read_ids(DOWNLOADS)
    indexed = _read_ids(INDEXINGS)

    # Step 1: index everything pending
    ready_to_index = sorted(downloaded - indexed)
    if ready_to_index:
        for bid in ready_to_index:
            logging.info(f"[CONTROL] Indexing book {bid}")
            try:
                ok = schedule_index_for_book(int(bid), datalake_root=datalake_root, control_dir=CONTROL_PATH)
                if ok:
                    _append_id(INDEXINGS, bid)
                else:
                    logging.warning(f"[CONTROL] Skipped indexing for {bid} (missing body file).")
            except Exception as e:
                logging.exception(f"[CONTROL] Error indexing {bid}: {e}")
        return True  # progress happened

    # Step 2: attempt NEW downloads
    if target_new_downloads <= 0:
        logging.info("[CONTROL] No download target this session.")
        return False

    new_downloaded = 0
    tries = 0
    # choose a generous ID space; Gutenberg has many gaps, randomness is OK here
    while new_downloaded < target_new_downloads and tries < total_tries:
        tries += 1
        candidate_id = str(random.randint(1, 70000))
        if candidate_id in downloaded:
            continue
        logging.info(f"[CONTROL] Attempting download ID {candidate_id} (try {tries})")
        ok = download_book(
            int(candidate_id),
            datalake_root=datalake_root,
            control_dir=CONTROL_PATH,
            alt_raw_root=raw_root,
            max_retries=3,
        )
        if ok:
            downloaded.add(candidate_id)
            _append_id(DOWNLOADS, candidate_id)
            new_downloaded += 1
            logging.info(f"[CONTROL] Downloaded new book {candidate_id} "
                         f"({new_downloaded}/{target_new_downloads} this session)")

    if new_downloaded > 0:
        # signal caller to loop so that the fresh downloads get indexed immediately
        return True

    logging.info("[CONTROL] No progress: nothing to index and no new downloads succeeded.")
    return False
