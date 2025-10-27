from pathlib import Path
from datetime import datetime
import requests
import re
import time
import logging
from typing import Optional, Tuple

USER_AGENT = "Stage1SearchEngineBot/1.0 (email@example.com)"

START_RE = re.compile(r"^\*\*\*\s*START OF (THIS|THE)?\s*PROJECT GUTENBERG EBOOK.*$", re.IGNORECASE | re.MULTILINE)
END_RE   = re.compile(r"^\*\*\*\s*END OF (THIS|THE)?\s*PROJECT GUTENBERG EBOOK.*$",   re.IGNORECASE | re.MULTILINE)
ALT_START = re.compile(r"\*\*\*\s*START OF .*PROJECT GUTENBERG.*\*\*\*", re.IGNORECASE)
ALT_END   = re.compile(r"\*\*\*\s*END OF .*PROJECT GUTENBERG.*\*\*\*",   re.IGNORECASE)

def _find_header_body(text: str) -> Optional[Tuple[str, str]]:
    m_start = START_RE.search(text) or ALT_START.search(text)
    if not m_start:
        return None
    start_pos = m_start.end()
    m_end = END_RE.search(text, start_pos) or ALT_END.search(text, start_pos)
    if not m_end:
        return None
    end_pos = m_end.start()
    return text[:m_start.start()].strip(), text[start_pos:end_pos].strip()

def datalake_paths(root: Path, book_id: int, ts: datetime = None) -> Tuple[Path, Path]:
    ts = ts or datetime.utcnow()
    folder = root / ts.strftime("%Y%m%d") / ts.strftime("%H")
    folder.mkdir(parents=True, exist_ok=True)
    return folder / f"{book_id}.header.txt", folder / f"{book_id}.body.txt"

def _fetch_text(url: str, timeout: int) -> Optional[str]:
    r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=timeout)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.text

def download_book(book_id: int, datalake_root: Path, control_dir: Path,
                  alt_raw_root: Path = None, max_retries: int = 3, timeout: int = 15) -> bool:
    control_dir.mkdir(parents=True, exist_ok=True)
    downloaded_txt = control_dir / "downloaded_books.txt"

    if downloaded_txt.exists():
        if str(book_id) in {l.strip() for l in downloaded_txt.read_text(encoding="utf-8").splitlines() if l.strip()}:
            logging.info(f"[DOWNLOADER] Book {book_id} already recorded.")
            return True

    base = f"https://www.gutenberg.org/cache/epub/{book_id}/pg{book_id}"
    candidates = [base + ".txt", base + ".txt.utf8"]

    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            logging.info(f"[DOWNLOADER] Fetching {book_id} (attempt {attempt})")
            text = None
            for url in candidates:
                text = _fetch_text(url, timeout)
                if text is not None:
                    break
            if text is None:
                logging.warning(f"[DOWNLOADER] Not found for {book_id}")
                return False

            hb = _find_header_body(text)
            if not hb:
                logging.warning(f"[DOWNLOADER] Markers not found for {book_id}")
                return False
            header, body = hb
            header_path, body_path = datalake_paths(datalake_root, book_id)
            body_path.write_text(body, encoding="utf-8")
            header_path.write_text(header, encoding="utf-8")

            if alt_raw_root:
                alt_raw_root.mkdir(parents=True, exist_ok=True)
                (alt_raw_root / f"{book_id}.txt").write_text(text, encoding="utf-8")

            with downloaded_txt.open("a", encoding="utf-8") as f:
                f.write(f"{book_id}\n")
            logging.info(f"[DOWNLOADER] Saved book {book_id} in {body_path}")
            return True

        except requests.RequestException as e:
            last_exc = e
            logging.warning(f"[DOWNLOADER] Request error {e} (attempt {attempt})")
            time.sleep(min(60, 2 ** attempt))
        except Exception as e:
            last_exc = e
            logging.exception(f"[DOWNLOADER] Unexpected error {e}")
            time.sleep(min(60, 2 ** attempt))
    logging.error(f"[DOWNLOADER] All attempts failed for {book_id}. Last: {last_exc}")
    return False
