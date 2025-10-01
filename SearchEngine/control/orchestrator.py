# src/search_project/crawler/downloader.py
"""
Downloader para Project Gutenberg:
- Descarga https://www.gutenberg.org/cache/epub/{id}/pg{id}.txt
- Detecta start/end markers y separa header/body
- Guarda en: data/datalake/YYYYMMDD/HH/{book_id}.body.txt  y  {book_id}.header.txt
- Añade el id a data/control/downloaded_books.txt (evita duplicados)
"""
from pathlib import Path
from datetime import datetime
import requests
import re
import time
import logging
from typing import Optional, Tuple

# Ajusta el correo en User-Agent
USER_AGENT = "Stage1SearchEngineBot/1.0 (email@example.com)"

# Regex para markers de inicio/fin
START_RE = re.compile(r"^\*\*\*\s*START OF (THIS|THE) PROJECT GUTENBERG EBOOK.*$", re.IGNORECASE | re.MULTILINE)
END_RE   = re.compile(r"^\*\*\*\s*END OF (THIS|THE) PROJECT GUTENBERG EBOOK.*$", re.IGNORECASE | re.MULTILINE)

def _find_header_body(text: str) -> Optional[Tuple[str, str]]:
    """Devuelve (header, body) o None si no encuentra markers."""
    m_start = START_RE.search(text)
    if not m_start:
        alt_start = re.search(r"\*\*\*\s*START OF .*PROJECT GUTENBERG.*\*\*\*", text, re.IGNORECASE)
        m_start = alt_start
    if not m_start:
        return None

    start_pos = m_start.end()
    m_end = END_RE.search(text, start_pos)
    if not m_end:
        alt_end = re.search(r"\*\*\*\s*END OF .*PROJECT GUTENBERG.*\*\*\*", text[start_pos:], re.IGNORECASE)
        if alt_end:
            end_pos = start_pos + alt_end.start()
        else:
            return None
    else:
        end_pos = m_end.start()

    header = text[:m_start.start()].strip()
    body = text[start_pos:end_pos].strip()
    return header, body

def datalake_paths(root: Path, book_id: int, ts: datetime = None) -> Tuple[Path, Path]:
    """Crea y devuelve (header_path, body_path) en datalake con estructura YYYYMMDD/HH/"""
    if ts is None:
        ts = datetime.utcnow()
    date = ts.strftime("%Y%m%d")
    hour = ts.strftime("%H")
    folder = root / date / hour
    folder.mkdir(parents=True, exist_ok=True)
    header_path = folder / f"{book_id}.header.txt"
    body_path = folder / f"{book_id}.body.txt"
    return header_path, body_path

def download_book(book_id: int,
                  datalake_root: Path,
                  control_dir: Path,
                  max_retries: int = 3,
                  timeout: int = 15) -> bool:
    """Descarga un libro; devuelve True si fue exitoso."""
    control_dir.mkdir(parents=True, exist_ok=True)
    downloaded_txt = control_dir / "downloaded_books.txt"

    # Evitar descargar dos veces
    if downloaded_txt.exists():
        if str(book_id) in {line.strip() for line in downloaded_txt.read_text(encoding="utf-8").splitlines()}:
            logging.info(f"[DOWNLOADER] Book {book_id} ya estaba registrado.")
            return True

    url = f"https://www.gutenberg.org/cache/epub/{book_id}/pg{book_id}.txt"
    headers = {"User-Agent": USER_AGENT}
    last_exc = None

    for attempt in range(1, max_retries + 1):
        try:
            logging.info(f"[DOWNLOADER] Descargando {book_id} (intento {attempt}) -> {url}")
            r = requests.get(url, headers=headers, timeout=timeout)
            r.raise_for_status()
            text = r.text

            found = _find_header_body(text)
            if not found:
                logging.warning(f"[DOWNLOADER] No se encontraron markers en {book_id}")
                return False

            header, body = found
            header_path, body_path = datalake_paths(datalake_root, book_id)
            body_path.write_text(body, encoding="utf-8")
            header_path.write_text(header, encoding="utf-8")

            with downloaded_txt.open("a", encoding="utf-8") as f:
                f.write(f"{book_id}\n")

            logging.info(f"[DOWNLOADER] Guardado book {book_id} en {body_path}")
            return True
        except requests.RequestException as e:
            last_exc = e
            logging.warning(f"[DOWNLOADER] Error {e} (intento {attempt})")
            time.sleep(2 ** attempt)
            continue
        except Exception as e:
            last_exc = e
            logging.exception(f"[DOWNLOADER] Error inesperado {e}")
            time.sleep(2 ** attempt)
            continue

    logging.error(f"[DOWNLOADER] Fallaron todos los intentos para {book_id}. Última excepción: {last_exc}")
    return False
