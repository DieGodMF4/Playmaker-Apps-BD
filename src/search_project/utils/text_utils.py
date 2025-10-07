# src/search_project/utils/text_utils.py
import re
import unicodedata

WORD_RE = re.compile(r"[a-zA-ZáéíóúüñÁÉÍÓÚÜÑ0-9]+")

def _strip_accents(s: str) -> str:
    return ''.join(c for c in unicodedata.normalize('NFKD', s) if not unicodedata.combining(c))

def tokenize(text: str):
    if not text:
        return []
    # normalize whitespace and remove uncommon punctuation
    text = text.lower()
    text = _strip_accents(text)
    words = WORD_RE.findall(text)
    return [w for w in words if w]
