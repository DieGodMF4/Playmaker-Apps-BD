import re
import unicodedata

WORD_RE = re.compile(r"[a-zA-ZáéíóúüñÁÉÍÓÚÜÑ0-9]+")

# Minimal English stopword set (expandable). PDFs ask to ignore stop words.
STOPWORDS_EN = {
    "the","a","an","and","or","but","if","then","else","when","while","for","to","of","in","on","at","by","from",
    "with","without","as","is","are","was","were","be","been","being","that","this","these","those","it","its","into",
    "over","under","again","once","no","not","so","too","very","can","could","should","would","do","does","did","done",
    "have","has","had","having","you","your","yours","i","me","my","mine","we","our","ours","he","him","his","she",
    "her","hers","they","them","their","theirs"
}

def _strip_accents(s: str) -> str:
    return ''.join(c for c in unicodedata.normalize('NFKD', s) if not unicodedata.combining(c))

def tokenize(text: str, remove_stopwords: bool = True):
    if not text:
        return []
    text = _strip_accents(text.lower())
    words = WORD_RE.findall(text)
    if remove_stopwords:
        return [w for w in words if w and w not in STOPWORDS_EN]
    return [w for w in words if w]
