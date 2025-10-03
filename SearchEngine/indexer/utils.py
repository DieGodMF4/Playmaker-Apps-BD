import re

def tokenize(text: str):
    """ Tokenize text into clean words (low case letters, without punctuation)"""
    text = text.lower()
    text = re.sub(r"[^a-záéíóúüñ\s]", " ", text)
    words = text.split()
    return words
