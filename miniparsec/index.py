import re
from pathlib import Path

from unidecode import unidecode


def index_file(path: Path, min_length: int = 2) -> tuple[set[str], int]:
    index = set()
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            clean_line = unidecode(line.strip().lower())
            line_words_raw = re.findall(r"[a-z]+", clean_line)
            line_words = set(str(w) for w in line_words_raw if len(w) >= min_length)
            index.update(line_words)
    return index, len(index)


def index_file_group(path: Path) -> tuple[dict[str, set[str]], int]:
    words = []
    index = {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            clean_line = unidecode(line.strip().lower())
            line_words_raw = re.findall(r"[a-z]+", clean_line)
            line_words = [str(w) for w in line_words_raw if w]
            words += line_words
    for i in range(len(words)):
        word = words[i]
        try:
            next = words[i + 1]
        except IndexError:
            next = ""
        if word not in index:
            index[word] = set()
        index[word].add(next)
    return index, len(index)
