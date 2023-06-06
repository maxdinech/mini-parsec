import re
from pathlib import Path

from unidecode import unidecode


def file_index(path: Path, min_length: int = 2) -> tuple[set[str], int]:
    word_count = 0
    index = set()
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            words_raw = re.findall(r"[a-z]+", unidecode(line.lower()))
            words = set(str(w) for w in words_raw if len(w) >= min_length)
            index.update(words)
            word_count += len(words)
    return index, len(index)
