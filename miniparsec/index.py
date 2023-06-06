import re


def file_index(path: str, min_length: int = 2) -> tuple[set[str], int]:
    word_count = 0
    index = set()
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            words = re.findall(r"\w+", line.lower())
            words = set(w for w in words if len(w) >= min_length)
            index.update(words)
            word_count += len(words)
    return index, len(index)
