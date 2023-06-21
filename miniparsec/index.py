import re
from pathlib import Path

import Stemmer
import textract
from textract.exceptions import ExtensionNotSupported
from unidecode import unidecode

from miniparsec.utils import console

stemmer = Stemmer.Stemmer("english")

REGEX = r"[a-zA-Z]+"

EXTENSIONS = {
    ".csv",
    ".doc",
    ".docx",
    ".eml",
    ".epub",
    ".gif",
    ".htm",
    ".html",
    ".jpeg",
    ".jpg",
    ".json",
    ".log",
    ".mp3",
    ".msg",
    ".odt",
    ".ogg",
    ".pdf",
    ".png",
    ".pptx",
    ".ps",
    ".psv",
    ".rtf",
    ".tab",
    ".tff",
    ".tif",
    ".tiff",
    ".tsv",
    ".txt",
    ".wav",
    ".xls",
    ".xlsx",
}


def stem(words: str | list[str]) -> str | set[str]:
    """Calcule le stem d'un mot."""
    if isinstance(words, str):
        return stemmer.stemWord(words.lower())
    return set(stemmer.stemWords(words))


def index_file(path: Path, min_length: int = 2) -> set[str]:
    """Retourne un ensemble contenant tous les mots d'un fichier.

    Args:
        path: Chemin vers le fichier
        min_length: Longueur minimale d'un mots
    """
    index = set()
    extension = path.suffix
    if extension in EXTENSIONS:
        try:
            text = textract.process(path)
        except Exception as e:
            console.error(e)
            return set()
    else:
        try:
            text = textract.process(path, extension="txt")
        except ExtensionNotSupported as e:
            console.error(e)
            return set()
        except Exception as e:
            console.error(e)
            return set()

    text = text.decode("utf-8")
    clean_text = unidecode(text.strip())

    words_raw = re.findall(REGEX, clean_text)
    stem_tokens = stem(words_raw)
    index.update(stem_tokens)
    return index
